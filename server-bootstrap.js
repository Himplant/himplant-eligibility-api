import "dotenv/config";
import express from "express";
import fetch from "node-fetch";

const originalListen = express.application.listen;
let capturedApp = null;
let capturedListenArgs = null;

express.application.listen = function interceptedListen(...args) {
  capturedApp = this;
  capturedListenArgs = args;
  return this;
};

await import("./server.js");
express.application.listen = originalListen;

if (!capturedApp || !capturedListenArgs) {
  throw new Error("Failed to capture Express app from server.js");
}

const ZOHO_ACCOUNTS = "https://accounts.zoho.com";
const ZOHO_API_BASE = "https://www.zohoapis.com";
const { ZOHO_CLIENT_ID, ZOHO_CLIENT_SECRET, ZOHO_REFRESH_TOKEN } = process.env;

const RETRY_AFTER_MS = 1000;
const FINAL_OWNER_GRACE_MS = 180_000;

const USER_FIELDS = {
  en: "Calendar_Link",
  es: "Spanish_Calendar_Link",
  ar: "Arabic_Calendar_Link",
  // No Portuguese calendar is configured in Zoho yet. Preserve the current
  // English booking fallback while keeping the requested locale as Portuguese.
  pt: "Calendar_Link",
};

// These fields are populated on the Lead by the existing Zoho calendarupdate
// workflow from the assigned owner's user profile. They are the most reliable
// delivery source because they avoid a second Users API dependency.
const LEAD_FIELDS = {
  en: "Lead_Owner_Calendar_Link",
  es: "Spanish_Calendar_Link",
  ar: "Arabic_Calendar_Link",
  pt: "Lead_Owner_Calendar_Link",
};

let accessToken = null;
let tokenExpiresAt = 0;

function normalizeEmail(value) {
  const email = String(value || "").trim().toLowerCase();
  if (!email) return "";
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) ? email : "";
}

function normalizeLanguage(value) {
  const lang = String(value || "en").trim().toLowerCase();
  if (["es", "es-es", "spanish", "espanol", "español"].includes(lang)) return "es";
  if (["ar", "ar-sa", "arabic", "العربية"].includes(lang)) return "ar";
  if (["pt", "pt-br", "pt_br", "portuguese", "português", "portugues", "português (brasil)"].includes(lang)) return "pt";
  return "en";
}

function ageMs(lead) {
  const createdAt = Date.parse(String(lead?.Created_Time || ""));
  return Number.isFinite(createdAt) ? Math.max(0, Date.now() - createdAt) : null;
}

function value(obj, field) {
  return String(obj?.[field] || "").trim() || null;
}

async function getAccessToken() {
  const now = Date.now();
  if (accessToken && now < tokenExpiresAt - 60_000) return accessToken;

  if (!ZOHO_CLIENT_ID || !ZOHO_CLIENT_SECRET || !ZOHO_REFRESH_TOKEN) {
    const error = new Error("Missing Zoho env vars");
    error.code = "ZOHO_ENV_MISSING";
    throw error;
  }

  const params = new URLSearchParams({
    refresh_token: ZOHO_REFRESH_TOKEN,
    client_id: ZOHO_CLIENT_ID,
    client_secret: ZOHO_CLIENT_SECRET,
    grant_type: "refresh_token",
  });

  const response = await fetch(`${ZOHO_ACCOUNTS}/oauth/v2/token?${params.toString()}`, { method: "POST" });
  const data = await response.json().catch(() => ({}));

  if (!response.ok || !data.access_token) {
    const error = new Error("Zoho token refresh failed");
    error.status = response.status;
    error.code = data?.error || "ZOHO_TOKEN_REFRESH_FAILED";
    throw error;
  }

  accessToken = data.access_token;
  tokenExpiresAt = Date.now() + (data.expires_in || 3600) * 1000;
  return accessToken;
}

async function zohoGet(path) {
  const token = await getAccessToken();
  const response = await fetch(`${ZOHO_API_BASE}${path}`, {
    headers: { Authorization: `Zoho-oauthtoken ${token}` },
  });
  const data = await response.json().catch(() => ({}));

  if (!response.ok) {
    const error = new Error(`Zoho request failed (${response.status})`);
    error.status = response.status;
    error.code = data?.code || "ZOHO_API_ERROR";
    throw error;
  }
  return data;
}

async function findLeadBySessionId(sessionId) {
  const criteria = `(Session_ID:equals:${sessionId})`;
  const data = await zohoGet(`/crm/v2/Leads/search?criteria=${encodeURIComponent(criteria)}`);
  return data?.data?.[0] || null;
}

async function findLeadByEmail(email) {
  if (!email) return null;
  const criteria = `(Email:equals:${email})`;
  const data = await zohoGet(`/crm/v2/Leads/search?criteria=${encodeURIComponent(criteria)}`);
  return data?.data?.[0] || null;
}

async function getLeadById(leadId) {
  const data = await zohoGet(`/crm/v2/Leads/${encodeURIComponent(leadId)}`);
  return data?.data?.[0] || null;
}

async function getUserById(userId) {
  const data = await zohoGet(`/crm/v2/users/${encodeURIComponent(userId)}`);
  return data?.users?.[0] || null;
}

async function resolveLead(sessionId, email) {
  let lead = await findLeadBySessionId(sessionId);
  let matchedBy = "session";

  if (!lead && email) {
    lead = await findLeadByEmail(email);
    matchedBy = "email";
  }

  if (!lead?.id) return { lead: null, matchedBy: null };

  const freshLead = await getLeadById(lead.id);
  return { lead: freshLead || lead, matchedBy };
}

function pending(res, reason, extra = {}) {
  return res.json({
    status: "pending",
    reason,
    retryAfterMs: RETRY_AFTER_MS,
    bookingUrl: null,
    ...extra,
  });
}

function ready(res, { lead, ownerId, language, bookingUrl, calendarField, matchedBy, source }) {
  return res.json({
    status: "ready",
    source,
    leadId: lead.id,
    owner: {
      id: ownerId,
      name: String(lead?.Owner?.name || "").trim() || null,
    },
    bookingUrl,
    language,
    calendarLanguage: language === "pt" ? "en" : language,
    calendarField,
    matchedBy,
  });
}

capturedApp.get("/api/lead-owner-booking", async (req, res) => {
  const sessionId = String(req.query.session_id || "").trim();
  const suppliedEmail = String(req.query.email || "").trim();
  const email = normalizeEmail(suppliedEmail);
  const language = normalizeLanguage(req.query.lang);

  if (!sessionId) return res.status(400).json({ status: "error", error: "session_id is required" });
  if (suppliedEmail && !email) return res.status(400).json({ status: "error", error: "email is invalid" });

  try {
    const { lead, matchedBy } = await resolveLead(sessionId, email);
    if (!lead?.id) return pending(res, "crm_record_not_ready", { stage: "lead" });

    const ownerId = String(lead?.Owner?.id || "").trim();
    const createdById = String(lead?.Created_By?.id || "").trim();
    const recordAge = ageMs(lead);

    if (!ownerId) {
      return pending(res, "owner_not_ready", { stage: "owner", leadId: lead.id, matchedBy });
    }

    // Never finalize against the temporary creator owner while Zoho assignment
    // rules may still be running. Production tests have shown 30-70+ second lag.
    if (createdById && ownerId === createdById && (recordAge === null || recordAge < FINAL_OWNER_GRACE_MS)) {
      return pending(res, "owner_assignment_in_progress", {
        stage: "owner",
        leadId: lead.id,
        ownerId,
        matchedBy,
      });
    }

    const leadCalendarField = LEAD_FIELDS[language];
    const syncedLeadUrl = value(lead, leadCalendarField);

    // PRIMARY: the Zoho calendarupdate workflow has already copied the current
    // owner's language-specific calendar onto this Lead. This is authoritative
    // for delivery and avoids Users API scope/availability failures.
    if (syncedLeadUrl) {
      return ready(res, {
        lead,
        ownerId,
        language,
        bookingUrl: syncedLeadUrl,
        calendarField: leadCalendarField,
        matchedBy,
        source: "zoho_lead_synced_owner_profile",
      });
    }

    // SECONDARY: directly read the current owner's user profile. This preserves
    // the user profile as the canonical configuration source when available.
    try {
      const owner = await getUserById(ownerId);
      const userCalendarField = USER_FIELDS[language];
      const ownerProfileUrl = value(owner, userCalendarField);

      if (ownerProfileUrl) {
        return ready(res, {
          lead,
          ownerId,
          language,
          bookingUrl: ownerProfileUrl,
          calendarField: userCalendarField,
          matchedBy,
          source: "zoho_user_profile",
        });
      }
    } catch (userError) {
      // Users API scope/network errors must never become a terminal failure.
      // The Lead workflow may still populate the synced field on a later poll.
      console.warn("[lead-owner-booking] user-profile lookup unavailable; continuing via Lead sync", {
        leadId: lead.id,
        ownerId,
        language,
        status: Number(userError?.status || 0) || null,
        code: String(userError?.code || "") || null,
      });
    }

    // Do not declare a calendar missing while assignment/workflow propagation may
    // still be completing. After 3 minutes, a stable non-creator owner with no
    // language URL in either source is treated as genuinely unconfigured.
    if (recordAge === null || recordAge < FINAL_OWNER_GRACE_MS) {
      return pending(res, "calendar_sync_in_progress", {
        stage: "calendar",
        leadId: lead.id,
        ownerId,
        language,
        calendarField: leadCalendarField,
        matchedBy,
      });
    }

    return res.json({
      status: "calendar_missing",
      reason: "language_calendar_missing",
      source: "zoho_lead_and_user_profile_checked",
      leadId: lead.id,
      owner: {
        id: ownerId,
        name: String(lead?.Owner?.name || "").trim() || null,
      },
      bookingUrl: null,
      language,
      calendarField: leadCalendarField,
      matchedBy,
    });
  } catch (error) {
    console.error("[GET /api/lead-owner-booking] error:", String(error?.message || error));
    // Infrastructure/API errors are non-terminal for the frontend resolver.
    return res.status(503).json({
      status: "pending",
      reason: "crm_temporarily_unavailable",
      retryAfterMs: 2000,
      bookingUrl: null,
    });
  }
});

originalListen.apply(capturedApp, capturedListenArgs);

