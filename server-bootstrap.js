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

const USER_CALENDAR_EN = "Calendar_Link";
const USER_CALENDAR_ES = "Spanish_Calendar_Link";
const USER_CALENDAR_AR = "Arabic_Calendar_Link";

// The website begins resolving in the background as soon as the Lead is created.
// Zoho assignment workflows can take 30-50+ seconds. While Owner is still the
// record creator, treat it as temporary for up to 60 seconds so we never cache
// the creator's missing language calendar before the real sales owner is assigned.
const ASSIGNMENT_GRACE_MS = 60_000;
const OWNER_STABILITY_CHECK_MS = 500;
const RETRY_AFTER_MS = 750;

let ownerLookupAccessToken = null;
let ownerLookupTokenExpiresAt = 0;

function normalizeEmail(value) {
  const email = String(value || "").trim().toLowerCase();
  if (!email) return "";
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) ? email : "";
}

function normalizeLanguage(value) {
  const lang = String(value || "en").trim().toLowerCase();
  if (["es", "es-es", "spanish", "espanol", "español"].includes(lang)) return "es";
  if (["ar", "ar-sa", "arabic", "العربية"].includes(lang)) return "ar";
  return "en";
}

function requestedCalendarField(language) {
  if (language === "es") return USER_CALENDAR_ES;
  if (language === "ar") return USER_CALENDAR_AR;
  return USER_CALENDAR_EN;
}

function pickOwnerCalendar(user, language) {
  const calendarField = requestedCalendarField(language);
  const bookingUrl = String(user?.[calendarField] || "").trim() || null;
  return { bookingUrl, calendarField };
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function recordAgeMs(lead) {
  const createdAt = Date.parse(String(lead?.Created_Time || ""));
  return Number.isFinite(createdAt) ? Math.max(0, Date.now() - createdAt) : null;
}

async function getOwnerLookupAccessToken() {
  const now = Date.now();
  if (ownerLookupAccessToken && now < ownerLookupTokenExpiresAt - 60_000) return ownerLookupAccessToken;

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
    const error = new Error("Zoho token refresh failed for owner booking lookup");
    error.status = response.status;
    error.code = data?.error || "ZOHO_TOKEN_REFRESH_FAILED";
    throw error;
  }

  ownerLookupAccessToken = data.access_token;
  ownerLookupTokenExpiresAt = Date.now() + (data.expires_in || 3600) * 1000;
  return ownerLookupAccessToken;
}

async function ownerLookupZohoGet(path) {
  const token = await getOwnerLookupAccessToken();
  const response = await fetch(`${ZOHO_API_BASE}${path}`, {
    headers: { Authorization: `Zoho-oauthtoken ${token}` },
  });
  const data = await response.json().catch(() => ({}));

  if (!response.ok) {
    const error = new Error(`Zoho owner booking lookup failed (${response.status})`);
    error.status = response.status;
    error.code = data?.code || "ZOHO_API_ERROR";
    error.zohoMessage = data?.message || null;
    throw error;
  }

  return data;
}

async function findLeadBySessionId(sessionId) {
  const criteria = `(Session_ID:equals:${sessionId})`;
  const data = await ownerLookupZohoGet(`/crm/v2/Leads/search?criteria=${encodeURIComponent(criteria)}`);
  return data?.data?.[0] || null;
}

async function findLeadByEmail(email) {
  if (!email) return null;
  const criteria = `(Email:equals:${email})`;
  const data = await ownerLookupZohoGet(`/crm/v2/Leads/search?criteria=${encodeURIComponent(criteria)}`);
  return data?.data?.[0] || null;
}

async function getLeadById(leadId) {
  const data = await ownerLookupZohoGet(`/crm/v2/Leads/${encodeURIComponent(leadId)}`);
  return data?.data?.[0] || null;
}

async function getUserById(userId) {
  const data = await ownerLookupZohoGet(`/crm/v2/users/${encodeURIComponent(userId)}`);
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

capturedApp.get("/api/lead-owner-booking", async (req, res) => {
  const sessionId = String(req.query.session_id || "").trim();
  const suppliedEmail = String(req.query.email || "").trim();
  const email = normalizeEmail(suppliedEmail);
  const language = normalizeLanguage(req.query.lang);

  if (!sessionId) return res.status(400).json({ status: "error", error: "session_id is required" });
  if (suppliedEmail && !email) return res.status(400).json({ status: "error", error: "email is invalid" });

  try {
    const first = await resolveLead(sessionId, email);
    let lead = first.lead;

    if (!lead?.id) return pending(res, "crm_record_not_ready", { stage: "lead" });

    let ownerId = String(lead?.Owner?.id || "").trim();
    if (!ownerId) {
      return pending(res, "owner_not_ready", { stage: "owner", leadId: lead.id, matchedBy: first.matchedBy });
    }

    const createdById = String(lead?.Created_By?.id || "").trim();
    const ageMs = recordAgeMs(lead);
    if (createdById && ownerId === createdById && (ageMs === null || ageMs < ASSIGNMENT_GRACE_MS)) {
      return pending(res, "owner_assignment_in_progress", {
        stage: "owner",
        leadId: lead.id,
        ownerId,
        matchedBy: first.matchedBy,
      });
    }

    await wait(OWNER_STABILITY_CHECK_MS);
    const confirmedLead = await getLeadById(lead.id);
    const confirmedOwnerId = String(confirmedLead?.Owner?.id || "").trim();

    if (!confirmedOwnerId || confirmedOwnerId !== ownerId) {
      return pending(res, "owner_assignment_in_progress", {
        stage: "owner",
        leadId: lead.id,
        matchedBy: first.matchedBy,
      });
    }

    // Repeat the creator-owner guard after the stability read so a creator-owned
    // snapshot can never become terminal during the assignment grace period.
    const confirmedCreatedById = String(confirmedLead?.Created_By?.id || createdById || "").trim();
    const confirmedAgeMs = recordAgeMs(confirmedLead || lead);
    if (
      confirmedCreatedById &&
      confirmedOwnerId === confirmedCreatedById &&
      (confirmedAgeMs === null || confirmedAgeMs < ASSIGNMENT_GRACE_MS)
    ) {
      return pending(res, "owner_assignment_in_progress", {
        stage: "owner",
        leadId: lead.id,
        ownerId: confirmedOwnerId,
        matchedBy: first.matchedBy,
      });
    }

    lead = confirmedLead || lead;
    ownerId = confirmedOwnerId;

    const owner = await getUserById(ownerId);
    if (!owner) {
      return pending(res, "owner_profile_not_ready", {
        stage: "owner_profile",
        leadId: lead.id,
        matchedBy: first.matchedBy,
      });
    }

    const { bookingUrl, calendarField } = pickOwnerCalendar(owner, language);

    if (!bookingUrl) {
      return res.json({
        status: "calendar_missing",
        reason: "language_calendar_missing",
        source: "zoho_user_profile",
        leadId: lead.id,
        owner: { id: ownerId, name: String(owner.full_name || lead?.Owner?.name || "").trim() || null },
        bookingUrl: null,
        language,
        calendarField,
        matchedBy: first.matchedBy,
      });
    }

    return res.json({
      status: "ready",
      source: "zoho_user_profile",
      leadId: lead.id,
      owner: { id: ownerId, name: String(owner.full_name || lead?.Owner?.name || "").trim() || null },
      bookingUrl,
      language,
      calendarField,
      matchedBy: first.matchedBy,
    });
  } catch (error) {
    const status = Number(error?.status || 0);
    const code = String(error?.code || "");

    if (status === 401 || status === 403 || code === "OAUTH_SCOPE_MISMATCH") {
      console.error("[GET /api/lead-owner-booking] Zoho Users API authorization error:", status, code);
      return res.status(503).json({
        status: "configuration_error",
        code: "ZOHO_USERS_SCOPE_REQUIRED",
        error: "Zoho user-profile calendar lookup is not authorized",
        bookingUrl: null,
      });
    }

    console.error("[GET /api/lead-owner-booking] error:", String(error?.message || error));
    return res.status(500).json({ status: "error", error: "owner booking lookup failed", bookingUrl: null });
  }
});

originalListen.apply(capturedApp, capturedListenArgs);
