import "dotenv/config";
import express from "express";
import fetch from "node-fetch";

// Bootstrap the existing production server without rewriting server.js.
// We temporarily intercept app.listen(), import the existing server, add the
// lead-owner booking endpoint, then start the captured Express app normally.
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

// These are Lead-module fields populated by the existing Zoho `calendarupdate`
// workflow after the assignment rule sets the Lead Owner. Reading the copied
// fields is more reliable than requiring the API token to have Users.READ.
const LEAD_CALENDAR_EN = "Lead_Owner_Calendar_Link";
const LEAD_CALENDAR_ES = "Spanish_Calendar_Link";
const LEAD_CALENDAR_AR = "Arabic_Calendar_Link";

let ownerLookupAccessToken = null;
let ownerLookupTokenExpiresAt = 0;

function normalizeEmail(value) {
  const email = String(value || "").trim().toLowerCase();
  if (!email) return "";
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) ? email : "";
}

function normalizeLanguage(value) {
  const lang = String(value || "en").trim().toLowerCase();
  return lang === "es" || lang === "ar" ? lang : "en";
}

function pickLeadCalendar(lead, language) {
  const english = String(lead?.[LEAD_CALENDAR_EN] || "").trim();
  const spanish = String(lead?.[LEAD_CALENDAR_ES] || "").trim();
  const arabic = String(lead?.[LEAD_CALENDAR_AR] || "").trim();

  if (language === "es") {
    if (spanish) return { bookingUrl: spanish, calendarField: LEAD_CALENDAR_ES };
    if (english) return { bookingUrl: english, calendarField: LEAD_CALENDAR_EN };
  }

  if (language === "ar") {
    if (arabic) return { bookingUrl: arabic, calendarField: LEAD_CALENDAR_AR };
    if (english) return { bookingUrl: english, calendarField: LEAD_CALENDAR_EN };
  }

  if (english) return { bookingUrl: english, calendarField: LEAD_CALENDAR_EN };
  return { bookingUrl: null, calendarField: null };
}

async function getOwnerLookupAccessToken() {
  const now = Date.now();
  if (ownerLookupAccessToken && now < ownerLookupTokenExpiresAt - 60_000) {
    return ownerLookupAccessToken;
  }

  if (!ZOHO_CLIENT_ID || !ZOHO_CLIENT_SECRET || !ZOHO_REFRESH_TOKEN) {
    throw new Error("Missing Zoho env vars");
  }

  const params = new URLSearchParams({
    refresh_token: ZOHO_REFRESH_TOKEN,
    client_id: ZOHO_CLIENT_ID,
    client_secret: ZOHO_CLIENT_SECRET,
    grant_type: "refresh_token",
  });

  const response = await fetch(`${ZOHO_ACCOUNTS}/oauth/v2/token?${params.toString()}`, {
    method: "POST",
  });
  const data = await response.json().catch(() => ({}));

  if (!response.ok || !data.access_token) {
    throw new Error("Zoho token refresh failed for owner booking lookup");
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
    throw new Error(`Zoho owner booking lookup failed (${response.status})`);
  }
  return data;
}

async function findLeadBySessionId(sessionId) {
  const criteria = `(Session_ID:equals:${sessionId})`;
  const data = await ownerLookupZohoGet(
    `/crm/v2/Leads/search?criteria=${encodeURIComponent(criteria)}`
  );
  return data?.data?.[0] || null;
}

async function findLeadByEmail(email) {
  if (!email) return null;
  const criteria = `(Email:equals:${email})`;
  const data = await ownerLookupZohoGet(
    `/crm/v2/Leads/search?criteria=${encodeURIComponent(criteria)}`
  );
  return data?.data?.[0] || null;
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function findLeadWithRetry(sessionId, email) {
  // Zoho's search index can lag briefly after record creation/update. Retry a
  // few times so the result page does not race the assignment/calendar workflow.
  const delays = [0, 500, 1000, 1500];
  let lastLead = null;
  let lastMatchedBy = null;

  for (const delay of delays) {
    if (delay) await wait(delay);

    let lead = await findLeadBySessionId(sessionId);
    let matchedBy = "session";

    if (!lead && email) {
      lead = await findLeadByEmail(email);
      matchedBy = "email";
    }

    if (lead?.id) {
      lastLead = lead;
      lastMatchedBy = matchedBy;
      const { bookingUrl } = pickLeadCalendar(lead, normalizeLanguage(lead?.Preferred_Language));
      if (bookingUrl && lead?.Owner?.id) {
        return { lead, matchedBy };
      }
    }
  }

  return { lead: lastLead, matchedBy: lastMatchedBy };
}

capturedApp.get("/api/lead-owner-booking", async (req, res) => {
  const sessionId = String(req.query.session_id || "").trim();
  const suppliedEmail = String(req.query.email || "").trim();
  const email = normalizeEmail(suppliedEmail);
  const language = normalizeLanguage(req.query.lang);

  if (!sessionId) {
    return res.status(400).json({ error: "session_id is required" });
  }

  if (suppliedEmail && !email) {
    return res.status(400).json({ error: "email is invalid" });
  }

  try {
    const { lead, matchedBy } = await findLeadWithRetry(sessionId, email);

    if (!lead?.id) {
      return res.status(404).json({ error: "lead not found" });
    }

    const ownerId = String(lead?.Owner?.id || "").trim();
    if (!ownerId) {
      return res.status(404).json({ error: "lead owner not found" });
    }

    const { bookingUrl, calendarField } = pickLeadCalendar(lead, language);

    return res.json({
      leadId: lead.id,
      owner: {
        id: ownerId,
        name: String(lead?.Owner?.name || "").trim() || null,
      },
      bookingUrl,
      language,
      calendarField,
      matchedBy,
    });
  } catch (error) {
    console.error("[GET /api/lead-owner-booking] error:", String(error?.message || error));
    return res.status(500).json({ error: "owner booking lookup failed" });
  }
});

originalListen.apply(capturedApp, capturedListenArgs);
