import express from "express";
import cors from "cors";
import axios from "axios";

const app = express();

/**
 * HIPAA SAFETY
 * - DO NOT log request bodies
 * - DO NOT store PHI anywhere besides Zoho
 */
app.use(express.json({ limit: "1mb" }));

/**
 * CORS (lock to your Webflow + frontend domains)
 * Add/remove domains as needed.
 */
const ALLOWED_ORIGINS = [
  "https://himplant.com",
  "https://www.himplant.com",
  "https://eligibility.himplant.com",
];

app.use(
  cors({
    origin: (origin, callback) => {
      // Allow curl/server-to-server (no origin header)
      if (!origin) return callback(null, true);
      if (ALLOWED_ORIGINS.includes(origin)) return callback(null, true);
      return callback(new Error("Not allowed by CORS"));
    },
    methods: ["GET", "POST", "PUT", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
  })
);

// -------------------------
// ZOHO CONFIG (US DC)
// -------------------------
const ZOHO_BASE = "https://www.zohoapis.com";
const ZOHO_TOKEN_URL = "https://accounts.zoho.com/oauth/v2/token";
const ZOHO_LEADS_MODULE = "Leads";

// In-memory token cache (Render is ephemeral; this is fine)
let cachedAccessToken = null;
let cachedAccessTokenExpiryMs = 0;

// -------------------------
// ZOHO AUTH
// -------------------------
async function getZohoAccessToken() {
  const now = Date.now();

  // Refresh if missing or expiring soon
  if (cachedAccessToken && now < cachedAccessTokenExpiryMs - 60_000) {
    return cachedAccessToken;
  }

  const params = new URLSearchParams({
    refresh_token: process.env.ZOHO_REFRESH_TOKEN,
    client_id: process.env.ZOHO_CLIENT_ID,
    client_secret: process.env.ZOHO_CLIENT_SECRET,
    grant_type: "refresh_token",
  });

  const resp = await axios.post(`${ZOHO_TOKEN_URL}?${params.toString()}`, null, {
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    timeout: 20_000,
  });

  const accessToken = resp?.data?.access_token;
  const expiresInSec = Number(resp?.data?.expires_in || 3600);

  if (!accessToken) {
    throw new Error("Zoho token refresh failed (no access_token returned).");
  }

  cachedAccessToken = accessToken;
  cachedAccessTokenExpiryMs = Date.now() + expiresInSec * 1000;
  return cachedAccessToken;
}

async function zohoRequest(method, path, { params, data } = {}) {
  const token = await getZohoAccessToken();
  const url = `${ZOHO_BASE}${path}`;

  return axios.request({
    method,
    url,
    params,
    data,
    timeout: 25_000,
    headers: {
      Authorization: `Zoho-oauthtoken ${token}`,
      "Content-Type": "application/json",
    },
  });
}

// -------------------------
// HELPERS
// -------------------------
function digitsOnly(s) {
  return String(s || "").replace(/\D/g, "");
}

function formatZohoPhone(phoneCountryCode, phoneNumber) {
  // digits-only, prepend digits-only country code
  const cc = digitsOnly(phoneCountryCode); // "+1" -> "1"
  const pn = digitsOnly(phoneNumber);
  if (!cc && !pn) return "";
  return `${cc}${pn}`;
}

function safeJoinArray(arr) {
  if (!Array.isArray(arr)) return "";
  return arr
    .map((x) => String(x || "").trim())
    .filter(Boolean)
    .join(", ");
}

function toBooleanValue(v) {
  if (v === true || v === false) return v;
  if (typeof v === "string") {
    const s = v.toLowerCase().trim();
    if (["true", "yes", "1"].includes(s)) return true;
    if (["false", "no", "0"].includes(s)) return false;
  }
  return v;
}

function isoOrNow(iso) {
  const d = iso ? new Date(iso) : new Date();
  if (Number.isNaN(d.getTime())) return new Date().toISOString();
  return d.toISOString();
}

/**
 * Avoid overwriting Zoho fields with blank values.
 */
function pruneEmpty(obj) {
  const out = {};
  for (const [k, v] of Object.entries(obj)) {
    if (v === undefined) continue;
    if (v === null) continue;
    if (typeof v === "string" && v.trim() === "") continue;
    out[k] = v;
  }
  return out;
}

// -------------------------
// ZOHO LEAD MAPPERS
// -------------------------
function mapPartialToZohoLead(submission) {
  const phone = formatZohoPhone(submission.phone_country_code, submission.phone_number);

  return pruneEmpty({
    First_Name: submission.first_name || "",
    Last_Name: submission.last_name || "",
    Email: submission.email || "",

    // store to both
    Phone: phone,
    Mobile: phone,

    // Patient current location
    Country: submission.current_location_country || "",
    State: submission.current_location_state || "",

    // Session link
    Session_ID: submission.session_id || "",

    // Dates
    Date_of_Birth: submission.date_of_birth || null,
    Intake_Date: isoOrNow(submission.submitted_at),
  });
}

function mapCompleteToZohoLead(submission) {
  const phone = formatZohoPhone(submission.phone_country_code, submission.phone_number);

  return pruneEmpty({
    First_Name: submission.first_name || "",
    Last_Name: submission.last_name || "",
    Email: submission.email || "",

    Phone: phone,
    Mobile: phone,

    // Patient current location
    Country: submission.current_location_country || "",
    State: submission.current_location_state || "",

    // Session link
    Session_ID: submission.session_id || "",

    // Surgeon selection lookup (record id)
    Surgeon_name_Lookup: submission.surgeon_id || "",

    // Preferences
    Payment_Method: submission.payment_method || "",
    Procedure_Timeline: submission.timeline || "",

    // Health
    Circumcised: toBooleanValue(submission.circumcised),
    Tobacco: toBooleanValue(submission.tobacco_use),
    Body_Type: submission.body_type || "",

    // ED
    ED_history: submission.ed_history || "",
    Can_maintain_erection: submission.ed_maintain_with_or_without_meds || "",

    // STD
    Active_STD: toBooleanValue(submission.active_std),
    STD_list: safeJoinArray(submission.std_list),
    Recent_Outbreak: toBooleanValue(submission.recent_outbreak_6mo),

    // Prior procedures
    Previous_Penis_Surgeries: toBooleanValue(submission.prior_procedures),

    // Conditions
    Medical_conditions_list: safeJoinArray(submission.medical_conditions_list),

    // Outcome
    Outcome: submission.outcome || "",

    // Dates
    Date_of_Birth: submission.date_of_birth || null,
    Intake_Date: isoOrNow(submission.submitted_at),
  });
}

// -------------------------
// ZOHO SEARCH HELPERS (EMAIL PRIORITY)
// -------------------------
async function searchLeadByEmail(email) {
  const emailSafe = String(email || "").trim();
  if (!emailSafe) return null;

  const criteria = `(Email:equals:${emailSafe})`;
  const resp = await zohoRequest("GET", `/crm/v2/${ZOHO_LEADS_MODULE}/search`, {
    params: { criteria },
  });

  const data = resp?.data?.data;
  if (Array.isArray(data) && data.length > 0) return data[0];
  return null;
}

async function searchLeadByPhone(phoneDigits) {
  const phoneSafe = digitsOnly(phoneDigits);
  if (!phoneSafe) return null;

  // Search phone stored in Phone or Mobile
  const criteria = `(Phone:equals:${phoneSafe}) or (Mobile:equals:${phoneSafe})`;
  const resp = await zohoRequest("GET", `/crm/v2/${ZOHO_LEADS_MODULE}/search`, {
    params: { criteria },
  });

  const data = resp?.data?.data;
  if (Array.isArray(data) && data.length > 0) return data[0];
  return null;
}

async function searchLeadBySessionId(sessionId) {
  const s = String(sessionId || "").trim();
  if (!s) return null;

  const criteria = `(Session_ID:equals:${s})`;
  const resp = await zohoRequest("GET", `/crm/v2/${ZOHO_LEADS_MODULE}/search`, {
    params: { criteria },
  });

  const data = resp?.data?.data;
  if (Array.isArray(data) && data.length > 0) return data[0];
  return null;
}

// -------------------------
// ZOHO CRUD HELPERS
// -------------------------
async function createLead(zohoLeadPayload) {
  const resp = await zohoRequest("POST", `/crm/v2/${ZOHO_LEADS_MODULE}`, {
    data: { data: [zohoLeadPayload] },
  });

  const created = resp?.data?.data?.[0];
  const id = created?.details?.id;
  if (!id) throw new Error("Zoho createLead failed (no id returned).");
  return id;
}

async function updateLead(leadId, zohoLeadPayload) {
  const resp = await zohoRequest("PUT", `/crm/v2/${ZOHO_LEADS_MODULE}/${leadId}`, {
    data: { data: [zohoLeadPayload] },
  });

  const updated = resp?.data?.data?.[0];
  if (updated?.status !== "success") {
    throw new Error(`Zoho updateLead failed: ${JSON.stringify(updated || {})}`);
  }
  return true;
}

// -------------------------
// ROUTES
// -------------------------
app.get("/health", (req, res) => res.json({ ok: true }));

/**
 * POST /api/submissions
 *
 * PARTIAL:
 * - Email priority:
 *   1) search by Email
 *   2) if none, search by Phone/Mobile
 * - Update if found else create
 *
 * COMPLETE:
 * - Priority:
 *   1) search by Session_ID
 *   2) if none, search by Email
 *   3) if none, search by Phone/Mobile
 * - If found -> update
 * - If not found -> create (so you never lose the completed intake)
 * - Always sets Session_ID on update/create (link future updates)
 */
app.post("/api/submissions", async (req, res) => {
  try {
    const submission = req.body || {};
    const submissionType = String(submission.submission_type || "").toLowerCase();

    if (!["partial", "complete"].includes(submissionType)) {
      return res.status(400).json({
        success: false,
        error: "submission_type must be 'partial' or 'complete'.",
      });
    }

    const sessionId = String(submission.session_id || "").trim();
    const email = String(submission.email || "").trim();
    const phoneDigits = formatZohoPhone(submission.phone_country_code, submission.phone_number);

    // Require at least ONE identifier for any submission
    if (!sessionId && !email && !digitsOnly(phoneDigits)) {
      return res.status(400).json({
        success: false,
        error: "Submission requires at least one identifier: session_id, email, or phone.",
      });
    }

    if (submissionType === "partial") {
      // For partial, session_id is required for linking
      if (!sessionId) {
        return res.status(400).json({ success: false, error: "Partial submission requires session_id." });
      }

      // EMAIL PRIORITY
      let lead = null;
      if (email) lead = await searchLeadByEmail(email);
      if (!lead && digitsOnly(phoneDigits)) lead = await searchLeadByPhone(phoneDigits);

      const zohoPayload = mapPartialToZohoLead(submission);

      if (lead?.id) {
        await updateLead(lead.id, zohoPayload);
      } else {
        await createLead(zohoPayload);
      }

      return res.json({ success: true });
    }

    // COMPLETE submission
    let lead = null;

    // 1) session id
    if (sessionId) lead = await searchLeadBySessionId(sessionId);

    // 2) fallback: email
    if (!lead && email) lead = await searchLeadByEmail(email);

    // 3) fallback: phone
    if (!lead && digitsOnly(phoneDigits)) lead = await searchLeadByPhone(phoneDigits);

    const zohoPayload = mapCompleteToZohoLead(submission);

    if (lead?.id) {
      // Ensure Session_ID is set in case we matched by email/phone
      if (sessionId) {
        await updateLead(lead.id, pruneEmpty({ ...zohoPayload, Session_ID: sessionId }));
      } else {
        await updateLead(lead.id, zohoPayload);
      }
    } else {
      // No match at all: create new lead so intake is not lost
      // If Last_Name is required in your Zoho, ensure frontend always sends it.
      await createLead(zohoPayload);
    }

    return res.json({ success: true });
  } catch (err) {
    // Do NOT include req.body in logs (PHI)
    console.error("[POST /api/submissions] error:", err?.message || err);
    return res.status(500).json({ success: false, error: "Server error." });
  }
});

// -------------------------
// START
// -------------------------
const port = process.env.PORT || 10000;
app.listen(port, () => {
  console.log(`API listening on port ${port}`);
});
