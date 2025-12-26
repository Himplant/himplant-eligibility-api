import express from "express";
import cors from "cors";
import fetch from "node-fetch";

const app = express();

app.use(express.json({ limit: "1mb" }));

/**
 * CORS:
 * You are currently testing from a Lovable preview origin like:
 * https://id-preview--....lovable.app
 *
 * For TODAY (testing):
 * - allow production domains
 * - allow *.onrender.com
 * - allow *.webflow.io
 * - allow *.lovable.app  ✅
 *
 * For TOMORROW (after custom domains):
 * - remove the wildcard allowances and lock to your custom domains only.
 */
const STRICT_ALLOWED_ORIGINS = [
  "https://himplant.com",
  "https://www.himplant.com",
  "https://eligibility.himplant.com",
];

function originAllowedByWildcard(origin) {
  // allow subdomains for testing environments
  return (
    origin.endsWith(".onrender.com") ||
    origin.endsWith(".webflow.io") ||
    origin.endsWith(".lovable.app")
  );
}

app.use(
  cors({
    origin: (origin, callback) => {
      // Allow curl/server-to-server (no origin header)
      if (!origin) return callback(null, true);

      // Always allow production domains
      if (STRICT_ALLOWED_ORIGINS.includes(origin)) return callback(null, true);

      // Allow known preview/staging hosts (today)
      if (originAllowedByWildcard(origin)) return callback(null, true);

      return callback(new Error(`Not allowed by CORS: ${origin}`));
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

// In-memory token cache
let cachedAccessToken = null;
let cachedAccessTokenExpiryMs = 0;

// -------------------------
// HELPERS
// -------------------------
function digitsOnly(s) {
  return String(s || "").replace(/\D/g, "");
}

function formatZohoPhone(phoneCountryCode, phoneNumber) {
  const cc = digitsOnly(phoneCountryCode);
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
// ZOHO AUTH
// -------------------------
async function getZohoAccessToken() {
  const now = Date.now();
  if (cachedAccessToken && now < cachedAccessTokenExpiryMs - 60_000) {
    return cachedAccessToken;
  }

  const params = new URLSearchParams({
    refresh_token: process.env.ZOHO_REFRESH_TOKEN,
    client_id: process.env.ZOHO_CLIENT_ID,
    client_secret: process.env.ZOHO_CLIENT_SECRET,
    grant_type: "refresh_token",
  });

  const resp = await fetch(`${ZOHO_TOKEN_URL}?${params.toString()}`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
  });

  if (!resp.ok) {
    const text = await resp.text().catch(() => "");
    throw new Error(`Zoho token refresh failed: ${resp.status} ${text}`);
  }

  const data = await resp.json();
  const accessToken = data?.access_token;
  const expiresInSec = Number(data?.expires_in || 3600);

  if (!accessToken) {
    throw new Error("Zoho token refresh failed (no access_token returned).");
  }

  cachedAccessToken = accessToken;
  cachedAccessTokenExpiryMs = Date.now() + expiresInSec * 1000;
  return cachedAccessToken;
}

async function zohoRequest(method, path, { params, data } = {}) {
  const token = await getZohoAccessToken();
  const url = new URL(`${ZOHO_BASE}${path}`);

  if (params) {
    for (const [k, v] of Object.entries(params)) {
      if (v !== undefined && v !== null && String(v).trim() !== "") {
        url.searchParams.set(k, v);
      }
    }
  }

  const resp = await fetch(url.toString(), {
    method,
    headers: {
      Authorization: `Zoho-oauthtoken ${token}`,
      "Content-Type": "application/json",
    },
    body: data ? JSON.stringify(data) : undefined,
  });

  const text = await resp.text().catch(() => "");
  let json = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = null;
  }

  if (!resp.ok) throw new Error(`Zoho API error ${resp.status}: ${text}`);
  return json;
}

// -------------------------
// MAPPING
// -------------------------
function mapPartialToZohoLead(submission) {
  const phone = formatZohoPhone(submission.phone_country_code, submission.phone_number);
  return pruneEmpty({
    First_Name: submission.first_name || "",
    Last_Name: submission.last_name || "",
    Email: submission.email || "",
    Phone: phone,
    Mobile: phone,
    Country: submission.current_location_country || "",
    State: submission.current_location_state || "",
    Session_ID: submission.session_id || "",
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
    Country: submission.current_location_country || "",
    State: submission.current_location_state || "",
    Session_ID: submission.session_id || "",
    Surgeon_name_Lookup: submission.surgeon_id || "",
    Payment_Method: submission.payment_method || "",
    Procedure_Timeline: submission.timeline || "",
    Circumcised: toBooleanValue(submission.circumcised),
    Tobacco: toBooleanValue(submission.tobacco_use),
    Body_Type: submission.body_type || "",
    ED_history: submission.ed_history || "",
    Can_maintain_erection: submission.ed_maintain_with_or_without_meds || "",
    Active_STD: toBooleanValue(submission.active_std),
    STD_list: safeJoinArray(submission.std_list),
    Recent_Outbreak: toBooleanValue(submission.recent_outbreak_6mo),
    Previous_Penis_Surgeries: toBooleanValue(submission.prior_procedures),
    Medical_conditions_list: safeJoinArray(submission.medical_conditions_list),
    Outcome: submission.outcome || "",
    Date_of_Birth: submission.date_of_birth || null,
    Intake_Date: isoOrNow(submission.submitted_at),
  });
}

// -------------------------
// SEARCH + CRUD (email priority)
// -------------------------
async function searchLeadByEmail(email) {
  const e = String(email || "").trim();
  if (!e) return null;
  const json = await zohoRequest("GET", `/crm/v2/${ZOHO_LEADS_MODULE}/search`, {
    params: { criteria: `(Email:equals:${e})` },
  });
  return Array.isArray(json?.data) && json.data.length ? json.data[0] : null;
}

async function searchLeadByPhone(phoneDigits) {
  const p = digitsOnly(phoneDigits);
  if (!p) return null;
  const json = await zohoRequest("GET", `/crm/v2/${ZOHO_LEADS_MODULE}/search`, {
    params: { criteria: `(Phone:equals:${p}) or (Mobile:equals:${p})` },
  });
  return Array.isArray(json?.data) && json.data.length ? json.data[0] : null;
}

async function searchLeadBySessionId(sessionId) {
  const s = String(sessionId || "").trim();
  if (!s) return null;
  const json = await zohoRequest("GET", `/crm/v2/${ZOHO_LEADS_MODULE}/search`, {
    params: { criteria: `(Session_ID:equals:${s})` },
  });
  return Array.isArray(json?.data) && json.data.length ? json.data[0] : null;
}

async function createLead(payload) {
  const json = await zohoRequest("POST", `/crm/v2/${ZOHO_LEADS_MODULE}`, {
    data: { data: [payload] },
  });
  const id = json?.data?.[0]?.details?.id;
  if (!id) throw new Error("Zoho createLead failed (no id returned).");
  return id;
}

async function updateLead(leadId, payload) {
  const json = await zohoRequest("PUT", `/crm/v2/${ZOHO_LEADS_MODULE}/${leadId}`, {
    data: { data: [payload] },
  });
  if (json?.data?.[0]?.status !== "success") {
    throw new Error(`Zoho updateLead failed: ${JSON.stringify(json?.data?.[0] || {})}`);
  }
  return true;
}

// -------------------------
// ROUTES
// -------------------------
app.get("/health", (req, res) => res.json({ ok: true }));

app.post("/api/submissions", async (req, res) => {
  try {
    const submission = req.body || {};
    const submissionType = String(submission.submission_type || "").toLowerCase();

    if (!["partial", "complete"].includes(submissionType)) {
      return res.status(400).json({ success: false, error: "submission_type must be 'partial' or 'complete'." });
    }

    const sessionId = String(submission.session_id || "").trim();
    const email = String(submission.email || "").trim();
    const phoneDigits = formatZohoPhone(submission.phone_country_code, submission.phone_number);

    if (!sessionId && !email && !digitsOnly(phoneDigits)) {
      return res.status(400).json({ success: false, error: "Need session_id, email, or phone." });
    }

    if (submissionType === "partial") {
      if (!sessionId) return res.status(400).json({ success: false, error: "Partial requires session_id." });

      let lead = null;
      if (email) lead = await searchLeadByEmail(email); // email priority
      if (!lead && digitsOnly(phoneDigits)) lead = await searchLeadByPhone(phoneDigits);

      const payload = mapPartialToZohoLead(submission);
      if (lead?.id) await updateLead(lead.id, payload);
      else await createLead(payload);

      return res.json({ success: true });
    }

    // complete: session -> email -> phone
    let lead = null;
    if (sessionId) lead = await searchLeadBySessionId(sessionId);
    if (!lead && email) lead = await searchLeadByEmail(email);
    if (!lead && digitsOnly(phoneDigits)) lead = await searchLeadByPhone(phoneDigits);

    const payload = mapCompleteToZohoLead(submission);

    if (lead?.id) {
      await updateLead(lead.id, sessionId ? pruneEmpty({ ...payload, Session_ID: sessionId }) : payload);
    } else {
      await createLead(payload);
    }

    return res.json({ success: true });
  } catch (err) {
    console.error("[POST /api/submissions] error:", err?.message || err);
    return res.status(500).json({ success: false, error: "Server error." });
  }
});

const port = process.env.PORT || 10000;
app.listen(port, () => console.log(`API listening on port ${port}`));
