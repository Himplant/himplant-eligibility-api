import express from "express";
import cors from "cors";
import fetch from "node-fetch";

const app = express();

// Keep payload size reasonable (PHI safety + abuse protection)
app.use(express.json({ limit: "1mb" }));

/**
 * HIPAA best practices:
 * - Restrict CORS to known production origins
 * - Do not log request bodies (PHI)
 */
const ALLOWED_ORIGINS = [
  "https://eligibility.himplant.com",
  "https://himplant.com",
  "https://www.himplant.com",
];

app.use(
  cors({
    origin: function (origin, callback) {
      // Allow no-origin (e.g., curl/postman)
      if (!origin) return callback(null, true);
      if (ALLOWED_ORIGINS.includes(origin)) return callback(null, true);
      return callback(new Error("Not allowed by CORS"));
    },
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
  })
);

// Zoho US endpoints
const ZOHO_ACCOUNTS = "https://accounts.zoho.com";
const ZOHO_API_BASE = "https://www.zohoapis.com";

// Env
const { ZOHO_CLIENT_ID, ZOHO_CLIENT_SECRET, ZOHO_REFRESH_TOKEN } = process.env;

// Modules
const MODULE_SURGEONS = "Surgeons";
const MODULE_LEADS = "Leads";

// Surgeons fields (Zoho API names)
const FIELD_ACTIVE = "Active_Status";
const FIELD_COUNTRY = "Country";
const FIELD_STATE = "State";
const FIELD_CITY = "City";
const FIELD_NAME = "Name";
const FIELD_PRICE = "Surgery_Price";
const FIELD_BOOK_EN = "Consult_Booking_EN";
const FIELD_BOOK_ES = "Consult_Booking_ES";
const FIELD_BOOK_AR = "Consult_Booking_AR";

// Access token cache
let cachedAccessToken = null;
let tokenExpiresAt = 0;

// -------------------------
// Helpers (PHI-safe)
// -------------------------
function digitsOnly(s) {
  return String(s || "").replace(/\D/g, "");
}

function pruneEmpty(obj) {
  const out = {};
  for (const [k, v] of Object.entries(obj)) {
    if (v === undefined || v === null) continue;
    if (typeof v === "string" && v.trim() === "") continue;
    out[k] = v;
  }
  return out;
}

function nullableText(v) {
  if (v === undefined || v === null) return null;
  const s = String(v).trim();
  return s === "" ? null : s;
}

/**
 * Convert booleans (or "true"/"false") to Yes/No strings.
 * Useful when Zoho fields are configured as Text or Picklist.
 */
function boolToYesNo(v) {
  if (v === true) return "Yes";
  if (v === false) return "No";
  if (typeof v === "string") {
    const s = v.trim().toLowerCase();
    if (s === "yes") return "Yes";
    if (s === "no") return "No";
    if (s === "true") return "Yes";
    if (s === "false") return "No";
  }
  return null;
}

/**
 * Zoho "jsonarray" fields MUST be a real array of strings.
 * Handles:
 * - array: ["HPV","HIV/AIDS"]
 * - JSON string: '["HPV","HIV/AIDS"]'
 * - comma string: "HPV, HIV/AIDS"
 */
function toZohoJsonArray(value) {
  if (value === undefined || value === null) return null;

  if (Array.isArray(value)) {
    const clean = value.map((x) => String(x || "").trim()).filter(Boolean);
    return clean.length ? clean : null;
  }

  if (typeof value === "string") {
    const s = value.trim();
    if (!s) return null;

    // Try JSON array string
    if (s.startsWith("[") && s.endsWith("]")) {
      try {
        const parsed = JSON.parse(s);
        if (Array.isArray(parsed)) {
          const clean = parsed.map((x) => String(x || "").trim()).filter(Boolean);
          return clean.length ? clean : null;
        }
      } catch {
        // fall through
      }
    }

    // Comma-separated string fallback
    const parts = s.split(",").map((x) => x.trim()).filter(Boolean);
    return parts.length ? parts : null;
  }

  const s = String(value).trim();
  return s ? [s] : null;
}

/**
 * Multiline text helper (Zoho multiline field).
 * If array -> newline separated.
 */
function toMultilineText(value) {
  if (value === undefined || value === null) return null;

  if (typeof value === "string") {
    const s = value.trim();
    return s ? s : null;
  }

  if (Array.isArray(value)) {
    const lines = value.map((x) => String(x || "").trim()).filter(Boolean);
    return lines.length ? lines.join("\n") : null;
  }

  const s = String(value).trim();
  return s ? s : null;
}

/**
 * Phone normalization to E.164: +<countrycode><number> (no spaces)
 */
const ISO_TO_DIAL = {
  US: "1",
  CA: "1",
  MX: "52",
  CO: "57",
  AE: "971",
  SA: "966",
  JO: "962",
  GB: "44",
  ES: "34",
  FR: "33",
  DE: "49",
  IT: "39",
  BR: "55",
  AR: "54",
  CL: "56",
  PE: "51",
  EC: "593",
  PA: "507",
  CR: "506",
  DO: "1",
};

function normalizePhoneE164(countryCodeOrDial, phoneNumber) {
  const raw = String(phoneNumber || "").trim();

  // Already E.164
  if (raw.startsWith("+")) {
    const cleaned = "+" + digitsOnly(raw);
    return cleaned.length > 1 ? cleaned : "";
  }

  const pn = digitsOnly(raw);
  const ccRaw = String(countryCodeOrDial || "").trim().toUpperCase();

  const dial =
    ISO_TO_DIAL[ccRaw] ||
    (digitsOnly(ccRaw) ? digitsOnly(ccRaw) : "");

  if (!dial && !pn) return "";
  if (!dial) return pn ? `+${pn}` : "";
  return pn ? `+${dial}${pn}` : `+${dial}`;
}

function getCurrentCountry(submission) {
  return (
    submission.current_location_country ||
    submission.location_country ||
    submission.country ||
    ""
  );
}

function getCurrentState(submission) {
  return (
    submission.current_location_state ||
    submission.location_state ||
    submission.state ||
    ""
  );
}

function pickBookingUrl(record, lang) {
  const l = String(lang || "en").toLowerCase();
  if (l === "es") return record?.[FIELD_BOOK_ES] || record?.[FIELD_BOOK_EN] || "";
  if (l === "ar") return record?.[FIELD_BOOK_AR] || record?.[FIELD_BOOK_EN] || "";
  return record?.[FIELD_BOOK_EN] || "";
}

// -------------------------
// Zoho Auth + Requests
// -------------------------
async function getAccessToken() {
  const now = Date.now();
  if (cachedAccessToken && now < tokenExpiresAt - 60_000) return cachedAccessToken;

  if (!ZOHO_CLIENT_ID || !ZOHO_CLIENT_SECRET || !ZOHO_REFRESH_TOKEN) {
    throw new Error("Missing Zoho env vars");
  }

  const params = new URLSearchParams({
    refresh_token: ZOHO_REFRESH_TOKEN,
    client_id: ZOHO_CLIENT_ID,
    client_secret: ZOHO_CLIENT_SECRET,
    grant_type: "refresh_token",
  });

  const res = await fetch(`${ZOHO_ACCOUNTS}/oauth/v2/token?${params.toString()}`, {
    method: "POST",
  });

  const data = await res.json().catch(() => ({}));
  if (!res.ok || !data.access_token) {
    throw new Error(`Zoho token refresh failed: ${JSON.stringify(data)}`);
  }

  cachedAccessToken = data.access_token;
  tokenExpiresAt = Date.now() + (data.expires_in || 3600) * 1000;
  return cachedAccessToken;
}

async function zohoRequest(method, path, body) {
  const token = await getAccessToken();
  const res = await fetch(`${ZOHO_API_BASE}${path}`, {
    method,
    headers: {
      Authorization: `Zoho-oauthtoken ${token}`,
      ...(body ? { "Content-Type": "application/json" } : {}),
    },
    body: body ? JSON.stringify(body) : undefined,
  });

  const text = await res.text();
  let data;
  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    data = { raw: text };
  }

  if (!res.ok) throw new Error(`Zoho API error ${res.status}: ${JSON.stringify(data)}`);
  return data;
}

const zohoGET = (path) => zohoRequest("GET", path);
const zohoPOST = (path, body) => zohoRequest("POST", path, body);
const zohoPUT = (path, body) => zohoRequest("PUT", path, body);

// -------------------------
// Health
// -------------------------
app.get("/health", (req, res) => res.json({ ok: true }));

// -------------------------
// CMS: Countries
// -------------------------
app.get("/api/geo/countries", async (req, res) => {
  try {
    const criteria = `(${FIELD_ACTIVE}:equals:true)`;
    const data = await zohoGET(
      `/crm/v2/${MODULE_SURGEONS}/search?criteria=${encodeURIComponent(criteria)}`
    );

    const countries = new Set();
    for (const r of data?.data || []) {
      const c = (r?.[FIELD_COUNTRY] || "").toString().trim();
      if (c) countries.add(c);
    }
    res.json(Array.from(countries).sort((a, b) => a.localeCompare(b)));
  } catch {
    res.status(500).json({ error: "countries lookup failed" });
  }
});

// -------------------------
// CMS: States (US only)
// -------------------------
app.get("/api/geo/states", async (req, res) => {
  try {
    const country = (req.query.country || "").toString().trim();
    if (!country) return res.status(400).json({ error: "country is required" });
    if (country !== "United States") return res.json([]);

    const criteria = `(${FIELD_ACTIVE}:equals:true) and (${FIELD_COUNTRY}:equals:${country})`;
    const data = await zohoGET(
      `/crm/v2/${MODULE_SURGEONS}/search?criteria=${encodeURIComponent(criteria)}`
    );

    const states = new Set();
    for (const r of data?.data || []) {
      const st = (r?.[FIELD_STATE] || "").toString().trim();
      if (st) states.add(st);
    }
    res.json(Array.from(states).sort((a, b) => a.localeCompare(b)));
  } catch {
    res.status(500).json({ error: "states lookup failed" });
  }
});

// -------------------------
// CMS: Cities
// -------------------------
app.get("/api/geo/cities", async (req, res) => {
  try {
    const country = (req.query.country || "").toString().trim();
    const state = (req.query.state || "").toString().trim();
    if (!country) return res.status(400).json({ error: "country is required" });

    let criteria = `(${FIELD_ACTIVE}:equals:true) and (${FIELD_COUNTRY}:equals:${country})`;
    if (country === "United States" && state) {
      criteria =
        `(${FIELD_ACTIVE}:equals:true) and (${FIELD_COUNTRY}:equals:${country}) and (${FIELD_STATE}:equals:${state})`;
    }

    const data = await zohoGET(
      `/crm/v2/${MODULE_SURGEONS}/search?criteria=${encodeURIComponent(criteria)}`
    );

    const cities = new Set();
    for (const r of data?.data || []) {
      const city = (r?.[FIELD_CITY] || "").toString().trim();
      if (city) cities.add(city);
    }
    res.json(Array.from(cities).sort((a, b) => a.localeCompare(b)));
  } catch {
    res.status(500).json({ error: "cities lookup failed" });
  }
});

// -------------------------
// CMS: Surgeons list
// -------------------------
app.get("/api/surgeons", async (req, res) => {
  try {
    const country = (req.query.country || "").toString().trim();
    const state = (req.query.state || "").toString().trim();
    const city = (req.query.city || "").toString().trim();
    const lang = (req.query.lang || "en").toString().trim().toLowerCase();

    if (!country) return res.status(400).json({ error: "country is required" });
    if (!city) return res.status(400).json({ error: "city is required" });

    let criteria =
      `(${FIELD_ACTIVE}:equals:true) and (${FIELD_COUNTRY}:equals:${country}) and (${FIELD_CITY}:equals:${city})`;
    if (country === "United States" && state) {
      criteria =
        `(${FIELD_ACTIVE}:equals:true) and (${FIELD_COUNTRY}:equals:${country}) and ` +
        `(${FIELD_STATE}:equals:${state}) and (${FIELD_CITY}:equals:${city})`;
    }

    const data = await zohoGET(
      `/crm/v2/${MODULE_SURGEONS}/search?criteria=${encodeURIComponent(criteria)}`
    );

    const surgeons = (data?.data || []).map((r) => {
      const bookingUrl = pickBookingUrl(r, lang);
      return {
        id: r.id,
        name: r?.[FIELD_NAME] || "",
        price: r?.[FIELD_PRICE] ?? null,
        bookingAvailable: !!bookingUrl,
        bookingUrl: bookingUrl || null,
      };
    });

    res.json(surgeons);
  } catch {
    res.status(500).json({ error: "surgeons lookup failed" });
  }
});

// -------------------------
// CMS: Surgeon detail
// -------------------------
app.get("/api/surgeons/:id", async (req, res) => {
  try {
    const surgeonId = (req.params.id || "").toString().trim();
    const lang = (req.query.lang || "en").toString().trim().toLowerCase();
    if (!surgeonId) return res.status(400).json({ error: "surgeonId required" });

    const record = await zohoGET(`/crm/v2/${MODULE_SURGEONS}/${surgeonId}`);
    const s = record?.data?.[0];
    if (!s) return res.status(404).json({ error: "surgeon not found" });

    const bookingUrl = pickBookingUrl(s, lang);

    res.json({
      id: surgeonId,
      name: s[FIELD_NAME] || "",
      price: s[FIELD_PRICE] ?? null,
      bookingUrl: bookingUrl || null,
    });
  } catch {
    res.status(500).json({ error: "surgeon lookup failed" });
  }
});

// -------------------------
// Lead search helpers
// -------------------------
async function searchLeadByEmail(email) {
  const e = String(email || "").trim();
  if (!e) return null;
  const criteria = `(Email:equals:${e})`;
  const data = await zohoGET(
    `/crm/v2/${MODULE_LEADS}/search?criteria=${encodeURIComponent(criteria)}`
  );
  return (data?.data || [])[0] || null;
}

async function searchLeadBySessionId(sessionId) {
  const s = String(sessionId || "").trim();
  if (!s) return null;
  const criteria = `(Session_ID:equals:${s})`;
  const data = await zohoGET(
    `/crm/v2/${MODULE_LEADS}/search?criteria=${encodeURIComponent(criteria)}`
  );
  return (data?.data || [])[0] || null;
}

async function searchLeadByPhoneE164(phoneE164) {
  const p = String(phoneE164 || "").trim();
  if (!p) return null;
  const criteria = `(Phone:equals:${p}) or (Mobile:equals:${p})`;
  const data = await zohoGET(
    `/crm/v2/${MODULE_LEADS}/search?criteria=${encodeURIComponent(criteria)}`
  );
  return (data?.data || [])[0] || null;
}

async function createLead(payload) {
  const resp = await zohoPOST(`/crm/v2/${MODULE_LEADS}`, { data: [payload] });
  const id = resp?.data?.[0]?.details?.id;
  if (!id) throw new Error("createLead failed (no id returned)");
  return id;
}

async function updateLead(leadId, payload) {
  const resp = await zohoPUT(`/crm/v2/${MODULE_LEADS}/${leadId}`, { data: [payload] });
  const status = resp?.data?.[0]?.status;
  if (status !== "success") throw new Error(`updateLead failed: ${JSON.stringify(resp?.data?.[0] || {})}`);
  return true;
}

// -------------------------
// Mapping to Zoho Leads API names (your list)
// -------------------------
function mapLeadToZohoLead(submission) {
  const phone = normalizePhoneE164(submission.phone_country_code, submission.phone_number);

  return pruneEmpty({
    First_Name: nullableText(submission.first_name),
    Last_Name: nullableText(submission.last_name),
    Email: nullableText(submission.email),

    Phone: phone || null,
    Mobile: phone || null,

    Country: nullableText(getCurrentCountry(submission)),
    State: nullableText(getCurrentState(submission)),

    Session_ID: nullableText(submission.session_id),
    Surgeon_name_Lookup: nullableText(submission.surgeon_id),

    Intake_Date: nullableText(submission.submitted_at) || new Date().toISOString(),
  });
}

function mapPartialToZohoLead(submission) {
  const phone = normalizePhoneE164(submission.phone_country_code, submission.phone_number);

  return pruneEmpty({
    First_Name: nullableText(submission.first_name),
    Last_Name: nullableText(submission.last_name),
    Email: nullableText(submission.email),

    Phone: phone || null,
    Mobile: phone || null,

    Country: nullableText(getCurrentCountry(submission)),
    State: nullableText(getCurrentState(submission)),

    Session_ID: nullableText(submission.session_id),

    Date_of_Birth: nullableText(submission.date_of_birth),
    Intake_Date: nullableText(submission.submitted_at) || new Date().toISOString(),
  });
}

function mapCompleteToZohoLead(submission) {
  const phone = normalizePhoneE164(submission.phone_country_code, submission.phone_number);

  return pruneEmpty({
    // Identity
    First_Name: nullableText(submission.first_name),
    Last_Name: nullableText(submission.last_name),
    Email: nullableText(submission.email),
    Phone: phone || null,
    Mobile: phone || null,
    Date_of_Birth: nullableText(submission.date_of_birth),

    // Location
    Country: nullableText(getCurrentCountry(submission)),
    State: nullableText(getCurrentState(submission)),

    // Linker
    Session_ID: nullableText(submission.session_id),

    // Lookup
    Surgeon_name_Lookup: nullableText(submission.surgeon_id),

    // Intake fields
    Payment_Method: nullableText(submission.payment_method),
    Procedure_Timeline: nullableText(submission.timeline),

    // boolean-looking fields (Zoho text/picklist safe)
    Circumcised: boolToYesNo(submission.circumcised),
    Tobacco: boolToYesNo(submission.tobacco_use),
    ED_history: boolToYesNo(submission.ed_history),
    Active_STD: boolToYesNo(submission.active_std),

    Can_maintain_erection:
      submission.ed_maintain_with_or_without_meds === null || submission.ed_maintain_with_or_without_meds === undefined
        ? null
        : boolToYesNo(submission.ed_maintain_with_or_without_meds),

    // ✅ jsonarray fields
    STD_list: toZohoJsonArray(submission.std_list),
    Previous_Penis_Surgeries: toZohoJsonArray(submission.prior_procedure_list),

    Recent_Outbreak:
      submission.recent_outbreak_6mo === null || submission.recent_outbreak_6mo === undefined
        ? null
        : boolToYesNo(submission.recent_outbreak_6mo),

    // Multiline text field
    Medical_conditions_list: toMultilineText(submission.medical_conditions_list),

    Body_Type: nullableText(submission.body_type),
    Outcome: nullableText(submission.outcome),

    Intake_Date: nullableText(submission.submitted_at) || new Date().toISOString(),
  });
}

// -------------------------
// Submissions endpoint
// - lead: create/update by Email -> Phone; always set Session_ID
// - partial: create/update by Email -> Phone
// - complete: update by Session_ID -> Email -> Phone; if matched by email/phone, bind Session_ID
// -------------------------
app.post("/api/submissions", async (req, res) => {
  try {
    const submission = req.body || {};
    const type = String(submission.submission_type || "").toLowerCase();

    if (!["lead", "partial", "complete"].includes(type)) {
      return res.status(400).json({
        success: false,
        error: "submission_type must be 'lead', 'partial', or 'complete'.",
      });
    }

    const sessionId = String(submission.session_id || "").trim();
    const email = String(submission.email || "").trim();
    const phoneE164 = normalizePhoneE164(submission.phone_country_code, submission.phone_number);

    // Validate minimal identifiers
    if ((type === "lead" || type === "complete") && !sessionId) {
      return res.status(400).json({ success: false, error: `${type} submission requires session_id.` });
    }

    if (!sessionId && !email && !phoneE164) {
      return res.status(400).json({ success: false, error: "Need session_id, email, or phone." });
    }

    if (type === "lead") {
      let lead = null;
      if (email) lead = await searchLeadByEmail(email);
      if (!lead && phoneE164) lead = await searchLeadByPhoneE164(phoneE164);

      const payload = mapLeadToZohoLead(submission);

      if (lead?.id) await updateLead(lead.id, payload);
      else await createLead(payload);

      return res.json({ success: true });
    }

    if (type === "partial") {
      let lead = null;
      if (email) lead = await searchLeadByEmail(email);
      if (!lead && phoneE164) lead = await searchLeadByPhoneE164(phoneE164);

      const payload = mapPartialToZohoLead(submission);

      if (lead?.id) await updateLead(lead.id, payload);
      else await createLead(payload);

      return res.json({ success: true });
    }

    // COMPLETE: Session_ID -> Email -> Phone
    let lead = null;
    if (sessionId) lead = await searchLeadBySessionId(sessionId);
    if (!lead && email) lead = await searchLeadByEmail(email);
    if (!lead && phoneE164) lead = await searchLeadByPhoneE164(phoneE164);

    const payload = mapCompleteToZohoLead(submission);

    // Bind new session to existing lead if we matched by email/phone
    if (lead?.id && sessionId) payload.Session_ID = sessionId;

    if (lead?.id) await updateLead(lead.id, payload);
    else await createLead(payload);

    return res.json({ success: true });
  } catch (e) {
    // PHI-safe error logging (no req.body)
    console.error("[POST /api/submissions] error:", String(e.message || e));
    return res.status(500).json({ success: false, error: "submission failed" });
  }
});

const port = process.env.PORT || 10000;
app.listen(port, () => console.log(`API running on port ${port}`));
