import express from "express";
import cors from "cors";
import fetch from "node-fetch";

const app = express();
app.use(express.json({ limit: "1mb" }));

/**
 * CORS
 * During final hardening, restrict to:
 * - https://eligibility.himplant.com
 * - https://himplant.com
 * - https://www.himplant.com
 *
 * For now, you can keep permissive while testing.
 */
app.use(
  cors({
    origin: "*",
    methods: ["GET", "POST", "PUT", "OPTIONS"],
  })
);

// Zoho US
const ZOHO_ACCOUNTS = "https://accounts.zoho.com";
const ZOHO_API_BASE = "https://www.zohoapis.com";

const { ZOHO_CLIENT_ID, ZOHO_CLIENT_SECRET, ZOHO_REFRESH_TOKEN } = process.env;

// Surgeons module and fields (CONFIRMED)
const MODULE_SURGEONS = "Surgeons";
const FIELD_ACTIVE = "Active_Status";
const FIELD_COUNTRY = "Country";
const FIELD_STATE = "State";
const FIELD_CITY = "City";
const FIELD_NAME = "Name";
const FIELD_PRICE = "Surgery_Price";
const FIELD_BOOK_EN = "Consult_Booking_EN";
const FIELD_BOOK_ES = "Consult_Booking_ES";
const FIELD_BOOK_AR = "Consult_Booking_AR";

// Leads module
const MODULE_LEADS = "Leads";

// Token cache
let cachedAccessToken = null;
let tokenExpiresAt = 0;

// -------------------------
// Zoho Auth + Request Helpers
// -------------------------
async function getAccessToken() {
  const now = Date.now();
  if (cachedAccessToken && now < tokenExpiresAt - 60_000) return cachedAccessToken;

  if (!ZOHO_CLIENT_ID || !ZOHO_CLIENT_SECRET || !ZOHO_REFRESH_TOKEN) {
    throw new Error("Missing Zoho env vars: ZOHO_CLIENT_ID, ZOHO_CLIENT_SECRET, ZOHO_REFRESH_TOKEN");
  }

  const params = new URLSearchParams({
    refresh_token: ZOHO_REFRESH_TOKEN,
    client_id: ZOHO_CLIENT_ID,
    client_secret: ZOHO_CLIENT_SECRET,
    grant_type: "refresh_token",
  });

  const res = await fetch(`${ZOHO_ACCOUNTS}/oauth/v2/token?${params.toString()}`, { method: "POST" });
  const data = await res.json();

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
// Small helpers
// -------------------------
function digitsOnly(s) {
  return String(s || "").replace(/\D/g, "");
}

function safeJoinArray(arr) {
  if (!Array.isArray(arr)) return "";
  return arr
    .map((x) => String(x || "").trim())
    .filter(Boolean)
    .join(", ");
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

function pickBookingUrl(record, lang) {
  const l = String(lang || "en").toLowerCase();
  if (l === "es") return record?.[FIELD_BOOK_ES] || record?.[FIELD_BOOK_EN] || "";
  if (l === "ar") return record?.[FIELD_BOOK_AR] || record?.[FIELD_BOOK_EN] || "";
  return record?.[FIELD_BOOK_EN] || "";
}

/**
 * Phone normalization
 * Frontend may send country as ISO ("US", "MX") per the questionnaire doc :contentReference[oaicite:7]{index=7},
 * but Zoho needs +<dialcode><number> with no spaces per your requirement.
 *
 * We:
 * - Accept already-E.164 numbers (if phone_number includes '+')
 * - Otherwise map ISO -> dial code (common set) and fallback to digits-only countryCode
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

  // If number already looks like E.164 (+...), keep only '+' + digits
  if (raw.startsWith("+")) {
    const cleaned = "+" + digitsOnly(raw);
    return cleaned.length > 1 ? cleaned : "";
  }

  const pn = digitsOnly(raw);
  const ccRaw = String(countryCodeOrDial || "").trim().toUpperCase();

  // If country code is ISO, map it; if it's digits, use digits
  const dial =
    ISO_TO_DIAL[ccRaw] ||
    (digitsOnly(ccRaw) ? digitsOnly(ccRaw) : "");

  if (!dial && !pn) return "";
  if (!dial) {
    // If we don't have a dial code, we at least avoid returning a broken '+'
    return pn ? `+${pn}` : "";
  }
  return pn ? `+${dial}${pn}` : `+${dial}`;
}

function getCurrentCountry(submission) {
  return (
    submission.current_location_country ||
    submission.location_country ||
    ""
  );
}

function getCurrentState(submission) {
  return (
    submission.current_location_state ||
    submission.location_state ||
    ""
  );
}

// -------------------------
// Health
// -------------------------
app.get("/health", (req, res) => res.json({ ok: true }));

// -------------------------
// CMS Endpoints (existing behavior)
// -------------------------

// Countries
app.get("/api/geo/countries", async (req, res) => {
  try {
    const criteria = `(${FIELD_ACTIVE}:equals:true)`;
    const data = await zohoGET(`/crm/v2/${MODULE_SURGEONS}/search?criteria=${encodeURIComponent(criteria)}`);

    const countries = new Set();
    for (const r of data?.data || []) {
      const c = (r?.[FIELD_COUNTRY] || "").toString().trim();
      if (c) countries.add(c);
    }
    res.json(Array.from(countries).sort((a, b) => a.localeCompare(b)));
  } catch (e) {
    res.status(500).json({ error: "countries lookup failed", details: String(e.message || e) });
  }
});

// States (US only)
app.get("/api/geo/states", async (req, res) => {
  try {
    const country = (req.query.country || "").toString().trim();
    if (!country) return res.status(400).json({ error: "country is required" });
    if (country !== "United States") return res.json([]);

    const criteria = `(${FIELD_ACTIVE}:equals:true) and (${FIELD_COUNTRY}:equals:${country})`;
    const data = await zohoGET(`/crm/v2/${MODULE_SURGEONS}/search?criteria=${encodeURIComponent(criteria)}`);

    const states = new Set();
    for (const r of data?.data || []) {
      const st = (r?.[FIELD_STATE] || "").toString().trim();
      if (st) states.add(st);
    }
    res.json(Array.from(states).sort((a, b) => a.localeCompare(b)));
  } catch (e) {
    res.status(500).json({ error: "states lookup failed", details: String(e.message || e) });
  }
});

// Cities
app.get("/api/geo/cities", async (req, res) => {
  try {
    const country = (req.query.country || "").toString().trim();
    const state = (req.query.state || "").toString().trim();
    if (!country) return res.status(400).json({ error: "country is required" });

    let criteria = `(${FIELD_ACTIVE}:equals:true) and (${FIELD_COUNTRY}:equals:${country})`;
    if (country === "United States" && state) {
      criteria = `(${FIELD_ACTIVE}:equals:true) and (${FIELD_COUNTRY}:equals:${country}) and (${FIELD_STATE}:equals:${state})`;
    }

    const data = await zohoGET(`/crm/v2/${MODULE_SURGEONS}/search?criteria=${encodeURIComponent(criteria)}`);

    const cities = new Set();
    for (const r of data?.data || []) {
      const city = (r?.[FIELD_CITY] || "").toString().trim();
      if (city) cities.add(city);
    }
    res.json(Array.from(cities).sort((a, b) => a.localeCompare(b)));
  } catch (e) {
    res.status(500).json({ error: "cities lookup failed", details: String(e.message || e) });
  }
});

// Surgeons list
app.get("/api/surgeons", async (req, res) => {
  try {
    const country = (req.query.country || "").toString().trim();
    const state = (req.query.state || "").toString().trim();
    const city = (req.query.city || "").toString().trim();
    const lang = (req.query.lang || "en").toString().trim().toLowerCase();

    if (!country) return res.status(400).json({ error: "country is required" });
    if (!city) return res.status(400).json({ error: "city is required" });

    let criteria = `(${FIELD_ACTIVE}:equals:true) and (${FIELD_COUNTRY}:equals:${country}) and (${FIELD_CITY}:equals:${city})`;
    if (country === "United States" && state) {
      criteria = `(${FIELD_ACTIVE}:equals:true) and (${FIELD_COUNTRY}:equals:${country}) and (${FIELD_STATE}:equals:${state}) and (${FIELD_CITY}:equals:${city})`;
    }

    const data = await zohoGET(`/crm/v2/${MODULE_SURGEONS}/search?criteria=${encodeURIComponent(criteria)}`);

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
  } catch (e) {
    res.status(500).json({ error: "surgeons lookup failed", details: String(e.message || e) });
  }
});

// Surgeon detail
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
  } catch (e) {
    res.status(500).json({ error: "surgeon lookup failed", details: String(e.message || e) });
  }
});

// -------------------------
// Submissions (LEAD + PARTIAL + COMPLETE)
// -------------------------
async function searchLeadByEmail(email) {
  const e = String(email || "").trim();
  if (!e) return null;
  const criteria = `(Email:equals:${e})`;
  const data = await zohoGET(`/crm/v2/${MODULE_LEADS}/search?criteria=${encodeURIComponent(criteria)}`);
  return (data?.data || [])[0] || null;
}

async function searchLeadBySessionId(sessionId) {
  const s = String(sessionId || "").trim();
  if (!s) return null;
  const criteria = `(Session_ID:equals:${s})`;
  const data = await zohoGET(`/crm/v2/${MODULE_LEADS}/search?criteria=${encodeURIComponent(criteria)}`);
  return (data?.data || [])[0] || null;
}

async function searchLeadByPhoneE164(phoneE164) {
  const p = String(phoneE164 || "").trim();
  if (!p) return null;
  const criteria = `(Phone:equals:${p}) or (Mobile:equals:${p})`;
  const data = await zohoGET(`/crm/v2/${MODULE_LEADS}/search?criteria=${encodeURIComponent(criteria)}`);
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

/**
 * ZOHO FIELD API NAMES (per your list)
 * First_Name, Last_Name, Date_of_Birth, Email, Mobile, Phone,
 * Country, State, Surgeon_name_Lookup, Payment_Method, Procedure_Timeline,
 * Circumcised, Tobacco, ED_history, Can_maintain_erection, Active_STD,
 * STD_list, Recent_Outbreak, Previous_Penis_Surgeries, Medical_conditions_list,
 * Body_Type, Outcome, Intake_Date
 */

function mapLeadToZohoLead(submission) {
  const phone = normalizePhoneE164(submission.phone_country_code, submission.phone_number);
  return pruneEmpty({
    First_Name: submission.first_name || "",
    Last_Name: submission.last_name || "",
    Email: submission.email || "",
    Phone: phone,
    Mobile: phone,

    // "Country" and "State" should map to "current" per your request:
    Country: getCurrentCountry(submission),
    State: getCurrentState(submission),

    Session_ID: submission.session_id || "",

    // Landing may include surgeon/location selection:
    Surgeon_name_Lookup: submission.surgeon_id || "",
    Intake_Date: submission.submitted_at || new Date().toISOString(),
  });
}

function mapPartialToZohoLead(submission) {
  const phone = normalizePhoneE164(submission.phone_country_code, submission.phone_number);
  return pruneEmpty({
    First_Name: submission.first_name || "",
    Last_Name: submission.last_name || "",
    Email: submission.email || "",
    Phone: phone,
    Mobile: phone,

    Country: getCurrentCountry(submission),
    State: getCurrentState(submission),

    Session_ID: submission.session_id || "",
    Date_of_Birth: submission.date_of_birth || null,
    Intake_Date: submission.submitted_at || new Date().toISOString(),
  });
}

function mapCompleteToZohoLead(submission) {
  const phone = normalizePhoneE164(submission.phone_country_code, submission.phone_number);

  return pruneEmpty({
    First_Name: submission.first_name || "",
    Last_Name: submission.last_name || "",
    Email: submission.email || "",
    Phone: phone,
    Mobile: phone,

    Country: getCurrentCountry(submission),
    State: getCurrentState(submission),

    Session_ID: submission.session_id || "",

    Surgeon_name_Lookup: submission.surgeon_id || "",

    Payment_Method: submission.payment_method || "",
    Procedure_Timeline: submission.timeline || "",

    Circumcised: submission.circumcised,
    Tobacco: submission.tobacco_use,

    ED_history: submission.ed_history,
    Can_maintain_erection: submission.ed_maintain_with_or_without_meds,

    Active_STD: submission.active_std,
    STD_list: safeJoinArray(submission.std_list),
    Recent_Outbreak: submission.recent_outbreak_6mo,

    // IMPORTANT FIX: user requested this maps to prior_procedure_list (not the boolean)
    Previous_Penis_Surgeries: safeJoinArray(submission.prior_procedure_list),

    Medical_conditions_list: safeJoinArray(submission.medical_conditions_list),

    Body_Type: submission.body_type || "",

    Outcome: submission.outcome || "",

    Date_of_Birth: submission.date_of_birth || null,
    Intake_Date: submission.submitted_at || new Date().toISOString(),
  });
}

app.post("/api/submissions", async (req, res) => {
  try {
    const submission = req.body || {};
    const type = String(submission.submission_type || "").toLowerCase();

    if (type !== "lead" && type !== "partial" && type !== "complete") {
      return res.status(400).json({
        success: false,
        error: "submission_type must be 'lead', 'partial', or 'complete'.",
      });
    }

    const sessionId = String(submission.session_id || "").trim();
    const email = String(submission.email || "").trim();
    const phoneE164 = normalizePhoneE164(submission.phone_country_code, submission.phone_number);

    // Basic requirements
    if ((type === "lead" || type === "complete") && !sessionId) {
      return res.status(400).json({ success: false, error: `${type} submission requires session_id.` });
    }

    if (!sessionId && !email && !phoneE164) {
      return res.status(400).json({ success: false, error: "Need session_id, email, or phone." });
    }

    // -------------------------
    // LEAD: create/update early (Email first), always store Session_ID
    // -------------------------
    if (type === "lead") {
      let lead = null;

      // Per your spec: search by Email first; set Session_ID
      if (email) lead = await searchLeadByEmail(email);
      if (!lead && phoneE164) lead = await searchLeadByPhoneE164(phoneE164);

      const payload = mapLeadToZohoLead(submission);

      if (lead?.id) await updateLead(lead.id, payload);
      else await createLead(payload);

      return res.json({ success: true });
    }

    // -------------------------
    // PARTIAL: best-effort updates (Email first, then phone)
    // -------------------------
    if (type === "partial") {
      let lead = null;

      if (email) lead = await searchLeadByEmail(email);
      if (!lead && phoneE164) lead = await searchLeadByPhoneE164(phoneE164);

      const payload = mapPartialToZohoLead(submission);

      if (lead?.id) await updateLead(lead.id, payload);
      else await createLead(payload);

      return res.json({ success: true });
    }

    // -------------------------
    // COMPLETE: session_id first, then email, then phone
    // -------------------------
    let lead = null;

    if (sessionId) lead = await searchLeadBySessionId(sessionId);
    if (!lead && email) lead = await searchLeadByEmail(email);
    if (!lead && phoneE164) lead = await searchLeadByPhoneE164(phoneE164);

    const payload = mapCompleteToZohoLead(submission);

    if (lead?.id) await updateLead(lead.id, payload);
    else await createLead(payload);

    return res.json({ success: true });
  } catch (e) {
    console.error("[POST /api/submissions] error:", String(e.message || e));
    return res.status(500).json({ success: false, error: "submission failed" });
  }
});

const port = process.env.PORT || 10000;
app.listen(port, () => console.log(`API running on port ${port}`));
