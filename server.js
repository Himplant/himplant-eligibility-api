import express from "express";
import cors from "cors";
import fetch from "node-fetch";

const app = express();
app.use(express.json({ limit: "1mb" }));

// Keep EXACTLY like your old working setup for now:
app.use(cors({ origin: "*", methods: ["GET", "POST", "OPTIONS", "PUT"] }));

// Zoho US
const ZOHO_ACCOUNTS = "https://accounts.zoho.com";
const ZOHO_API_BASE = "https://www.zohoapis.com";

const { ZOHO_CLIENT_ID, ZOHO_CLIENT_SECRET, ZOHO_REFRESH_TOKEN } = process.env;

// Zoho Surgeons module and field API names (CONFIRMED)
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

// Zoho Leads module
const MODULE_LEADS = "Leads";

// Access token cache
let cachedAccessToken = null;
let tokenExpiresAt = 0;

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

async function zohoGET(path) {
  const token = await getAccessToken();
  const res = await fetch(`${ZOHO_API_BASE}${path}`, {
    headers: { Authorization: `Zoho-oauthtoken ${token}` },
  });

  const text = await res.text();
  let data;
  try {
    data = JSON.parse(text);
  } catch {
    data = { raw: text };
  }
  if (!res.ok) throw new Error(`Zoho API error ${res.status}: ${JSON.stringify(data)}`);
  return data;
}

async function zohoPOST(path, body) {
  const token = await getAccessToken();
  const res = await fetch(`${ZOHO_API_BASE}${path}`, {
    method: "POST",
    headers: {
      Authorization: `Zoho-oauthtoken ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });

  const text = await res.text();
  let data;
  try {
    data = JSON.parse(text);
  } catch {
    data = { raw: text };
  }
  if (!res.ok) throw new Error(`Zoho API error ${res.status}: ${JSON.stringify(data)}`);
  return data;
}

async function zohoPUT(path, body) {
  const token = await getAccessToken();
  const res = await fetch(`${ZOHO_API_BASE}${path}`, {
    method: "PUT",
    headers: {
      Authorization: `Zoho-oauthtoken ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });

  const text = await res.text();
  let data;
  try {
    data = JSON.parse(text);
  } catch {
    data = { raw: text };
  }
  if (!res.ok) throw new Error(`Zoho API error ${res.status}: ${JSON.stringify(data)}`);
  return data;
}

// -------------------------
// Small helpers (no PHI logging)
// -------------------------
function digitsOnly(s) {
  return String(s || "").replace(/\D/g, "");
}

function formatZohoPhone(phone_country_code, phone_number) {
  const cc = digitsOnly(phone_country_code); // "+1" -> "1"
  const pn = digitsOnly(phone_number);
  if (!cc && !pn) return "";
  return `${cc}${pn}`; // digits only
}

function safeJoinArray(arr) {
  if (!Array.isArray(arr)) return "";
  return arr.map((x) => String(x || "").trim()).filter(Boolean).join(", ");
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

app.get("/health", (req, res) => res.json({ ok: true }));

// -------------------------
// CMS ENDPOINTS (UNCHANGED BEHAVIOR)
// -------------------------

// A) Active countries
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

// A2) Active states for a country (US only)
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

// B) Active cities for a country (optionally filtered by US state)
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

// C) Surgeons for country+city (includes price). For US, optionally filter by state.
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
        `(${FIELD_ACTIVE}:equals:true) and (${FIELD_COUNTRY}:equals:${country}) and (${FIELD_STATE}:equals:${state}) and (${FIELD_CITY}:equals:${city})`;
    }

    const data = await zohoGET(`/crm/v2/${MODULE_SURGEONS}/search?criteria=${encodeURIComponent(criteria)}`);

    const surgeons = (data?.data || []).map((r) => {
      const bookingUrl = pickBookingUrl(r, lang);
      return {
        id: r.id,
        name: r?.[FIELD_NAME] || "",
        price: r?.[FIELD_PRICE] ?? null,
        // keep your old boolean plus ALSO provide bookingUrl (helps your new UI)
        bookingAvailable: !!bookingUrl,
        bookingUrl: bookingUrl || null,
      };
    });

    res.json(surgeons);
  } catch (e) {
    res.status(500).json({ error: "surgeons lookup failed", details: String(e.message || e) });
  }
});

// D) Selected surgeon details (price + booking link)
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
// NEW: SUBMISSIONS ENDPOINT
// -------------------------

async function searchLeadByEmail(email) {
  const e = String(email || "").trim();
  if (!e) return null;

  const criteria = `(Email:equals:${e})`;
  const data = await zohoGET(`/crm/v2/${MODULE_LEADS}/search?criteria=${encodeURIComponent(criteria)}`);
  return (data?.data || [])[0] || null;
}

async function searchLeadByPhone(phoneDigits) {
  const p = digitsOnly(phoneDigits);
  if (!p) return null;

  const criteria = `(Phone:equals:${p}) or (Mobile:equals:${p})`;
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

function mapPartialToLead(submission) {
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
    Intake_Date: submission.submitted_at || new Date().toISOString(),
  });
}

function mapCompleteToLead(submission) {
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

    Circumcised: submission.circumcised,
    Tobacco: submission.tobacco_use,
    Body_Type: submission.body_type || "",

    ED_history: submission.ed_history || "",
    Can_maintain_erection: submission.ed_maintain_with_or_without_meds || "",

    Active_STD: submission.active_std,
    STD_list: safeJoinArray(submission.std_list),
    Recent_Outbreak: submission.recent_outbreak_6mo,

    Previous_Penis_Surgeries: submission.prior_procedures,
    Medical_conditions_list: safeJoinArray(submission.medical_conditions_list),

    Outcome: submission.outcome || "",

    Date_of_Birth: submission.date_of_birth || null,
    Intake_Date: submission.submitted_at || new Date().toISOString(),
  });
}

app.post("/api/submissions", async (req, res) => {
  try {
    const submission = req.body || {};
    const type = String(submission.submission_type || "").toLowerCase();

    if (type !== "partial" && type !== "complete") {
      return res.status(400).json({ success: false, error: "submission_type must be 'partial' or 'complete'." });
    }

    const sessionId = String(submission.session_id || "").trim();
    const email = String(submission.email || "").trim();
    const phoneDigits = formatZohoPhone(submission.phone_country_code, submission.phone_number);

    if (type === "partial" && !sessionId) {
      return res.status(400).json({ success: false, error: "Partial submission requires session_id." });
    }

    if (!sessionId && !email && !digitsOnly(phoneDigits)) {
      return res.status(400).json({ success: false, error: "Need at least one identifier: session_id, email, or phone." });
    }

    if (type === "partial") {
      // EMAIL PRIORITY, then phone
      let lead = null;
      if (email) lead = await searchLeadByEmail(email);
      if (!lead && digitsOnly(phoneDigits)) lead = await searchLeadByPhone(phoneDigits);

      const payload = mapPartialToLead(submission);

      if (lead?.id) await updateLead(lead.id, payload);
      else await createLead(payload);

      return res.json({ success: true });
    }

    // COMPLETE: session -> email -> phone
    let lead = null;
    if (sessionId) lead = await searchLeadBySessionId(sessionId);
    if (!lead && email) lead = await searchLeadByEmail(email);
    if (!lead && digitsOnly(phoneDigits)) lead = await searchLeadByPhone(phoneDigits);

    let payload = mapCompleteToLead(submission);
    if (sessionId) payload = pruneEmpty({ ...payload, Session_ID: sessionId });

    if (lead?.id) await updateLead(lead.id, payload);
    else await createLead(payload);

    return r
