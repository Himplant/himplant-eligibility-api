import "dotenv/config";
import express from "express";
import cors from "cors";
import fetch from "node-fetch";

const app = express();
app.use(express.json({ limit: "5mb" }));

const ALLOWED_ORIGINS = [
  "https://eligibility.himplant.com",
  "https://himplant.com",
  "https://www.himplant.com",
  "https://get.himplant.com",
  "https://lovableproject.com",
  "https://lovable.dev",
  "https://bc248be2-fa03-4ded-a845-6db79ac12fb7.lovableproject.com",
  "https://himplanteligibility.lovable.app",
  "http://localhost:8080",
  "http://localhost:3000",
  "http://localhost:5173",
  "http://127.0.0.1:8080",
  "http://127.0.0.1:3000",
  "http://127.0.0.1:5173",
];

app.use(
  cors({
    origin(origin, callback) {
      if (!origin) return callback(null, true);
      if (ALLOWED_ORIGINS.includes(origin)) return callback(null, true);
      if (origin.startsWith("http://localhost:") || origin.startsWith("http://127.0.0.1:")) {
        return callback(null, true);
      }
      return callback(new Error("Not allowed by CORS"));
    },
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
  })
);

const ZOHO_ACCOUNTS = "https://accounts.zoho.com";
const ZOHO_API_BASE = "https://www.zohoapis.com";

const {
  ZOHO_CLIENT_ID,
  ZOHO_CLIENT_SECRET,
  ZOHO_REFRESH_TOKEN,
  CREATE_ZOHO_ERROR_TASKS,
  DEBUG_ZOHO,
  SUPABASE_ELIGIBILITY_API_KEY,
  SUPABASE_FUNCTION_URL,
  SUPABASE_ANON_KEY,
} = process.env;

const ELIGIBILITY_WEBHOOK_URL =
  SUPABASE_FUNCTION_URL || "https://nfoeswlppebvxaomfnsk.supabase.co/functions/v1/eligibility-complete";

const DEBUG = String(DEBUG_ZOHO || "").toLowerCase() === "true";

const MODULE_SURGEONS = "Surgeons";
const MODULE_LEADS = "Leads";
const MODULE_TASKS = "Tasks";

const FIELD_QUESTIONNAIRE_DETAILS = "Questionnaire_Details";
const FIELD_QUESTIONNAIRE_DETAILS_2 = "Questionnaire_Details_2";
const QUESTIONNAIRE_DETAILS_MAX_CHARS = 1800;
const QUESTIONNAIRE_DETAILS_2_MAX_CHARS = 45000;

const FIELD_ACTIVE = "Active_Status";
const FIELD_COUNTRY = "Country";
const FIELD_STATE = "State";
const FIELD_CITY = "City";
const FIELD_NAME = "Name";
const FIELD_PRICE = "Surgery_Price";
const FIELD_BOOK_EN = "Consult_Booking_EN";
const FIELD_BOOK_ES = "Consult_Booking_ES";
const FIELD_BOOK_AR = "Consult_Booking_AR";
const FIELD_SURGEON_ALIAS = "Surgeon_Alias";

const FIELD_LEAD_EMBED_SOURCE_URL = "embed_source_url";
const FIELD_LEAD_SURGEON_NAME = "Pick_Your_Surgeon";
const FIELD_LEAD_PREFERRED_LANGUAGE = "Preferred_Language";
const FIELD_LEAD_MEDICAL_CONDITION_LIST = "Medical_Condition_List";

const inFlightSubmissions = new Map();
const recentSubmissionResults = new Map();
const IN_FLIGHT_TTL_MS = 15_000;
const RECENT_RESULT_TTL_MS = 20_000;

let cachedAccessToken = null;
let tokenExpiresAt = 0;

function debugLog(...args) {
  if (DEBUG) console.log(...args);
}

function digitsOnly(s) {
  return String(s || "").replace(/\D/g, "");
}

function pruneEmpty(obj) {
  const out = {};
  for (const [k, v] of Object.entries(obj || {})) {
    if (v === undefined || v === null) continue;
    if (typeof v === "string" && v.trim() === "") continue;
    if (Array.isArray(v) && v.length === 0) continue;
    out[k] = v;
  }
  return out;
}

function nullableText(v) {
  if (v === undefined || v === null) return null;
  const s = String(v).trim();
  return s === "" ? null : s;
}

function firstText(...values) {
  for (const value of values) {
    const s = nullableText(value);
    if (s) return s;
  }
  return null;
}

function normalizeEmail(v) {
  const s = String(v || "").trim().toLowerCase();
  if (!s) return "";
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(s) ? s : "";
}

function boolToYesNo(v) {
  if (v === true) return "Yes";
  if (v === false) return "No";
  if (typeof v === "string") {
    const s = v.trim().toLowerCase();
    if (["yes", "true", "1"].includes(s)) return "Yes";
    if (["no", "false", "0"].includes(s)) return "No";
  }
  return null;
}

function normalizePreferredLanguageForZoho(v) {
  const s = String(v || "").trim();
  if (!s) return null;
  const lower = s.toLowerCase();
  if (lower === "english" || lower === "en") return "English";
  if (lower === "spanish" || lower === "es") return "Spanish";
  if (lower === "arabic" || lower === "ar") return "Arabic";
  return null;
}

function toZohoJsonArray(value) {
  if (value === undefined || value === null) return null;
  if (Array.isArray(value)) {
    const clean = value.map((x) => String(x || "").trim()).filter(Boolean);
    return clean.length ? clean : null;
  }
  if (typeof value === "string") {
    const s = value.trim();
    if (!s) return null;
    if (s.startsWith("[") && s.endsWith("]")) {
      try {
        const parsed = JSON.parse(s);
        if (Array.isArray(parsed)) {
          const clean = parsed.map((x) => String(x || "").trim()).filter(Boolean);
          return clean.length ? clean : null;
        }
      } catch {}
    }
    const parts = s.split(",").map((x) => x.trim()).filter(Boolean);
    return parts.length ? parts : null;
  }
  const s = String(value).trim();
  return s ? [s] : null;
}

function toMultilineText(value) {
  if (value === undefined || value === null) return null;
  if (typeof value === "string") {
    const s = value.trim();
    return s ? s : null;
  }
  if (Array.isArray(value)) {
    const lines = value
      .map((x) => {
        if (x === undefined || x === null) return "";
        if (typeof x === "object") return safeJsonStringify(x, 4000);
        return String(x).trim();
      })
      .filter(Boolean);
    return lines.length ? lines.join("\n") : null;
  }
  if (typeof value === "object") {
    return safeJsonStringify(value, 12000);
  }
  const s = String(value).trim();
  return s ? s : null;
}

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
  if (raw.startsWith("+")) {
    const cleaned = "+" + digitsOnly(raw);
    return cleaned.length > 1 ? cleaned : "";
  }
  const pn = digitsOnly(raw);
  const ccRaw = String(countryCodeOrDial || "").trim().toUpperCase();
  const dial = ISO_TO_DIAL[ccRaw] || (digitsOnly(ccRaw) ? digitsOnly(ccRaw) : "");
  if (!dial && !pn) return "";
  if (!dial) return pn ? `+${pn}` : "";
  return pn ? `+${dial}${pn}` : `+${dial}`;
}

function getCurrentCountry(submission) {
  return submission.current_location_country || submission.location_country || submission.country || "";
}

function getCurrentState(submission) {
  return submission.current_location_state || submission.location_state || submission.state || "";
}

function getCurrentCity(submission) {
  return submission.current_location_city || submission.location_city || submission.city || "";
}

function pickBookingUrl(record, lang) {
  const l = String(lang || "en").toLowerCase();
  if (l === "es") return record?.[FIELD_BOOK_ES] || record?.[FIELD_BOOK_EN] || "";
  if (l === "ar") return record?.[FIELD_BOOK_AR] || record?.[FIELD_BOOK_EN] || "";
  return record?.[FIELD_BOOK_EN] || "";
}

function todayYYYYMMDD() {
  return new Date().toISOString().slice(0, 10);
}

function safeJsonStringify(obj, maxLen = 28000) {
  let s = "";
  try {
    s = JSON.stringify(obj, null, 2);
  } catch {
    s = String(obj);
  }
  if (s.length > maxLen) return s.slice(0, maxLen) + "\n\n[TRUNCATED]";
  return s;
}

function shouldCreateErrorTasks() {
  if (CREATE_ZOHO_ERROR_TASKS === undefined) return true;
  return String(CREATE_ZOHO_ERROR_TASKS).toLowerCase() === "true";
}

function normalizeUrl(v) {
  const s = String(v || "").trim();
  return s || null;
}

function normalizeHostFromValue(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  const stripped = raw.replace(/^Referral:\s*/i, "").trim();
  try {
    return new URL(stripped).hostname.toLowerCase().replace(/^www\./, "");
  } catch {
    return stripped.toLowerCase().replace(/^https?:\/\//, "").replace(/^www\./, "").split(/[/?#]/)[0];
  }
}

function normalizeLeadSourceForZoho(value) {
  const s = String(value || "").trim();
  if (!s) return null;
  const lower = s.toLowerCase();
  const host = normalizeHostFromValue(s);

  if (lower === "google ads" || host === "get.himplant.com" || lower.includes("get.himplant.com")) {
    return "Google Ads";
  }

  if (host === "himplant.com" || lower.includes("himplant.com") || lower.includes("www.himplant.com")) {
    return "himplant.com";
  }

  return s;
}

function deriveLeadSource(submission) {
  const signals = [
    submission.gclid,
    submission.gclid2,
    submission.gbraid,
    submission.wbraid,
    submission.gad_source,
    submission.utm_source,
    submission.embed_source_url,
    submission.landing_page_url,
    submission.referrer,
    submission.lead_source,
  ]
    .map((x) => String(x || "").toLowerCase())
    .join(" ");

  if (
    submission.gclid ||
    submission.gclid2 ||
    submission.gbraid ||
    submission.wbraid ||
    submission.gad_source ||
    signals.includes("get.himplant.com") ||
    signals.includes("google")
  ) {
    return "Google Ads";
  }

  return normalizeLeadSourceForZoho(submission.lead_source || submission.embed_source_url || submission.landing_page_url || submission.referrer);
}

function getOutcomeValue(submission) {
  return firstText(
    submission.outcome,
    submission.eligibility_outcome,
    submission.eligibility_result,
    submission.result,
    submission.eligibility
  );
}

function getEligibilityValue(submission) {
  return firstText(submission.eligibility, submission.eligibility_outcome, submission.outcome, submission.result);
}

function preserveExistingLeadSource(lead, payload) {
  const existingLeadSource = nullableText(lead?.Lead_Source);
  if (!existingLeadSource) return payload;
  const { Lead_Source, ...rest } = payload || {};
  return rest;
}

const ZOHO_TRIGGER = ["workflow", "blueprint"];

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
  const res = await fetch(`${ZOHO_ACCOUNTS}/oauth/v2/token?${params.toString()}`, { method: "POST" });
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
  if (!res.ok) {
    const err = new Error(`Zoho API error ${res.status}`);
    err.zoho = data;
    err.httpStatus = res.status;
    throw err;
  }
  return data;
}

const zohoGET = (path) => zohoRequest("GET", path);
const zohoPOST = (path, body) => zohoRequest("POST", path, body);
const zohoPUT = (path, body) => zohoRequest("PUT", path, body);

async function fetchSurgeonAlias(surgeonId) {
  const id = String(surgeonId || "").trim();
  if (!id) return null;
  try {
    const record = await zohoGET(`/crm/v2/${MODULE_SURGEONS}/${id}`);
    const s = record?.data?.[0];
    if (!s) return null;
    const alias = String(s?.[FIELD_SURGEON_ALIAS] || "").trim();
    return alias || null;
  } catch {
    return null;
  }
}

async function createZohoErrorTask({ leadId, submissionType, sessionId, email, phoneE164, errorMessage, zohoDetails, submissionPayload }) {
  if (!shouldCreateErrorTasks()) return;
  const subjectBits = [
    "Eligibility API Error",
    submissionType ? `(${submissionType})` : "",
    email ? `email:${email}` : "",
    phoneE164 ? `phone:${phoneE164}` : "",
  ].filter(Boolean);
  const description =
    `Error Message:\n${errorMessage || "(none)"}\n\n` +
    `Zoho Details:\n${safeJsonStringify(zohoDetails || {}, 8000)}\n\n` +
    `Context:\nsession_id=${sessionId || ""}\nemail=${email || ""}\nphone=${phoneE164 || ""}\n\n` +
    `Payload:\n${safeJsonStringify(submissionPayload || {}, 28000)}`;
  const taskRecord = pruneEmpty({
    Subject: subjectBits.join(" ").trim() || "Eligibility API Error",
    Status: "Backlogged",
    Due_Date: todayYYYYMMDD(),
    Description: description,
    Who_Id: leadId || null,
  });
  try {
    await zohoPOST(`/crm/v2/${MODULE_TASKS}`, { trigger: ZOHO_TRIGGER, data: [taskRecord] });
  } catch (e) {
    console.error("[Zoho Task] failed:", String(e.message || e));
  }
}

async function notifySupabaseEligibilityComplete(email) {
  if (!SUPABASE_ELIGIBILITY_API_KEY) return { success: false, reason: "not_configured" };
  if (!SUPABASE_ANON_KEY) return { success: false, reason: "anon_key_not_configured" };
  if (!email) return { success: false, reason: "no_email" };
  try {
    const response = await fetch(ELIGIBILITY_WEBHOOK_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${SUPABASE_ANON_KEY}`,
      },
      body: JSON.stringify({ email, api_key: SUPABASE_ELIGIBILITY_API_KEY }),
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) return { success: false, reason: "request_failed", status: response.status, data };
    return { success: true, data };
  } catch (err) {
    return { success: false, reason: "exception", error: err.message };
  }
}

app.get("/health", (req, res) => res.json({ ok: true }));

app.get("/api/geo/countries", async (req, res) => {
  try {
    const data = await zohoGET(`/crm/v2/${MODULE_SURGEONS}/search?criteria=${encodeURIComponent(`(${FIELD_ACTIVE}:equals:true)`)}`);
    const countries = new Set();
    for (const r of data?.data || []) {
      const c = String(r?.[FIELD_COUNTRY] || "").trim();
      if (c) countries.add(c);
    }
    res.json(Array.from(countries).sort((a, b) => a.localeCompare(b)));
  } catch {
    res.status(500).json({ error: "countries lookup failed" });
  }
});

app.get("/api/geo/states", async (req, res) => {
  try {
    const country = String(req.query.country || "").trim();
    if (!country) return res.status(400).json({ error: "country is required" });
    if (country !== "United States") return res.json([]);
    const criteria = `(${FIELD_ACTIVE}:equals:true) and (${FIELD_COUNTRY}:equals:${country})`;
    const data = await zohoGET(`/crm/v2/${MODULE_SURGEONS}/search?criteria=${encodeURIComponent(criteria)}`);
    const states = new Set();
    for (const r of data?.data || []) {
      const st = String(r?.[FIELD_STATE] || "").trim();
      if (st) states.add(st);
    }
    res.json(Array.from(states).sort((a, b) => a.localeCompare(b)));
  } catch {
    res.status(500).json({ error: "states lookup failed" });
  }
});

app.get("/api/geo/cities", async (req, res) => {
  try {
    const country = String(req.query.country || "").trim();
    const state = String(req.query.state || "").trim();
    if (!country) return res.status(400).json({ error: "country is required" });
    let criteria = `(${FIELD_ACTIVE}:equals:true) and (${FIELD_COUNTRY}:equals:${country})`;
    if (country === "United States" && state) {
      criteria = `(${FIELD_ACTIVE}:equals:true) and (${FIELD_COUNTRY}:equals:${country}) and (${FIELD_STATE}:equals:${state})`;
    }
    const data = await zohoGET(`/crm/v2/${MODULE_SURGEONS}/search?criteria=${encodeURIComponent(criteria)}`);
    const cities = new Set();
    for (const r of data?.data || []) {
      const city = String(r?.[FIELD_CITY] || "").trim();
      if (city) cities.add(city);
    }
    res.json(Array.from(cities).sort((a, b) => a.localeCompare(b)));
  } catch {
    res.status(500).json({ error: "cities lookup failed" });
  }
});

app.get("/api/surgeons", async (req, res) => {
  try {
    const country = String(req.query.country || "").trim();
    const state = String(req.query.state || "").trim();
    const city = String(req.query.city || "").trim();
    const lang = String(req.query.lang || "en").trim().toLowerCase();
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
  } catch {
    res.status(500).json({ error: "surgeons lookup failed" });
  }
});

app.get("/api/surgeons/:id", async (req, res) => {
  try {
    const surgeonId = String(req.params.id || "").trim();
    const lang = String(req.query.lang || "en").trim().toLowerCase();
    if (!surgeonId) return res.status(400).json({ error: "surgeonId required" });
    const record = await zohoGET(`/crm/v2/${MODULE_SURGEONS}/${surgeonId}`);
    const s = record?.data?.[0];
    if (!s) return res.status(404).json({ error: "surgeon not found" });
    const bookingUrl = pickBookingUrl(s, lang);
    res.json({ id: surgeonId, name: s[FIELD_NAME] || "", price: s[FIELD_PRICE] ?? null, bookingUrl: bookingUrl || null });
  } catch {
    res.status(500).json({ error: "surgeon lookup failed" });
  }
});

async function searchLeadByEmail(email) {
  const e = normalizeEmail(email);
  if (!e) return null;
  const data = await zohoGET(`/crm/v2/${MODULE_LEADS}/search?criteria=${encodeURIComponent(`(Email:equals:${e})`)}`);
  return (data?.data || [])[0] || null;
}

async function searchLeadBySessionId(sessionId) {
  const s = String(sessionId || "").trim();
  if (!s) return null;
  const data = await zohoGET(`/crm/v2/${MODULE_LEADS}/search?criteria=${encodeURIComponent(`(Session_ID:equals:${s})`)}`);
  return (data?.data || [])[0] || null;
}

async function searchLeadByPhoneE164(phoneE164) {
  const p = String(phoneE164 || "").trim();
  if (!p) return null;
  const criteria = `(Phone:equals:${p}) or (Mobile:equals:${p})`;
  const data = await zohoGET(`/crm/v2/${MODULE_LEADS}/search?criteria=${encodeURIComponent(criteria)}`);
  return (data?.data || [])[0] || null;
}

async function getLeadByIdForAppend(leadId) {
  const record = await zohoGET(`/crm/v2/${MODULE_LEADS}/${leadId}`);
  return record?.data?.[0] || null;
}

async function findLeadForInitialLead({ sessionId, email, phoneE164 }) {
  if (sessionId) {
    const bySession = await searchLeadBySessionId(sessionId);
    if (bySession?.id) return { lead: bySession, matchedBy: "session" };
  }
  if (email) {
    const byEmail = await searchLeadByEmail(email);
    if (byEmail?.id) return { lead: byEmail, matchedBy: "email" };
  }
  if (phoneE164) {
    const byPhone = await searchLeadByPhoneE164(phoneE164);
    if (byPhone?.id) return { lead: byPhone, matchedBy: "phone" };
  }
  return { lead: null, matchedBy: "none" };
}

async function findLeadForQuestionnaire({ sessionId }) {
  const bySession = await searchLeadBySessionId(sessionId);
  if (bySession?.id) return { lead: bySession, matchedBy: "session" };
  return { lead: null, matchedBy: "none" };
}

function buildFullEntryString(submission, type, extra = {}) {
  const ts = new Date().toISOString();
  const header =
    `===== ${ts} | submission_type=${type || ""}` +
    (extra.sessionId ? ` | session_id=${extra.sessionId}` : "") +
    (extra.email ? ` | email=${extra.email}` : "") +
    (extra.phone ? ` | phone=${extra.phone}` : "") +
    ` =====\n`;
  return header + safeJsonStringify(submission, QUESTIONNAIRE_DETAILS_2_MAX_CHARS) + "\n\n";
}

function buildSummaryEntryString(submission, type, extra = {}) {
  const ts = new Date().toISOString();
  const lines = [
    `===== ${ts} | submission_type=${type || ""} =====`,
    extra.sessionId ? `session_id=${extra.sessionId}` : null,
    extra.email ? `email=${extra.email}` : null,
    extra.phone ? `phone=${extra.phone}` : null,
    `outcome=${getOutcomeValue(submission) || ""}`,
    `eligibility=${getEligibilityValue(submission) || ""}`,
    `surgeon=${submission.surgeon_name || submission.surgeon_id || ""}`,
    `location=${[getCurrentCity(submission), getCurrentState(submission), getCurrentCountry(submission)].filter(Boolean).join(", ")}`,
    `language=${submission.preferred_language || ""}`,
    `source=${deriveLeadSource(submission) || ""}`,
  ].filter(Boolean);

  return lines.join("\n") + "\n\n";
}

function clampToMaxCharsKeepNewest(text, maxChars) {
  const s = String(text || "");
  if (s.length <= maxChars) return s;
  return "[TRUNCATED_OLD_ENTRIES]\n\n" + s.slice(s.length - maxChars);
}

async function appendQuestionnaireDetailsToExistingLead(leadId, payload, submission, type, ctx) {
  const newSummaryEntry = buildSummaryEntryString(submission, type, ctx);
  const newFullEntry = buildFullEntryString(submission, type, ctx);
  let existingSmall = "";
  let existingLarge = "";
  try {
    const lead = await getLeadByIdForAppend(leadId);
    existingSmall = String(lead?.[FIELD_QUESTIONNAIRE_DETAILS] || "");
    existingLarge = String(lead?.[FIELD_QUESTIONNAIRE_DETAILS_2] || "");
  } catch {}
  const combinedSmall = existingSmall ? existingSmall + "\n" + newSummaryEntry : newSummaryEntry;
  const combinedLarge = existingLarge ? existingLarge + "\n" + newFullEntry : newFullEntry;
  return {
    ...payload,
    [FIELD_QUESTIONNAIRE_DETAILS]: clampToMaxCharsKeepNewest(combinedSmall, QUESTIONNAIRE_DETAILS_MAX_CHARS),
    [FIELD_QUESTIONNAIRE_DETAILS_2]: clampToMaxCharsKeepNewest(combinedLarge, QUESTIONNAIRE_DETAILS_2_MAX_CHARS),
  };
}

function mapAttribution(submission) {
  return pruneEmpty({
    Lead_Source: deriveLeadSource(submission),
    gclid2: nullableText(submission.gclid || submission.gclid2),
    GBRAID: nullableText(submission.gbraid),
    WBRAID: nullableText(submission.wbraid),
    FBCLID: nullableText(submission.fbclid),
    MSCLKID: nullableText(submission.msclkid),
    Gad_Source: nullableText(submission.gad_source),
    utm_source: nullableText(submission.utm_source),
    utm_medium: nullableText(submission.utm_medium),
    utm_campaign: nullableText(submission.utm_campaign),
    utm_content: nullableText(submission.utm_content),
    utm_term: nullableText(submission.utm_term),
    [FIELD_LEAD_EMBED_SOURCE_URL]: normalizeUrl(submission.embed_source_url),
    Landing_Page_URL: normalizeUrl(submission.landing_page_url || submission.embed_source_url),
    Idempotency_Key: nullableText(submission.idempotency_key),
  });
}

function mapCommonBase(submission, surgeonAlias, { includeIdentity = true } = {}) {
  const phone = normalizePhoneE164(submission.phone_country_code, submission.phone_number);
  const identity = includeIdentity
    ? {
        First_Name: nullableText(submission.first_name),
        Last_Name: nullableText(submission.last_name),
        Email: normalizeEmail(submission.email) || null,
        Phone: phone || null,
        Mobile: phone || null,
      }
    : {};

  return pruneEmpty({
    ...identity,
    Country: nullableText(getCurrentCountry(submission)),
    State: nullableText(getCurrentState(submission)),
    City: nullableText(getCurrentCity(submission)),
    Session_ID: nullableText(submission.session_id),
    [FIELD_LEAD_PREFERRED_LANGUAGE]: normalizePreferredLanguageForZoho(submission.preferred_language),
    Surgeon_name_Lookup: nullableText(submission.surgeon_id),
    [FIELD_LEAD_SURGEON_NAME]: nullableText(surgeonAlias),
    Intake_Date: nullableText(submission.submitted_at) || new Date().toISOString(),
    ...mapAttribution(submission),
  });
}

function mapLeadBase(submission, surgeonAlias) {
  return mapCommonBase(submission, surgeonAlias, { includeIdentity: true });
}

function mapPartialBase(submission, surgeonAlias) {
  return pruneEmpty({
    ...mapCommonBase(submission, surgeonAlias, { includeIdentity: false }),
    Date_of_Birth: nullableText(submission.date_of_birth),
    Eligibility: getEligibilityValue(submission),
    Outcome: getOutcomeValue(submission),
    Eligibility_Flags: toMultilineText(submission.eligibility_flags || submission.flags),
    Eligibility_Reasons: toMultilineText(submission.eligibility_reasons || submission.reasons),
  });
}

function mapCompleteBase(submission, surgeonAlias) {
  return pruneEmpty({
    ...mapCommonBase(submission, surgeonAlias, { includeIdentity: false }),
    Date_of_Birth: nullableText(submission.date_of_birth),
    Payment_Method: nullableText(submission.payment_method),
    Procedure_Timeline: nullableText(submission.timeline || submission.procedure_timeline),
    Circumcised: boolToYesNo(submission.circumcised),
    Tobacco: boolToYesNo(submission.tobacco_use),
    ED_history: boolToYesNo(submission.ed_history),
    Active_STD: boolToYesNo(submission.active_std),
    Can_maintain_erection:
      submission.ed_maintain_with_or_without_meds === null || submission.ed_maintain_with_or_without_meds === undefined
        ? null
        : boolToYesNo(submission.ed_maintain_with_or_without_meds),
    STD_list: toZohoJsonArray(submission.std_list),
    Previous_Penis_Surgeries: toZohoJsonArray(submission.prior_procedure_list),
    Recent_Outbreak:
      submission.recent_outbreak_6mo === null || submission.recent_outbreak_6mo === undefined
        ? null
        : boolToYesNo(submission.recent_outbreak_6mo),
    [FIELD_LEAD_MEDICAL_CONDITION_LIST]: toMultilineText(submission.medical_conditions_list),
    Body_Type: nullableText(submission.body_type),
    Eligibility: getEligibilityValue(submission),
    Outcome: getOutcomeValue(submission),
    Eligibility_Flags: toMultilineText(submission.eligibility_flags || submission.flags),
    Eligibility_Reasons: toMultilineText(submission.eligibility_reasons || submission.reasons),
  });
}

function stripPhoneFields(payload) {
  const { Phone, Mobile, ...rest } = payload || {};
  return rest;
}

function isDuplicateZohoErrorForField(zohoErr, apiName) {
  return zohoErr?.code === "DUPLICATE_DATA" && zohoErr?.details?.api_name === apiName;
}

function isInvalidDataLike(zohoErr) {
  const code = zohoErr?.code;
  return code === "INVALID_DATA" || code === "MANDATORY_NOT_FOUND";
}

function getApiNameFromZohoErr(zohoErr) {
  return zohoErr?.details?.api_name || null;
}

async function createLead(payload) {
  const resp = await zohoPOST(`/crm/v2/${MODULE_LEADS}`, { trigger: ZOHO_TRIGGER, data: [payload] });
  const first = resp?.data?.[0];
  if (first?.status !== "success") {
    const err = new Error("createLead failed");
    err.zoho = first || resp;
    err.httpStatus = 400;
    throw err;
  }
  const id = first?.details?.id;
  if (!id) {
    const err = new Error("createLead failed (no id returned)");
    err.zoho = first || resp;
    err.httpStatus = 400;
    throw err;
  }
  return id;
}

async function createLeadWithRecovery(payload) {
  let working = { ...payload };
  const removed = [];
  for (let attempt = 1; attempt <= 8; attempt++) {
    try {
      const id = await createLead(working);
      return { id, removed_fields: removed };
    } catch (e) {
      const zohoErr = e?.zoho || null;
      if (isDuplicateZohoErrorForField(zohoErr, "Email")) throw e;
      if (isDuplicateZohoErrorForField(zohoErr, "Phone") || isDuplicateZohoErrorForField(zohoErr, "Mobile")) throw e;
      if (isInvalidDataLike(zohoErr)) {
        const apiName = getApiNameFromZohoErr(zohoErr);
        if (apiName && Object.prototype.hasOwnProperty.call(working, apiName)) {
          delete working[apiName];
          removed.push(apiName);
          console.log(`[Zoho create reject] removing field ${apiName} and retrying...`);
          continue;
        }
      }
      throw e;
    }
  }
  const err = new Error("createLead failed after retries");
  err.httpStatus = 500;
  throw err;
}

async function updateLeadOnce(leadId, payload) {
  const resp = await zohoPUT(`/crm/v2/${MODULE_LEADS}/${leadId}`, { trigger: ZOHO_TRIGGER, data: [payload] });
  const first = resp?.data?.[0];
  if (first?.status !== "success") {
    const err = new Error("updateLead failed");
    err.zoho = first || resp;
    err.httpStatus = 400;
    throw err;
  }
  return true;
}

async function updateLeadWithRecovery(leadId, payload) {
  let working = { ...payload };
  const removed = [];
  for (let attempt = 1; attempt <= 8; attempt++) {
    try {
      await updateLeadOnce(leadId, working);
      return { success: true, removed_fields: removed };
    } catch (e) {
      const zohoErr = e?.zoho || null;
      if (isDuplicateZohoErrorForField(zohoErr, "Email") && working.Email) {
        delete working.Email;
        removed.push("Email");
        debugLog("[Zoho update] DUPLICATE Email -> remove from update payload and retry");
        continue;
      }
      if (
        (isDuplicateZohoErrorForField(zohoErr, "Phone") && (working.Phone || working.Mobile)) ||
        (isDuplicateZohoErrorForField(zohoErr, "Mobile") && (working.Phone || working.Mobile))
      ) {
        working = stripPhoneFields(working);
        removed.push("Phone", "Mobile");
        debugLog("[Zoho update] DUPLICATE phone -> remove Phone/Mobile from update payload and retry");
        continue;
      }
      if (isInvalidDataLike(zohoErr)) {
        const apiName = getApiNameFromZohoErr(zohoErr);
        if (apiName && Object.prototype.hasOwnProperty.call(working, apiName)) {
          delete working[apiName];
          removed.push(apiName);
          console.log(`[Zoho reject] removing field ${apiName} and retrying...`);
          continue;
        }
      }
      throw e;
    }
  }
  const err = new Error("updateLead failed after retries");
  err.httpStatus = 500;
  throw err;
}

function makeSubmissionLockKey({ type, sessionId, email, phoneE164, idempotencyKey }) {
  const idem = String(idempotencyKey || "").trim();
  if (idem) return `idem:${idem}`;
  if (type === "lead") return `lead:${sessionId || ""}:${email || ""}:${phoneE164 || ""}`;
  return `${type}:${sessionId || ""}`;
}

async function withSubmissionLock(key, fn, { cacheRecent = false } = {}) {
  const now = Date.now();
  const cached = recentSubmissionResults.get(key);
  if (cached && cached.expiresAt > now) return cached.result;
  if (cached) recentSubmissionResults.delete(key);

  if (inFlightSubmissions.has(key)) return inFlightSubmissions.get(key);

  const promise = (async () => {
    const result = await fn();
    if (cacheRecent) {
      recentSubmissionResults.set(key, { result, expiresAt: Date.now() + RECENT_RESULT_TTL_MS });
      setTimeout(() => recentSubmissionResults.delete(key), RECENT_RESULT_TTL_MS).unref?.();
    }
    return result;
  })();

  inFlightSubmissions.set(key, promise);
  setTimeout(() => inFlightSubmissions.delete(key), IN_FLIGHT_TTL_MS).unref?.();
  try {
    return await promise;
  } finally {
    inFlightSubmissions.delete(key);
  }
}

async function handleDuplicateCreateAsUpdate({ error, payloadWithAppend, sessionId, email, phoneE164 }) {
  const zohoErr = error?.zoho || null;
  let found = null;
  let matchedBy = "none";

  if (isDuplicateZohoErrorForField(zohoErr, "Email") && email) {
    found = await searchLeadByEmail(email);
    matchedBy = "duplicate_email";
  }
  if (!found && (isDuplicateZohoErrorForField(zohoErr, "Phone") || isDuplicateZohoErrorForField(zohoErr, "Mobile")) && phoneE164) {
    found = await searchLeadByPhoneE164(phoneE164);
    matchedBy = "duplicate_phone";
  }
  if (!found && sessionId) {
    found = await searchLeadBySessionId(sessionId);
    matchedBy = "duplicate_session";
  }
  if (!found?.id) throw error;

  const payloadPreservingSource = preserveExistingLeadSource(found, payloadWithAppend);
  const updatePayload = await appendQuestionnaireDetailsToExistingLead(found.id, payloadPreservingSource, {}, "duplicate_recovery", {
    sessionId,
    email,
    phone: phoneE164,
  });
  const upd = await updateLeadWithRecovery(found.id, updatePayload);
  return { id: found.id, created: false, matched_by: matchedBy, removed_fields: upd.removed_fields || [] };
}

app.post("/api/submissions", async (req, res) => {
  const submission = req.body || {};
  const type = String(submission.submission_type || "").toLowerCase();
  const sessionId = String(submission.session_id || "").trim();
  const email = normalizeEmail(submission.email);
  const suppliedEmail = String(submission.email || "").trim();
  const phoneE164 = normalizePhoneE164(submission.phone_country_code, submission.phone_number);
  const idempotencyKey = nullableText(submission.idempotency_key);
  let lead = null;

  try {
    if (!["lead", "partial", "complete"].includes(type)) {
      return res.status(400).json({ success: false, error: "submission_type must be 'lead', 'partial', or 'complete'." });
    }

    if (!sessionId) {
      return res.status(400).json({ success: false, error: "session_id is required." });
    }

    if (suppliedEmail && !email) {
      return res.status(400).json({ success: false, error: "email is invalid." });
    }

    if (type === "lead" && !email && !phoneE164) {
      return res.status(400).json({ success: false, error: "lead submissions require a valid email or phone." });
    }

    const lockKey = makeSubmissionLockKey({ type, sessionId, email, phoneE164, idempotencyKey });
    const cacheRecent = Boolean(idempotencyKey) || type === "lead";

    const result = await withSubmissionLock(
      lockKey,
      async () => {
        const found =
          type === "lead"
            ? await findLeadForInitialLead({ sessionId, email, phoneE164 })
            : await findLeadForQuestionnaire({ sessionId });

        lead = found.lead;
        let matchedBy = found.matchedBy;

        if (!lead?.id && type !== "lead") {
          return {
            status: 409,
            body: {
              success: false,
              error: "No existing Lead found for this session_id. Partial and complete submissions cannot create Leads.",
              session_id: sessionId,
            },
          };
        }

        const surgeonId = String(submission.surgeon_id || "").trim();
        const surgeonAlias = surgeonId ? await fetchSurgeonAlias(surgeonId) : null;
        const rawPayloadBase =
          type === "lead" ? mapLeadBase(submission, surgeonAlias) : type === "partial" ? mapPartialBase(submission, surgeonAlias) : mapCompleteBase(submission, surgeonAlias);
        const payloadBase = lead?.id ? preserveExistingLeadSource(lead, rawPayloadBase) : rawPayloadBase;

        if (lead?.id) {
          const payloadWithAppend = await appendQuestionnaireDetailsToExistingLead(lead.id, payloadBase, submission, type, {
            sessionId,
            email,
            phone: phoneE164,
          });

          const upd = await updateLeadWithRecovery(lead.id, payloadWithAppend);
          let supabaseWebhookResult = null;
          const webhookEmail = email || normalizeEmail(lead?.Email);
          if (type === "complete") {
            supabaseWebhookResult = await notifySupabaseEligibilityComplete(webhookEmail);
          }

          return {
            status: 200,
            body: {
              success: true,
              created: false,
              lead_id: lead.id,
              matched_by: matchedBy,
              removed_fields: upd.removed_fields || [],
              supabase_webhook: supabaseWebhookResult,
            },
          };
        }

        const initialSummaryEntry = buildSummaryEntryString(submission, type, { sessionId, email, phone: phoneE164 });
        const initialFullEntry = buildFullEntryString(submission, type, { sessionId, email, phone: phoneE164 });
        const createPayload = {
          ...payloadBase,
          [FIELD_QUESTIONNAIRE_DETAILS]: clampToMaxCharsKeepNewest(initialSummaryEntry, QUESTIONNAIRE_DETAILS_MAX_CHARS),
          [FIELD_QUESTIONNAIRE_DETAILS_2]: clampToMaxCharsKeepNewest(initialFullEntry, QUESTIONNAIRE_DETAILS_2_MAX_CHARS),
        };

        try {
          const created = await createLeadWithRecovery(createPayload);
          return {
            status: 200,
            body: {
              success: true,
              created: true,
              lead_id: created.id,
              matched_by: "created",
              removed_fields: created.removed_fields || [],
              supabase_webhook: null,
            },
          };
        } catch (e) {
          const recovered = await handleDuplicateCreateAsUpdate({
            error: e,
            payloadWithAppend: createPayload,
            sessionId,
            email,
            phoneE164,
          });
          return {
            status: 200,
            body: {
              success: true,
              created: recovered.created,
              lead_id: recovered.id,
              matched_by: recovered.matched_by,
              removed_fields: recovered.removed_fields || [],
              duplicate_recovered: true,
              supabase_webhook: null,
            },
          };
        }
      },
      { cacheRecent }
    );

    return res.status(result.status).json(result.body);
  } catch (e) {
    const zohoErr = e?.zoho || null;
    const zohoHttp = e?.httpStatus || null;
    await createZohoErrorTask({
      leadId: lead?.id || null,
      submissionType: type,
      sessionId,
      email,
      phoneE164,
      errorMessage: String(e?.message || e),
      zohoDetails: zohoErr,
      submissionPayload: submission,
    });
    console.error(
      "[POST /api/submissions] error:",
      String(e?.message || e),
      zohoHttp ? `(zoho_http=${zohoHttp})` : "",
      zohoErr?.code ? `(zoho_code=${zohoErr.code})` : "",
      zohoErr?.details?.api_name ? `(api_name=${zohoErr.details.api_name})` : ""
    );
    const nonRetryable = zohoHttp && zohoHttp >= 400 && zohoHttp < 500;
    if (nonRetryable) {
      return res.status(200).json({
        success: true,
        warning: "zoho_rejected_payload_manual_review_needed",
        zoho_http: zohoHttp,
        zoho_code: zohoErr?.code || null,
        zoho_api_name: zohoErr?.details?.api_name || null,
        zoho_message: zohoErr?.message || null,
        zoho_debug: DEBUG ? zohoErr : undefined,
      });
    }
    return res.status(500).json({ success: false, error: "submission failed" });
  }
});

const port = process.env.PORT || 10000;
app.listen(port, () => console.log(`API running on port ${port}`));
