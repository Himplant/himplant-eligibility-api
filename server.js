import express from "express";
import cors from "cors";
import fetch from "node-fetch";

const app = express();
app.use(express.json({ limit: "5mb" }));

const ALLOWED_ORIGINS = [
  "https://eligibility.himplant.com",
  "https://himplant.com",
  "https://www.himplant.com",
];

app.use(
  cors({
    origin: function (origin, callback) {
      if (!origin) return callback(null, true);
      if (ALLOWED_ORIGINS.includes(origin)) return callback(null, true);
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
} = process.env;

const MODULE_SURGEONS = "Surgeons";
const MODULE_LEADS = "Leads";
const MODULE_TASKS = "Tasks";

const FIELD_QUESTIONNAIRE_DETAILS = "Questionnaire_Details";
const QUESTIONNAIRE_DETAILS_MAX_CHARS = 28000;

const PDF_ATTACHMENT_MAX_BYTES = 18 * 1024 * 1024;

const FIELD_ACTIVE = "Active_Status";
const FIELD_COUNTRY = "Country";
const FIELD_STATE = "State";
const FIELD_CITY = "City";
const FIELD_NAME = "Name";
const FIELD_PRICE = "Surgery_Price";
const FIELD_BOOK_EN = "Consult_Booking_EN";
const FIELD_BOOK_ES = "Consult_Booking_ES";
const FIELD_BOOK_AR = "Consult_Booking_AR";

let cachedAccessToken = null;
let tokenExpiresAt = 0;

// -------------------------
// Helpers
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
      } catch {
        // fall through
      }
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
    const lines = value.map((x) => String(x || "").trim()).filter(Boolean);
    return lines.length ? lines.join("\n") : null;
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
  const dial =
    ISO_TO_DIAL[ccRaw] || (digitsOnly(ccRaw) ? digitsOnly(ccRaw) : "");

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

function summarizeZohoError(zohoObj) {
  const d0 = zohoObj?.data?.[0] || zohoObj || {};
  const details = d0?.details || {};
  return pruneEmpty({
    code: d0?.code,
    message: d0?.message,
    status: d0?.status,
    api_name: details?.api_name,
    expected_data_type: details?.expected_data_type,
    duplicate_record_id: details?.id,
  });
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

// -------------------------
// PDF Attachment upload
// -------------------------
function extractBase64Payload(maybeDataUrlOrBase64) {
  const s = String(maybeDataUrlOrBase64 || "").trim();
  if (!s) return "";
  const commaIdx = s.indexOf(",");
  if (s.startsWith("data:") && commaIdx !== -1) return s.slice(commaIdx + 1).trim();
  return s;
}

async function uploadLeadPdfAttachment({ leadId, submissionPdfBase64, filename }) {
  const b64 = extractBase64Payload(submissionPdfBase64);
  if (!b64) return { uploaded: false, reason: "no_pdf" };

  let buf;
  try {
    buf = Buffer.from(b64, "base64");
  } catch {
    return { uploaded: false, reason: "invalid_base64" };
  }

  if (!buf?.length) return { uploaded: false, reason: "empty_pdf" };
  if (buf.length > PDF_ATTACHMENT_MAX_BYTES) return { uploaded: false, reason: "pdf_too_large" };

  const form = new FormData();
  const blob = new Blob([buf], { type: "application/pdf" });
  form.append("file", blob, filename);

  const token = await getAccessToken();
  const res = await fetch(`${ZOHO_API_BASE}/crm/v2/${MODULE_LEADS}/${leadId}/Attachments`, {
    method: "POST",
    headers: { Authorization: `Zoho-oauthtoken ${token}` },
    body: form,
  });

  const text = await res.text();
  let data;
  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    data = { raw: text };
  }

  if (!res.ok) {
    const err = new Error(`Zoho attachment upload error ${res.status}`);
    err.zoho = data;
    err.httpStatus = res.status;
    throw err;
  }

  return { uploaded: true, data };
}

// -------------------------
// Zoho Task creation on errors
// -------------------------
async function createZohoErrorTask({
  leadId,
  submissionType,
  sessionId,
  email,
  phoneE164,
  errorMessage,
  zohoDetails,
  submissionPayload,
}) {
  if (!shouldCreateErrorTasks()) return;

  const subject =
    [
      "Eligibility API Error",
      submissionType ? `(${submissionType})` : "",
      email ? `email:${email}` : "",
      phoneE164 ? `phone:${phoneE164}` : "",
    ]
      .filter(Boolean)
      .join(" ")
      .trim() || "Eligibility API Error";

  const description =
    `Error Message:\n${errorMessage || "(none)"}\n\n` +
    `Zoho Details:\n${safeJsonStringify(zohoDetails || {}, 8000)}\n\n` +
    `Context:\nsession_id=${sessionId || ""}\nemail=${email || ""}\nphone=${phoneE164 || ""}\n\n` +
    `Payload:\n${safeJsonStringify(submissionPayload || {}, 28000)}`;

  const taskRecord = pruneEmpty({
    Subject: subject,
    Status: "Backlogged",
    Due_Date: todayYYYYMMDD(),
    Description: description,
    Who_Id: leadId || null,
  });

  try {
    await zohoPOST(`/crm/v2/${MODULE_TASKS}`, { data: [taskRecord] });
  } catch (e) {
    console.error("[Zoho Task] failed:", String(e?.message || e));
  }
}

// -------------------------
// Health
// -------------------------
app.get("/health", (req, res) => res.json({ ok: true }));

// -------------------------
// CMS
// -------------------------
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
  } catch {
    res.status(500).json({ error: "countries lookup failed" });
  }
});

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
  } catch {
    res.status(500).json({ error: "states lookup failed" });
  }
});

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
  } catch {
    res.status(500).json({ error: "cities lookup failed" });
  }
});

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
      criteria =
        `(${FIELD_ACTIVE}:equals:true) and (${FIELD_COUNTRY}:equals:${country}) and ` +
        `(${FIELD_STATE}:equals:${state}) and (${FIELD_CITY}:equals:${city})`;
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
// Lead helpers
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

async function getLeadById(leadId) {
  const record = await zohoGET(`/crm/v2/${MODULE_LEADS}/${leadId}`);
  return record?.data?.[0] || null;
}

async function createLead(payload) {
  const resp = await zohoPOST(`/crm/v2/${MODULE_LEADS}`, { data: [payload] });
  const first = resp?.data?.[0];
  if (first?.status !== "success") {
    const err = new Error("createLead failed");
    err.zoho = first || resp;
    throw err;
  }
  return first.details.id;
}

async function updateLead(leadId, payload) {
  const resp = await zohoPUT(`/crm/v2/${MODULE_LEADS}/${leadId}`, { data: [payload] });
  const first = resp?.data?.[0];
  if (first?.status !== "success") {
    const err = new Error("updateLead failed");
    err.zoho = first || resp;
    throw err;
  }
  return true;
}

// -------------------------
// Questionnaire_Details best-effort append (DOES NOT BLOCK main upsert)
// -------------------------
function buildEntryString(submission, type, ctx) {
  const ts = new Date().toISOString();
  const header =
    `===== ${ts} | submission_type=${type || ""}` +
    (ctx.sessionId ? ` | session_id=${ctx.sessionId}` : "") +
    (ctx.email ? ` | email=${ctx.email}` : "") +
    (ctx.phone ? ` | phone=${ctx.phone}` : "") +
    ` =====\n`;

  return header + safeJsonStringify(submission, QUESTIONNAIRE_DETAILS_MAX_CHARS) + "\n\n";
}

function clampToMaxCharsKeepNewest(text, maxChars) {
  const s = String(text || "");
  if (s.length <= maxChars) return s;
  return "[TRUNCATED_OLD_ENTRIES]\n\n" + s.slice(s.length - maxChars);
}

async function bestEffortAppendQuestionnaireDetails(leadId, submission, type, ctx) {
  try {
    const lead = await getLeadById(leadId);
    const existing = String(lead?.[FIELD_QUESTIONNAIRE_DETAILS] || "");
    const entry = buildEntryString(submission, type, ctx);

    const combined = existing ? existing + "\n" + entry : entry;

    // IMPORTANT: Ensure it's a string (not object), clamp length
    const updatePayload = {
      [FIELD_QUESTIONNAIRE_DETAILS]: clampToMaxCharsKeepNewest(combined, QUESTIONNAIRE_DETAILS_MAX_CHARS),
    };

    await updateLead(leadId, updatePayload);
    return { ok: true };
  } catch (e) {
    const zohoSummary = summarizeZohoError(e?.zoho);
    console.error("[Questionnaire_Details append] failed:", zohoSummary);

    await createZohoErrorTask({
      leadId,
      submissionType: type,
      sessionId: ctx.sessionId,
      email: ctx.email,
      phoneE164: ctx.phone,
      errorMessage: "Failed to append Questionnaire_Details (field type/layout mismatch likely).",
      zohoDetails: e?.zoho || null,
      submissionPayload: submission,
    });

    return { ok: false, zoho: zohoSummary };
  }
}

// -------------------------
// Mapping (NO Questionnaire_Details here)
// -------------------------
function mapLeadBase(submission) {
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

function mapPartialBase(submission) {
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

function mapCompleteBase(submission) {
  const phone = normalizePhoneE164(submission.phone_country_code, submission.phone_number);

  return pruneEmpty({
    First_Name: nullableText(submission.first_name),
    Last_Name: nullableText(submission.last_name),
    Email: nullableText(submission.email),
    Phone: phone || null,
    Mobile: phone || null,
    Date_of_Birth: nullableText(submission.date_of_birth),

    Country: nullableText(getCurrentCountry(submission)),
    State: nullableText(getCurrentState(submission)),

    Session_ID: nullableText(submission.session_id),
    Surgeon_name_Lookup: nullableText(submission.surgeon_id),

    Payment_Method: nullableText(submission.payment_method),
    Procedure_Timeline: nullableText(submission.timeline),

    Circumcised: boolToYesNo(submission.circumcised),
    Tobacco: boolToYesNo(submission.tobacco_use),
    ED_history: boolToYesNo(submission.ed_history),
    Active_STD: boolToYesNo(submission.active_std),

    Can_maintain_erection:
      submission.ed_maintain_with_or_without_meds === null ||
      submission.ed_maintain_with_or_without_meds === undefined
        ? null
        : boolToYesNo(submission.ed_maintain_with_or_without_meds),

    STD_list: toZohoJsonArray(submission.std_list),
    Previous_Penis_Surgeries: toZohoJsonArray(submission.prior_procedure_list),

    Recent_Outbreak:
      submission.recent_outbreak_6mo === null || submission.recent_outbreak_6mo === undefined
        ? null
        : boolToYesNo(submission.recent_outbreak_6mo),

    Medical_conditions_list: toMultilineText(submission.medical_conditions_list),

    Body_Type: nullableText(submission.body_type),
    Outcome: nullableText(submission.outcome),

    Intake_Date: nullableText(submission.submitted_at) || new Date().toISOString(),
  });
}

// -------------------------
// Submissions endpoint
// -------------------------
app.post("/api/submissions", async (req, res) => {
  const submission = req.body || {};
  const type = String(submission.submission_type || "").toLowerCase();

  const sessionId = String(submission.session_id || "").trim();
  const email = String(submission.email || "").trim();
  const phoneE164 = normalizePhoneE164(submission.phone_country_code, submission.phone_number);
  const submissionPdf = submission.submission_pdf;

  let lead = null;

  try {
    if (!["lead", "partial", "complete"].includes(type)) {
      return res.status(400).json({ success: false, error: "invalid_submission_type" });
    }

    if ((type === "lead" || type === "complete") && !sessionId) {
      return res.status(400).json({ success: false, error: "missing_session_id" });
    }

    if (!sessionId && !email && !phoneE164) {
      return res.status(400).json({ success: false, error: "missing_identifiers" });
    }

    // Find lead
    if (type === "complete" && sessionId) lead = await searchLeadBySessionId(sessionId);
    if (!lead && email) lead = await searchLeadByEmail(email);
    if (!lead && phoneE164) lead = await searchLeadByPhoneE164(phoneE164);

    // Base payload for main upsert
    const basePayload =
      type === "lead"
        ? mapLeadBase(submission)
        : type === "partial"
          ? mapPartialBase(submission)
          : mapCompleteBase(submission);

    if (type === "complete" && lead?.id && sessionId) basePayload.Session_ID = sessionId;

    // Upsert main lead fields (DO NOT include Questionnaire_Details here)
    let leadId = lead?.id;

    if (leadId) {
      await updateLead(leadId, basePayload);
    } else {
      leadId = await createLead(basePayload);
    }

    // Best-effort append Questionnaire_Details (does not block success)
    const appendResult = await bestEffortAppendQuestionnaireDetails(
      leadId,
      submission,
      type,
      { sessionId, email, phone: phoneE164 }
    );

    // Best-effort attachment (does not block success)
    let attachmentUploaded = false;
    if (submissionPdf && leadId) {
      try {
        const fname = `Eligibility_Summary_${todayYYYYMMDD()}_${sessionId || "no_session"}.pdf`;
        await uploadLeadPdfAttachment({ leadId, submissionPdfBase64: submissionPdf, filename: fname });
        attachmentUploaded = true;
      } catch (e) {
        console.error("[Attachment] upload failed:", summarizeZohoError(e?.zoho));
        await createZohoErrorTask({
          leadId,
          submissionType: type,
          sessionId,
          email,
          phoneE164,
          errorMessage: "Attachment upload failed",
          zohoDetails: e?.zoho || null,
          submissionPayload: submission,
        });
      }
    }

    return res.json({
      success: true,
      lead_id: leadId,
      questionnaire_details_appended: appendResult.ok,
      questionnaire_details_error: appendResult.ok ? null : appendResult.zoho,
      pdf_uploaded: attachmentUploaded,
    });
  } catch (e) {
    const zohoSummary = summarizeZohoError(e?.zoho);
    console.error("[POST /api/submissions] main upsert failed:", zohoSummary);

    await createZohoErrorTask({
      leadId: lead?.id || null,
      submissionType: type,
      sessionId,
      email,
      phoneE164,
      errorMessage: "Main lead upsert failed",
      zohoDetails: e?.zoho || null,
      submissionPayload: submission,
    });

    return res.status(500).json({ success: false, error: "submission_failed", zoho: zohoSummary });
  }
});

const port = process.env.PORT || 10000;
app.listen(port, () => console.log(`API running on port ${port}`));
