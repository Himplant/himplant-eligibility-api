import express from "express";
import cors from "cors";
import fetch from "node-fetch";

const app = express();

// Bigger limit so we don't randomly fail on size
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
  DEBUG_ZOHO,
} = process.env;

const MODULE_SURGEONS = "Surgeons";
const MODULE_LEADS = "Leads";
const MODULE_TASKS = "Tasks";

const FIELD_QUESTIONNAIRE_DETAILS = "Questionnaire_Details";
const QUESTIONNAIRE_DETAILS_MAX_CHARS = 28000;

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

// token cache
let cachedAccessToken = null;
let tokenExpiresAt = 0;

const DEBUG = String(DEBUG_ZOHO || "").toLowerCase() === "true";

function debugLog(...args) {
  if (DEBUG) console.log(...args);
}

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

// -------------------------
// Zoho auth + requests
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
// Error Task
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

  const subjectBits = [
    "Eligibility API Error",
    submissionType ? `(${submissionType})` : "",
    email ? `email:${email}` : "",
    phoneE164 ? `phone:${phoneE164}` : "",
  ].filter(Boolean);

  const subject = subjectBits.join(" ").trim() || "Eligibility API Error";

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
    console.error("[Zoho Task] failed:", String(e.message || e));
  }
}

// -------------------------
// Health
// -------------------------
app.get("/health", (req, res) => res.json({ ok: true }));

// -------------------------
// Surgeons endpoints
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

async function getLeadByIdForAppend(leadId) {
  const record = await zohoGET(`/crm/v2/${MODULE_LEADS}/${leadId}`);
  return record?.data?.[0] || null;
}

// -------------------------
// Questionnaire_Details append logic
// -------------------------
function buildEntryString(submission, type, extra = {}) {
  const ts = new Date().toISOString();
  const header =
    `===== ${ts} | submission_type=${type || ""}` +
    (extra.sessionId ? ` | session_id=${extra.sessionId}` : "") +
    (extra.email ? ` | email=${extra.email}` : "") +
    (extra.phone ? ` | phone=${extra.phone}` : "") +
    ` =====\n`;

  return header + safeJsonStringify(submission, QUESTIONNAIRE_DETAILS_MAX_CHARS) + "\n\n";
}

function clampToMaxCharsKeepNewest(text, maxChars) {
  const s = String(text || "");
  if (s.length <= maxChars) return s;
  return "[TRUNCATED_OLD_ENTRIES]\n\n" + s.slice(s.length - maxChars);
}

async function attachAppendedQuestionnaireDetails(leadIdOrNull, payload, submission, type, ctx) {
  const newEntry = buildEntryString(submission, type, ctx);

  if (!leadIdOrNull) {
    return {
      ...payload,
      [FIELD_QUESTIONNAIRE_DETAILS]: clampToMaxCharsKeepNewest(newEntry, QUESTIONNAIRE_DETAILS_MAX_CHARS),
    };
  }

  let existing = "";
  try {
    const lead = await getLeadByIdForAppend(leadIdOrNull);
    existing = String(lead?.[FIELD_QUESTIONNAIRE_DETAILS] || "");
  } catch {
    existing = "";
  }

  const combined = existing ? (existing + "\n" + newEntry) : newEntry;

  return {
    ...payload,
    [FIELD_QUESTIONNAIRE_DETAILS]: clampToMaxCharsKeepNewest(combined, QUESTIONNAIRE_DETAILS_MAX_CHARS),
  };
}

// -------------------------
// Zoho mapping
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
      submission.recent_outbreak_6mo === null ||
      submission.recent_outbreak_6mo === undefined
        ? null
        : boolToYesNo(submission.recent_outbreak_6mo),
    Medical_conditions_list: toMultilineText(submission.medical_conditions_list),
    Body_Type: nullableText(submission.body_type),
    Outcome: nullableText(submission.outcome),
    Intake_Date: nullableText(submission.submitted_at) || new Date().toISOString(),
  });
}

// -------------------------
// Zoho errors + recovery
// -------------------------
function stripPhoneFields(payload) {
  const { Phone, Mobile, ...rest } = payload || {};
  return rest;
}

function isDuplicatePhoneZohoError(zohoErr) {
  const code = zohoErr?.code;
  const api = zohoErr?.details?.api_name;
  return code === "DUPLICATE_DATA" && (api === "Phone" || api === "Mobile");
}

function getApiNameFromZohoErr(zohoErr) {
  return zohoErr?.details?.api_name || null;
}

function isInvalidDataLike(zohoErr) {
  const code = zohoErr?.code;
  return code === "INVALID_DATA" || code === "MANDATORY_NOT_FOUND";
}

// -------------------------
// Zoho upsert
// NOTE: Zoho can return HTTP 200 with data[0].status="error"
// We treat those as httpStatus=400 so caller can react.
// -------------------------
async function getAccessTokenOrThrow() {
  return await getAccessToken();
}

async function createLead(payload) {
  const resp = await zohoPOST(`/crm/v2/${MODULE_LEADS}`, { data: [payload] });
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

async function updateLeadOnce(leadId, payload) {
  const resp = await zohoPUT(`/crm/v2/${MODULE_LEADS}/${leadId}`, { data: [payload] });
  const first = resp?.data?.[0];

  if (first?.status !== "success") {
    const err = new Error("updateLead failed");
    err.zoho = first || resp;
    err.httpStatus = 400;
    throw err;
  }
  return { ok: true, resp };
}

/**
 * Auto-fix loop:
 * - if Zoho says field X is invalid, remove X and retry (keeps Questionnaire_Details)
 * - returns list of removed fields so we can permanently fix mapping next
 */
async function updateLeadWithAutoFix(leadId, payload) {
  let working = { ...payload };
  const removed = [];

  for (let attempt = 1; attempt <= 8; attempt++) {
    try {
      const out = await updateLeadOnce(leadId, working);
      return { success: true, removedFields: removed, zohoResponse: out.resp };
    } catch (e) {
      const zohoErr = e?.zoho || null;

      if (isDuplicatePhoneZohoError(zohoErr)) {
        working = stripPhoneFields(working);
        removed.push("Phone");
        removed.push("Mobile");
        continue;
      }

      if (isInvalidDataLike(zohoErr)) {
        const apiName = getApiNameFromZohoErr(zohoErr);
        if (apiName && Object.prototype.hasOwnProperty.call(working, apiName)) {
          debugLog(`[Zoho reject] removing field ${apiName} and retrying...`);
          delete working[apiName];
          removed.push(apiName);
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

// -------------------------
// Zoho task creation toggle
// -------------------------
async function createZohoErrorTaskSafe(args) {
  try {
    await createZohoErrorTask(args);
  } catch {}
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

  let lead = null;
  let removedFields = [];

  try {
    if (!["lead", "partial", "complete"].includes(type)) {
      return res.status(400).json({
        success: false,
        error: "submission_type must be 'lead', 'partial', or 'complete'.",
      });
    }

    if ((type === "lead" || type === "complete") && !sessionId) {
      return res.status(400).json({ success: false, error: `${type} submission requires session_id.` });
    }

    if (!sessionId && !email && !phoneE164) {
      return res.status(400).json({ success: false, error: "Need session_id, email, or phone." });
    }

    // Find lead
    if (type === "complete" && sessionId) lead = await searchLeadBySessionId(sessionId);
    if (!lead && email) lead = await searchLeadByEmail(email);
    if (!lead && phoneE164) lead = await searchLeadByPhoneE164(phoneE164);

    let payloadBase =
      type === "lead"
        ? mapLeadBase(submission)
        : type === "partial"
          ? mapPartialBase(submission)
          : mapCompleteBase(submission);

    if (type === "complete" && lead?.id && sessionId) payloadBase.Session_ID = sessionId;

    let payload = await attachAppendedQuestionnaireDetails(
      lead?.id || null,
      payloadBase,
      submission,
      type,
      { sessionId, email, phone: phoneE164 }
    );

    if (lead?.id) {
      const upd = await updateLeadWithAutoFix(lead.id, payload);
      removedFields = upd.removedFields || [];
    } else {
      const newId = await createLead(payload);
      lead = { id: newId };
    }

    // Return success; include removed fields for debugging in browser
    return res.json({
      success: true,
      removed_fields: removedFields,
    });
  } catch (e) {
    const zohoErr = e?.zoho || null;
    const zohoHttp = e?.httpStatus || null;

    await createZohoErrorTaskSafe({
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

    // For Zoho 4xx, return 200 so the client isn't blocked.
    const nonRetryable = zohoHttp && zohoHttp >= 400 && zohoHttp < 500;

    if (nonRetryable) {
      return res.status(200).json({
        success: true,
        warning: "zoho_rejected_payload_manual_review_needed",
        zoho_http: zohoHttp,
        zoho_code: zohoErr?.code || null,
        zoho_api_name: zohoErr?.details?.api_name || null,
        zoho_message: zohoErr?.message || null,
        // include full zoho error body ONLY in debug mode
        zoho_debug: DEBUG ? zohoErr : undefined,
      });
    }

    return res.status(500).json({
      success: false,
      error: "submission failed",
      zoho_debug: DEBUG ? zohoErr : undefined,
    });
  }
});

const port = process.env.PORT || 10000;
app.listen(port, () => console.log(`API running on port ${port}`));
