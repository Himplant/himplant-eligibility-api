import "dotenv/config";
import express from "express";
import cors from "cors";
import fetch from "node-fetch";

const originalPort = process.env.PORT || "10000";
const externalPort = Number(originalPort) || 10000;
const internalPort = Number(process.env.INTERNAL_SERVER_PORT || externalPort + 1);

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

const app = express();
app.use(express.json({ limit: "5mb" }));
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

const inFlightLeadRequests = new Map();
const recentLeadResults = new Map();
const IN_FLIGHT_TTL_MS = 30_000;
const RECENT_RESULT_TTL_MS = 60_000;

function digitsOnly(value) {
  return String(value || "").replace(/\D/g, "");
}

function normalizeEmail(value) {
  return String(value || "").trim().toLowerCase();
}

function normalizePhoneE164(countryCodeOrDial, phoneNumber) {
  const raw = String(phoneNumber || "").trim();
  if (raw.startsWith("+")) {
    const cleaned = "+" + digitsOnly(raw);
    return cleaned.length > 1 ? cleaned : "";
  }
  const dial = digitsOnly(countryCodeOrDial);
  const pn = digitsOnly(raw);
  if (!dial && !pn) return "";
  if (!dial) return pn ? `+${pn}` : "";
  return pn ? `+${dial}${pn}` : `+${dial}`;
}

function getLeadIdentityKey(body) {
  if (String(body?.submission_type || "").toLowerCase() !== "lead") return null;
  const sessionId = String(body?.session_id || "").trim();
  const email = normalizeEmail(body?.email);
  const phone = normalizePhoneE164(body?.phone_country_code, body?.phone_number);
  if (!sessionId && !email && !phone) return null;
  return `lead:${sessionId}:${email}:${phone}`;
}

function cleanupMapLater(map, key, ttl) {
  setTimeout(() => map.delete(key), ttl).unref?.();
}

function headersForInternal(req) {
  const headers = { ...req.headers };
  delete headers.host;
  delete headers["content-length"];
  return headers;
}

async function forwardToInternal(req) {
  const url = `http://127.0.0.1:${internalPort}${req.originalUrl}`;
  const hasBody = !["GET", "HEAD"].includes(req.method.toUpperCase());
  const response = await fetch(url, {
    method: req.method,
    headers: headersForInternal(req),
    body: hasBody ? JSON.stringify(req.body || {}) : undefined,
  });

  const text = await response.text();
  return {
    status: response.status,
    contentType: response.headers.get("content-type") || "application/json",
    bodyText: text,
  };
}

function sendForwarded(res, forwarded) {
  res.status(forwarded.status);
  res.setHeader("Content-Type", forwarded.contentType);
  return res.send(forwarded.bodyText);
}

async function handleSubmission(req, res) {
  const key = getLeadIdentityKey(req.body || {});

  if (!key) {
    return sendForwarded(res, await forwardToInternal(req));
  }

  const now = Date.now();
  const cached = recentLeadResults.get(key);
  if (cached && cached.expiresAt > now) {
    return sendForwarded(res, cached.forwarded);
  }
  if (cached) recentLeadResults.delete(key);

  const existing = inFlightLeadRequests.get(key);
  if (existing) {
    return sendForwarded(res, await existing);
  }

  const promise = forwardToInternal(req)
    .then((forwarded) => {
      if (forwarded.status >= 200 && forwarded.status < 300) {
        recentLeadResults.set(key, { forwarded, expiresAt: Date.now() + RECENT_RESULT_TTL_MS });
        cleanupMapLater(recentLeadResults, key, RECENT_RESULT_TTL_MS);
      }
      return forwarded;
    })
    .finally(() => {
      inFlightLeadRequests.delete(key);
    });

  inFlightLeadRequests.set(key, promise);
  cleanupMapLater(inFlightLeadRequests, key, IN_FLIGHT_TTL_MS);

  return sendForwarded(res, await promise);
}

process.env.PORT = String(internalPort);
await import("./server.js");
process.env.PORT = originalPort;

app.post("/api/submissions", (req, res) => {
  handleSubmission(req, res).catch(async (error) => {
    console.error("[lead identity lock] fail-open:", String(error?.message || error));
    try {
      return sendForwarded(res, await forwardToInternal(req));
    } catch (forwardError) {
      console.error("[lead identity lock] forward failed:", String(forwardError?.message || forwardError));
      return res.status(500).json({ success: false, error: "submission failed" });
    }
  });
});

app.all("*", async (req, res) => {
  try {
    return sendForwarded(res, await forwardToInternal(req));
  } catch (error) {
    console.error("[lead identity lock] proxy failed:", String(error?.message || error));
    return res.status(500).json({ error: "proxy failed" });
  }
});

app.listen(externalPort, () => {
  console.log(`Lead identity lock proxy running on port ${externalPort}; internal API on ${internalPort}`);
});
