import "dotenv/config";
import express from "express";
import cors from "cors";
import fetch from "node-fetch";

const originalPort = process.env.PORT || "10000";
const externalPort = Number(originalPort) || 10000;
const internalPort = Number(process.env.INTERNAL_SERVER_PORT || externalPort + 1);

process.env.PORT = String(internalPort);
await import("./server.js");
process.env.PORT = originalPort;

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

const inFlightLeadSubmissions = new Map();
const recentLeadResults = new Map();
const IN_FLIGHT_TTL_MS = 15_000;
const RECENT_RESULT_TTL_MS = 60_000;

function normalizeText(value) {
  return String(value || "").trim().toLowerCase();
}

function digitsOnly(value) {
  return String(value || "").replace(/\D/g, "");
}

function makeLeadIdentityKey(body) {
  const type = normalizeText(body?.submission_type);
  if (type !== "lead") return null;

  const sessionId = normalizeText(body?.session_id);
  const email = normalizeText(body?.email);
  const phoneCountryCode = normalizeText(body?.phone_country_code);
  const phoneNumber = digitsOnly(body?.phone_number);
  const phone = `${phoneCountryCode}:${phoneNumber}`;

  if (!sessionId && !email && phone === ":") return null;
  return `lead:${sessionId}:${email}:${phone}`;
}

async function postToInternal(body, headers = {}) {
  const response = await fetch(`http://127.0.0.1:${internalPort}/api/submissions`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
      Authorization: headers.authorization || headers.Authorization || "",
    },
    body: JSON.stringify(body || {}),
  });

  const text = await response.text();
  let parsed = null;
  try {
    parsed = text ? JSON.parse(text) : null;
  } catch {
    parsed = { raw: text };
  }

  return {
    status: response.status,
    body: parsed,
  };
}

async function withLeadIdentityLock(key, fn) {
  const now = Date.now();
  const cached = recentLeadResults.get(key);
  if (cached && cached.expiresAt > now) {
    return {
      ...cached.result,
      body: {
        ...(cached.result.body || {}),
        duplicate_replay: true,
      },
    };
  }
  if (cached) recentLeadResults.delete(key);

  const existing = inFlightLeadSubmissions.get(key);
  if (existing) return existing;

  const promise = (async () => {
    const result = await fn();
    if (result?.status >= 200 && result?.status < 300 && result?.body?.success !== false) {
      recentLeadResults.set(key, { result, expiresAt: Date.now() + RECENT_RESULT_TTL_MS });
      setTimeout(() => recentLeadResults.delete(key), RECENT_RESULT_TTL_MS).unref?.();
    }
    return result;
  })();

  inFlightLeadSubmissions.set(key, promise);
  setTimeout(() => inFlightLeadSubmissions.delete(key), IN_FLIGHT_TTL_MS).unref?.();

  try {
    return await promise;
  } finally {
    inFlightLeadSubmissions.delete(key);
  }
}

app.post("/api/submissions", async (req, res) => {
  const key = makeLeadIdentityKey(req.body);

  try {
    const result = key
      ? await withLeadIdentityLock(key, () => postToInternal(req.body, req.headers))
      : await postToInternal(req.body, req.headers);

    return res.status(result.status).json(result.body);
  } catch (error) {
    // Fail open. Lead capture is more important than the wrapper. If the lock/proxy
    // path ever throws, make one direct attempt to the normal server.js handler.
    console.error("[identity-lock wrapper] fail-open direct submission:", String(error?.message || error));
    try {
      const fallback = await postToInternal(req.body, req.headers);
      return res.status(fallback.status).json(fallback.body);
    } catch (fallbackError) {
      console.error("[identity-lock wrapper] fallback failed:", String(fallbackError?.message || fallbackError));
      return res.status(500).json({ success: false, error: "submission failed" });
    }
  }
});

app.use(async (req, res) => {
  const target = `http://127.0.0.1:${internalPort}${req.originalUrl}`;
  try {
    const response = await fetch(target, {
      method: req.method,
      headers: {
        Accept: req.headers.accept || "application/json",
        "Content-Type": req.headers["content-type"] || "application/json",
        Authorization: req.headers.authorization || "",
      },
      body: ["GET", "HEAD"].includes(req.method) ? undefined : JSON.stringify(req.body || {}),
    });
    const text = await response.text();
    res.status(response.status);
    const contentType = response.headers.get("content-type");
    if (contentType) res.setHeader("content-type", contentType);
    return res.send(text);
  } catch (error) {
    console.error("[identity-lock wrapper] proxy failed:", String(error?.message || error));
    return res.status(502).json({ success: false, error: "proxy failed" });
  }
});

app.listen(externalPort, () => {
  console.log(`Identity-lock wrapper running on port ${externalPort}; internal server.js on ${internalPort}`);
});
