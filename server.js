import express from "express";
import cors from "cors";
import fetch from "node-fetch";

const app = express();
app.use(express.json({ limit: "1mb" }));

// Tighten later to your Render frontend + Webflow domains
app.use(cors({ origin: "*", methods: ["GET", "POST", "OPTIONS"] }));

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

// Access token cache
let cachedAccessToken = null;
let tokenExpiresAt = 0;

async function getAccessToken() {
  const now = Date.now();
  if (cachedAccessToken && now < tokenExpiresAt - 60_000) return cachedAccessToken;

  const params = new URLSearchParams({
    refresh_token: ZOHO_REFRESH_TOKEN,
    client_id: ZOHO_CLIENT_ID,
    client_secret: ZOHO_CLIENT_SECRET,
    grant_type: "refresh_token"
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
    headers: { Authorization: `Zoho-oauthtoken ${token}` }
  });
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); } catch { data = { raw: text }; }
  if (!res.ok) throw new Error(`Zoho API error ${res.status}: ${JSON.stringify(data)}`);
  return data;
}

app.get("/health", (req, res) => res.json({ ok: true }));

// A) Active countries
app.get("/api/geo/countries", async (req, res) => {
  try {
    const criteria = `(${FIELD_ACTIVE}:equals:true)`;
    const data = await zohoGET(`/crm/v2/${MODULE_SURGEONS}/search?criteria=${encodeURIComponent(criteria)}`);

    const countries = new Set();
    for (const r of (data?.data || [])) {
      const c = (r?.[FIELD_COUNTRY] || "").toString().trim();
      if (c) countries.add(c);
    }

    res.json(Array.from(countries).sort((a, b) => a.localeCompare(b)));
  } catch (e) {
    res.status(500).json({ error: "countries lookup failed", details: String(e.message || e) });
  }
});

// B) Active cities for a country
app.get("/api/geo/cities", async (req, res) => {
  try {
    const country = (req.query.country || "").toString().trim();
    if (!country) return res.status(400).json({ error: "country is required" });

    const criteria = `(${FIELD_ACTIVE}:equals:true) and (${FIELD_COUNTRY}:equals:${country})`;
    const data = await zohoGET(`/crm/v2/${MODULE_SURGEONS}/search?criteria=${encodeURIComponent(criteria)}`);

    const cities = new Set();
    for (const r of (data?.data || [])) {
      const city = (r?.[FIELD_CITY] || "").toString().trim();
      if (city) cities.add(city);
    }

    res.json(Array.from(cities).sort((a, b) => a.localeCompare(b)));
  } catch (e) {
    res.status(500).json({ error: "cities lookup failed", details: String(e.message || e) });
  }
});

// C) Surgeons for country+city (includes price)
app.get("/api/surgeons", async (req, res) => {
  try {
    const country = (req.query.country || "").toString().trim();
    const city = (req.query.city || "").toString().trim();
    const lang = (req.query.lang || "en").toString().trim().toLowerCase();

    if (!country) return res.status(400).json({ error: "country is required" });
    if (!city) return res.status(400).json({ error: "city is required" });

    const criteria =
      `(${FIELD_ACTIVE}:equals:true) and (${FIELD_COUNTRY}:equals:${country}) and (${FIELD_CITY}:equals:${city})`;

    const data = await zohoGET(`/crm/v2/${MODULE_SURGEONS}/search?criteria=${encodeURIComponent(criteria)}`);

    const surgeons = (data?.data || []).map((r) => {
      const bookingField = lang === "es" ? FIELD_BOOK_ES : lang === "ar" ? FIELD_BOOK_AR : FIELD_BOOK_EN;
      return {
        id: r.id,
        name: r?.[FIELD_NAME] || "",
        price: r?.[FIELD_PRICE] ?? null,
        bookingAvailable: !!(r?.[bookingField] || r?.[FIELD_BOOK_EN])
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

    let bookingUrl = "";
    if (lang === "es") bookingUrl = s[FIELD_BOOK_ES] || "";
    else if (lang === "ar") bookingUrl = s[FIELD_BOOK_AR] || "";
    else bookingUrl = s[FIELD_BOOK_EN] || "";
    if (!bookingUrl) bookingUrl = s[FIELD_BOOK_EN] || "";

    res.json({
      id: surgeonId,
      name: s[FIELD_NAME] || "",
      price: s[FIELD_PRICE] ?? null,
      bookingUrl: bookingUrl || null
    });
  } catch (e) {
    res.status(500).json({ error: "surgeon lookup failed", details: String(e.message || e) });
  }
});

const port = process.env.PORT || 10000;
app.listen(port, () => console.log(`API running on port ${port}`));
