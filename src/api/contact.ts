import type { Express, Request, Response, NextFunction, RequestHandler } from "express";
import type { Db, WithId, Document } from "mongodb";

// ---- Minimal in-memory rate limiter (per IP) ----
const RATE_LIMIT_WINDOW_MS = 60_000; // 1 minute
const RATE_LIMIT_MAX = 10; // 10 requests/IP/min
const ipHits = new Map<string, { count: number; resetAt: number }>();

const rateLimit: RequestHandler = (req: Request, res: Response, next: NextFunction) => {
  const fwd = req.headers["x-forwarded-for"];
  const first = Array.isArray(fwd) ? fwd[0] : (fwd || "");
  const ip = (first ? first.split(",")[0].trim() : "") || req.socket.remoteAddress || "unknown";

  const now = Date.now();
  const rec = ipHits.get(ip);

  if (!rec || now > rec.resetAt) {
    ipHits.set(ip, { count: 1, resetAt: now + RATE_LIMIT_WINDOW_MS });
    next();
    return;
  }

  if (rec.count >= RATE_LIMIT_MAX) {
    res.status(429).json({ error: "Too many requests. Please try again shortly." });
    return;
  }

  rec.count += 1;
  next();
};

// ---- Validation ----
type ContactPayload = {
  firstName: string;
  lastName: string;
  email: string;
  company: string;   // mobile number
  persona: string;   // product (will be canonicalized)
  message: string;
  agree: boolean;
  website?: string;  // honeypot
};

const ALLOWED_PERSONAS = new Set<string>([
  "2-in-1 Trader's Essential Bundle",
  "ALGO Simulator",
  "Both / Not sure",
  "Select a product",
]);

function canonicalizePersona(p: string) {
  if (!p) return p;
  let v = String(p)
    .replace(/[\u2018\u2019\u2032]/g, "'") // smart -> straight apostrophe
    .replace(/[\u2010-\u2015]/g, "-")      // any dash -> hyphen-minus
    .replace(/\s+/g, " ")
    .trim();

  if (/^2\s*-?\s*in\s*-?\s*1\s+trader'?s?\s+essential\s+bundle$/i.test(v)) {
    return "2-in-1 Trader's Essential Bundle";
  }
  return v;
}

function isEmail(x: string) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(x);
}
function isPhoneLike(x: string) {
  const digits = (x || "").replace(/\D/g, "");
  return digits.length >= 8;
}
function validateBody(body: any): { ok: boolean; error?: string; personaCanonical?: string } {
  const b: ContactPayload = body || {};
  if (!b.firstName || typeof b.firstName !== "string") return { ok: false, error: "firstName is required" };
  if (!b.lastName || typeof b.lastName !== "string") return { ok: false, error: "lastName is required" };
  if (!b.email || !isEmail(b.email)) return { ok: false, error: "Valid email is required" };
  if (b.company && !isPhoneLike(b.company)) return { ok: false, error: "Mobile number looks invalid" };

  const personaCanonical = canonicalizePersona(b.persona || "");
  if (!personaCanonical || !ALLOWED_PERSONAS.has(personaCanonical)) {
    return { ok: false, error: "Please select a valid product" };
  }

  if (typeof b.agree !== "boolean" || !b.agree) return { ok: false, error: "You must agree to Terms & Privacy" };
  if (typeof b.message !== "string") return { ok: false, error: "message must be a string" };
  if (b.website && b.website.trim().length > 0) return { ok: false, error: "Spam detected" };
  return { ok: true, personaCanonical };
}

// ---- Index Helper (idempotent) ----
async function ensureIndexes(collectionName: string, db: Db) {
  const col = db.collection(collectionName);
  await col.createIndex({ createdAt: -1 });
  await col.createIndex({ email: 1, createdAt: -1 });
  await col.createIndex({ persona: 1, createdAt: -1 });
}

export default function registerContactRoutes(app: Express, db: Db) {
  const COLLECTION = "contact_messages";

  // Ensure indexes on boot (fire-and-forget)
  void ensureIndexes(COLLECTION, db).catch((e) =>
    console.warn("Index creation error (contact_messages):", e)
  );

  // POST /api/contact — save a contact submission
  const postContact: RequestHandler = async (req: Request, res: Response) => {
    try {
      const check = validateBody(req.body);
      if (!check.ok) {
        res.status(400).json({ error: check.error });
        return;
      }

      const payload: ContactPayload = req.body;
      const personaCanonical = check.personaCanonical!;

      const doc = {
        firstName: payload.firstName.trim(),
        lastName: payload.lastName.trim(),
        email: payload.email.trim().toLowerCase(),
        mobile: (payload.company || "").trim(),
        persona: personaCanonical, // store canonical enum value
        message: (payload.message || "").trim(),
        agree: !!payload.agree,
        createdAt: new Date(),
        status: "new" as "new" | "contacted" | "closed",
        meta: {
          ip: (() => {
            const fwd = req.headers["x-forwarded-for"];
            const first = Array.isArray(fwd) ? fwd[0] : (fwd || "");
            return (first ? first.split(",")[0].trim() : "") || req.socket.remoteAddress || null;
          })(),
          ua: req.headers["user-agent"] || null,
          referer: req.headers["referer"] || null,
          personaLabel: typeof (req.body as any).personaLabel === "string" ? (req.body as any).personaLabel : null,
        },
      };

      const col = db.collection(COLLECTION);
      const result = await col.insertOne(doc);

      res.status(201).json({
        ok: true,
        id: result.insertedId,
        message: "Thanks! We received your message.",
      });
      return;
    } catch (err: any) {
      console.error("POST /api/contact error:", err);
      res.status(500).json({ error: "Internal server error" });
      return;
    }
  };

  // GET /api/contact — list recent messages (admin)
  const getContactList: RequestHandler = async (req: Request, res: Response) => {
    try {
      const secret = req.header("x-admin-secret");
      if (!process.env.CONTACT_ADMIN_SECRET || secret !== process.env.CONTACT_ADMIN_SECRET) {
        res.status(403).json({ error: "Forbidden" });
        return;
      }

      const col = db.collection(COLLECTION);
      const items: WithId<Document>[] = await col
        .find({})
        .sort({ createdAt: -1 })
        .limit(200)
        .toArray();

      res.json({ ok: true, count: items.length, items });
      return;
    } catch (err: any) {
      console.error("GET /api/contact error:", err);
      res.status(500).json({ error: "Internal server error" });
      return;
    }
  };

  app.post("/api/contact", rateLimit, postContact);
  app.get("/api/contact", getContactList);
}
