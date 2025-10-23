// appnew.ts
import express, { Request, Response, NextFunction, Express } from "express";
import cors from "cors";
import dotenv from "dotenv";
import compression from "compression"; // ✅ NEW
import { MongoClient, Db } from "mongodb";
import { createServer } from "http";
import { Server as SocketIOServer } from "socket.io";

/* ---------- Market / data sockets & services ---------- */
import { DhanSocket } from "./socket/dhan.socket";
import { ltpRoutes } from "./routes/ltp.route";
import { setLtpDatabase } from "./services/ltp.service";
import {
  fetchMarketQuote,
  saveMarketQuote,
  fetchAndStoreInstruments,
  setQuoteDatabase,
  startFutstkOhlcRefresher, // background FUTSTK refresher (uses env)
} from "./services/quote.service";
import { setInstrumentDatabase } from "./services/instrument.service";

/* ---------- Core routes & middlewares ---------- */
import routes from "./routes";
import authRoutes from "./routes/auth.routes";
import otpRoutes from "./routes/otp.routes";
import { errorMiddleware } from "./middleware/error.middleware";
import { setDatabase as setAuthControllerDb } from "./controllers/auth.controller";

/* ---------- Public analytics/data APIs ---------- */
import AnalysisRoutes from "./api/analysis.api";
import registerNiftyRoutes from "./api/call_put";
import cash_dataRoutes from "./api/cash data.api";
import ClientRoutes from "./api/client.api";
import DIIRoutes from "./api/dii.api";
import FIIRoutes from "./api/fii.api";
import ProRoutes from "./api/pro.api";
import summaryRoutes from "./api/summary.api";
import { Stocks } from "./api/stocks";
import { AdvDec } from "./api/advdec";
import { Heatmap } from "./api/heatmap";

/* ---------- Commerce / users ---------- */
import productsRoutes, { setProductsDb } from "./routes/products.routes";
import paymentRoutes from "./routes/payment.routes";
import { setPaymentDatabase } from "./controllers/payment.controller";
import { setUserDatabase } from "./controllers/user.controller";

/* ---------- Journaling & instruments ---------- */
import registerDailyJournalRoutes from "./routes/dailyJournal.routes";
import registerTradeJournalRoutes from "./routes/tradeJournal.routes";
import registerTradeCalendarRoutes from "./routes/tradeCalendar.routes";
import instrumentRouter from "./routes/instruments";

/* ---------- Contact / careers ---------- */
import registerContactRoutes from "./api/contact";
import registerCareersRoutes from "./routes/Careers.routes";

/* ---------- Entitlements & Auth ---------- */
import {
  requireEntitlement,
  setRequireEntitlementDb,
} from "./middleware/requireEntitlement.middleware";
import { authenticate, setAuthDb } from "./middleware/auth.middleware";

/* ---------- Journal indexes bootstrap ---------- */
import { ensureCalendarIndexes } from "./services/snapshots";

/* ---------- PUBLIC Orderbook bundle (summary/strategies) ---------- */
import { Orderbook } from "./api/orderbook";
import registerFeedbackRoutes from "./routes/registerFeedbackRoutes";

/* ---------- Invoice ---------- */
import { registerInvoiceRoutes } from "./utils/invoice";

/* ---------- OC watcher deps ---------- */
import {
  fetchExpiryList,
  pickNearestExpiry,
  fetchOptionChainRaw,
  getLiveOptionChain,
  toNormalizedArray,
  DhanOptionLeg,
} from "./services/option_chain";
import { istNowString, istTimestamp } from "./utils/time";

/* ---------- Option Chain PUBLIC endpoints ---------- */
import registerOptionChainExpiries from "./api/optionchain/expiries";
import registerOptionChainSnapshot from "./api/optionchain/snapshot";

/* ---------- Global pacer (for logging gap) ---------- */
import { getDhanMinGap } from "./utils/dhanPacer";

/* ---------- Users / Admin ---------- */
import userRoutes from "./routes/user.routes";
import registerAdminRoutes from "./routes/admin.routes";

/* ---------- OC row & FUTSTK OHLC APIs ---------- */
import registerOcRow from "./api/oc_row.api";
import registerFutstkOhlcRoutes from "./api/futstk_ohlc.api";

/* ---------- GEX cache (Mongo-backed) ---------- */
import gexCacheRouter from "./routes/gex_cache.routes";
import { setGexCacheDb } from "./controllers/gex_cache.controller";

/* ---------- NEW: OC rows cache materializer & bulk endpoint ---------- */
import {
  startOcRowsMaterializer,
  ensureOcRowsIndexes,
} from "./services/oc_rows_cache";
import registerOcRowsBulk from "./api/oc_rows_bulk.api"; // ✅ NEW
import { GexLevelsCalc, startGexLevelsEveryMinute } from "./api/gexLevelsCalc";
import { AdvDecSave, startAdvDecMinuteJob } from "./api/advdecSave";

/* ---------- OTP in-memory store (for /api/ads OTP check) ---------- */
import { otpStore } from "./controllers/otp_send.controller";

dotenv.config();

/* ====================================================================== */
/* ================== Small utils (IST & time) ========================== */
/* ====================================================================== */
function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}
function getISTDate(): Date {
  return new Date();
}
function isMarketOpen(): boolean {
  const now = getISTDate();
  const day = now.getDay(); // 0=Sun,6=Sat
  if (day === 0 || day === 6) return false;
  const totalMinutes = now.getHours() * 60 + now.getMinutes();
  // NSE: 09:15–15:30 IST
  return totalMinutes >= 9 * 60 + 15 && totalMinutes <= 15 * 60 + 30;
}

/* ====================================================================== */
/* ================== Option Chain Watcher (safe scheduler) ============== */
/* ====================================================================== */

function strikeStepFromKeys(keys: string[]): number {
  const n = Math.min(keys.length, 20);
  const nums = keys
    .slice(0, n)
    .map((k) => Number(k))
    .filter(Number.isFinite);
  nums.sort((a, b) => a - b);
  let step = 50;
  for (let i = 1; i < nums.length; i++) {
    const diff = Math.abs(nums[i] - nums[i - 1]);
    if (diff > 0) {
      step = diff;
      break;
    }
  }
  return step;
}
function roundToStep(price: number, step: number): number {
  return Math.round(price / step) * step;
}
function isActive(leg?: DhanOptionLeg): boolean {
  if (!leg) return false;
  return (
    (leg.last_price ?? 0) > 0 ||
    (leg.top_bid_price ?? 0) > 0 ||
    (leg.top_ask_price ?? 0) > 0 ||
    (leg.oi ?? 0) > 0 ||
    (leg.volume ?? 0) > 0
  );
}

type OcWatcherHandle = { stop: () => void };

async function startOptionChainWatcher(db: Db): Promise<OcWatcherHandle> {
  if ((process.env.OC_DISABLED || "false").toLowerCase() === "true") {
    // console.log("⏸️  [OC] watcher disabled by env OC_DISABLED=true");
    return { stop: () => {} };
  }

  const sym = process.env.OC_SYMBOL || "NIFTY";
  const id = Number(process.env.OC_UNDERLYING_ID || 13);
  const seg = process.env.OC_SEGMENT || "IDX_I";
  const windowSteps = Number(process.env.OC_WINDOW_STEPS || 15);
  const pcrSteps = Number(process.env.OC_PCR_STEPS || 3);
  const verbose =
    (process.env.OC_LOG_VERBOSE || "true").toLowerCase() === "true";

  // Market-hours gate & closed sleep
  const MARKET_HOURS_ONLY =
    (process.env.OC_MARKET_HOURS_ONLY || "true").toLowerCase() !== "false";
  const CLOSED_SLEEP_MS = Math.max(
    10_000,
    Number(process.env.OC_CLOSED_MS || 60_000)
  );
  let lastClosedLog = 0; // rate-limit the closed log to once/min

  const BASE_MIN = 3100;
  let baseInterval = Math.max(Number(process.env.OC_LIVE_MS || 7000), BASE_MIN);

  let backoffSteps = 0;
  const MAX_BACKOFF_STEPS = 12;
  const STEP_MS = 1000;

  const START_OFFSET_MS = Number(process.env.OC_START_OFFSET_MS || 1600);

  // Helpful indexes (idempotent)
  try {
    await db
      .collection("option_chain")
      .createIndex(
        { underlying_security_id: 1, underlying_segment: 1, expiry: 1 },
        { unique: true }
      );
    await db.collection("option_chain").createIndex({ "strikes.strike": 1 });
    await db.collection("option_chain_ticks").createIndex({
      underlying_security_id: 1,
      underlying_segment: 1,
      expiry: 1,
      ts: 1,
    });
  } catch {}

  async function resolveExpiry(): Promise<string | null> {
    try {
      const exps = await fetchExpiryList(id, seg);
      const picked = pickNearestExpiry(exps);
      if (picked) {
        await sleep(3100);
        return picked;
      }
    } catch {
      /* ignore and try live fetch */
    }
    const res = await getLiveOptionChain(id, seg);
    // getLiveOptionChain may return null in some builds; normalize to null-safe here
    return (res as any)?.expiry ?? null;
  }

  let expiry: string | null = (process.env.OC_EXPIRY || "").trim() || null;
  if (!expiry) {
    // retry with gentle backoff until we get an expiry
    let waitMs = 15000;
    for (;;) {
      expiry = await resolveExpiry();
      if (expiry) break;
      console.warn(
        `⚠️ [OC] No expiry yet. Retrying in ${Math.floor(waitMs / 1000)}s...`
      );
      await sleep(waitMs);
      waitMs = Math.min(waitMs * 2, 5 * 60_000);
    }
  }

  console.log(
    `▶️  [OC] Live Option Chain for ${sym} ${id}/${seg} @ expiry ${expiry}`
  );
  console.log(
    `⏱️  [OC] Interval: ${baseInterval} ms (Dhan limit ≥ 3000 ms) | Global min gap: ${getDhanMinGap()} ms`
  );

  let stopped = false;
  let inFlight = false;

  const effectiveInterval = () =>
    Math.max(BASE_MIN, baseInterval + backoffSteps * STEP_MS) +
    Math.floor(Math.random() * 250);

  // Re-resolve expiry after a day change (first open tick of the day)
  let lastExpiryResolveDayKey = new Date().toISOString().slice(0, 10);
  const dayKey = () => new Date().toISOString().slice(0, 10);

  async function tickOnce() {
    if (stopped || inFlight) return;

    // Skip outside market hours (with rate-limited log)
    if (MARKET_HOURS_ONLY && !isMarketOpen()) {
      const now = Date.now();
      if (now - lastClosedLog > 60_000) {
        console.log("⏳ Market closed. Skipping Option chain.");
        lastClosedLog = now;
      }
      return;
    }

    inFlight = true;
    try {
      // Re-anchor expiry once per new day when we are within market hours
      const dk = dayKey();
      if (dk !== lastExpiryResolveDayKey) {
        try {
          const newExp = await resolveExpiry();
          if (newExp && newExp !== expiry) {
            console.log(
              `📅 [OC] New day detected → expiry ${expiry} → ${newExp}`
            );
            expiry = newExp;
          }
        } catch {}
        lastExpiryResolveDayKey = dk;
      }

      const ts = new Date();
      const ts_ist = istTimestamp(ts);

      if (!expiry) return; // guard
      const { data } = await fetchOptionChainRaw(id, seg, expiry); // paced inside service
      const norm = toNormalizedArray(data.oc);

      // Upsert latest snapshot
      await db.collection("option_chain").updateOne(
        { underlying_security_id: id, underlying_segment: seg, expiry },
        {
          $set: {
            underlying_security_id: id,
            underlying_segment: seg,
            underlying_symbol: sym,
            expiry,
            last_price: data.last_price,
            strikes: norm,
            updated_at: ts,
            updated_at_ist: ts_ist,
          },
        },
        { upsert: true }
      );

      // Append tick
      await db.collection("option_chain_ticks").insertOne({
        underlying_security_id: id,
        underlying_segment: seg,
        underlying_symbol: sym,
        expiry,
        last_price: data.last_price,
        strikes: norm,
        ts,
        ts_ist,
      });

      if (backoffSteps > 0) backoffSteps = Math.max(0, backoffSteps - 1);

      if (verbose) {
        const keys = Object.keys(data.oc);
        const step = strikeStepFromKeys(keys);
        const atm = roundToStep(data.last_price, step);

        const windowed = norm.filter(
          (r) => Math.abs(r.strike - atm) <= windowSteps * step
        );
        const ceOIAll = windowed.reduce((a, r) => a + (r.ce?.oi ?? 0), 0);
        const peOIAll = windowed.reduce((a, r) => a + (r.pe?.oi ?? 0), 0);
        const pcrAll = ceOIAll > 0 ? peOIAll / ceOIAll : 0;

        const near = norm.filter(
          (r) => Math.abs(r.strike - atm) <= pcrSteps * step
        );
        const ceOINear = near.reduce((a, r) => a + (r.ce?.oi ?? 0), 0);
        const peOINear = near.reduce((a, r) => a + (r.pe?.oi ?? 0), 0);
        const pcrNear = ceOINear > 0 ? peOINear / ceOINear : 0;

        console.log(
          `[${istNowString()}] [OC] LTP:${
            data.last_price
          } ATM:${atm} PCR(±win):${pcrAll.toFixed(
            2
          )} | PCR(near):${pcrNear.toFixed(2)}`
        );
      }
    } catch (e: any) {
      const status = e?.response?.status;
      const msg = status
        ? `${status} ${e?.response?.statusText || ""}`.trim()
        : e?.message || String(e);
      console.warn(`[OC] Tick error: ${msg}`);
      if (status === 429 || (status >= 500 && status < 600)) {
        backoffSteps = Math.min(MAX_BACKOFF_STEPS, backoffSteps + 1);
        console.warn(`[OC] Backing off → interval ~${effectiveInterval()} ms`);
      }
    } finally {
      inFlight = false;
    }
  }

  async function loop() {
    await sleep(START_OFFSET_MS); // stagger vs quotes
    while (!stopped) {
      await tickOnce();
      // Longer sleep when market is closed
      const delay =
        MARKET_HOURS_ONLY && !isMarketOpen()
          ? CLOSED_SLEEP_MS
          : effectiveInterval();
      await sleep(delay);
    }
  }

  loop();
  return {
    stop: () => {
      // actually stop
      stopped = true;
      console.log("🛑 [OC] watcher stopped.");
    },
  };
}

/* ====================================================================== */
/* ============================ App runtime ============================= */
/* ====================================================================== */

const app: Express = express();
const httpServer = createServer(app);

app.use(
  cors({
    origin: process.env.CLIENT_URL || "http://localhost:5173",
    credentials: true,
  })
);
app.use(compression()); // ✅ NEW: gzip/brotli
app.use(express.json());

/* ================== Global DB refs ================== */
let db: Db;
let mongoClient: MongoClient;

/* ================== Symbols for market quote polling ================== */
const securityIds = [
  40072, 40073, 40074, 40075, 40847, 40848, 42359, 42360, 42361, 42362, 42363,
];

const QUOTE_BASE_MIN = 3000;
const QUOTE_BASE_INTERVAL = Math.max(
  Number(process.env.QUOTE_INTERVAL_MS || 4500),
  QUOTE_BASE_MIN
);
const QUOTE_JITTER_MS = Number(process.env.QUOTE_JITTER_MS || 250);

async function startMarketQuotePolling() {
  if ((process.env.QUOTE_DISABLED || "false").toLowerCase() === "true") {
    console.log("⏸️  Market Quote Polling disabled by env QUOTE_DISABLED=true");
    return;
  }
  console.log("🚀 Starting Market Quote Polling...");
  let currentIndex = 0;
  let inFlight = false;

  let backoffSteps = 0;
  const MAX_BACKOFF_STEPS = 8;
  const STEP_MS = 500;

  const effectiveInterval = () =>
    Math.max(QUOTE_BASE_MIN, QUOTE_BASE_INTERVAL + backoffSteps * STEP_MS) +
    Math.floor(Math.random() * QUOTE_JITTER_MS);

  async function tickOnce() {
    if (!isMarketOpen()) {
      console.log("⏳ Market closed. Skipping Market Quote Polling.");
      return;
    }
    if (inFlight) return;
    inFlight = true;
    try {
      const batch = securityIds.slice(currentIndex, currentIndex + 1000);
      if (batch.length > 0) {
        const data = await fetchMarketQuote(batch); // paced + retried inside service
        await saveMarketQuote(data);
      }
      currentIndex += 1000;
      if (currentIndex >= securityIds.length) currentIndex = 0;
      if (backoffSteps > 0) backoffSteps = Math.max(0, backoffSteps - 1);
    } catch (err: any) {
      const status = err?.response?.status;
      if (status === 429 || (status >= 500 && status < 600)) {
        backoffSteps = Math.min(MAX_BACKOFF_STEPS, backoffSteps + 1);
        console.warn(`⚠ Quote poll backoff step=${backoffSteps}`);
      } else {
        console.error("❌ Error in Market Quote Polling:", err?.message || err);
      }
    } finally {
      inFlight = false;
    }
  }

  (async function loop() {
    while (true) {
      await tickOnce();
      await sleep(effectiveInterval());
    }
  })();
}

/* ================== Dhan WebSocket (LTP) ================== */
const dhanSocket = new DhanSocket(
  process.env.DHAN_API_KEY!,
  process.env.DHAN_CLIENT_ID!
);
function startWsIfOpen() {
  if (isMarketOpen()) {
    try {
      dhanSocket.connect(securityIds);
    } catch (e: any) {
      console.error("WS connect error:", e?.message || e);
    }
  } else {
    console.log("⏳ Market is closed. Skipping WebSocket connection.");
  }
}
startWsIfOpen();

/* ================== DB connect + server start ================== */
let ocWatcherHandle: OcWatcherHandle | null = null;
/* NEW: keep a ref to the OC rows materializer interval so we can clear it on shutdown */
let ocRowsTimer: NodeJS.Timeout | null = null;

const connectDB = async () => {
  try {
    if (!process.env.MONGO_URI || !process.env.MONGO_DB_NAME) {
      throw new Error("❌ Missing MongoDB URI or DB Name in .env");
    }

    const mongoUri = process.env.MONGO_URI;
    const mongoDbName = process.env.MONGO_DB_NAME;

    mongoClient = new MongoClient(mongoUri);
    await mongoClient.connect();
    db = mongoClient.db(mongoDbName);
    console.log("✅ Connected to MongoDB");

    /* ---- Inject DB into modules ---- */
    setAuthControllerDb(db);
    setAuthDb(db);
    setLtpDatabase(db);
    setQuoteDatabase(db);
    setProductsDb(db);
    setPaymentDatabase(db);
    setUserDatabase(db);
    setRequireEntitlementDb(db);
    setGexCacheDb(db);

    GexLevelsCalc(app, db);
    startGexLevelsEveryMinute(db);
    AdvDecSave(app, db); // register the API

    // start the 1-minute saver (never stops)
    startAdvDecMinuteJob(db, {
      bin: 5,
      sinceMin: 1440,
      // expiry: "2025-10-20", // optional
      symbol: "NIFTY",
    });

    // Helpful OC indexes & logs (optional)
    try {
      const oc = db.collection("option_chain");
      await oc.createIndexes([
        {
          key: {
            underlying_security_id: 1,
            underlying_segment: 1,
            expiry: 1,
            updated_at: -1,
          },
          name: "oc_core",
        },
        {
          key: { underlying_symbol: 1, expiry: 1, updated_at: -1 },
          name: "oc_symbol",
        },
      ]);
      const count = await oc.estimatedDocumentCount();
      const sample = await oc
        .find({})
        .project({
          underlying_security_id: 1,
          underlying_segment: 1,
          underlying_symbol: 1,
          expiry: 1,
          updated_at: 1,
        })
        .sort({ updated_at: -1, _id: -1 })
        .limit(2)
        .toArray();
      console.log("option_chain count:", count);
      console.log("option_chain latest samples:", sample);
    } catch (e) {
      console.warn("option_chain index/log skipped:", (e as any)?.message || e);
    }

    try {
      const ticks = db.collection("option_chain_ticks");
      await ticks.createIndexes([
        {
          key: {
            underlying_security_id: 1,
            underlying_segment: 1,
            expiry: 1,
            ts: -1,
          },
          name: "ticks_core",
        },
        {
          key: { underlying_symbol: 1, expiry: 1, ts: -1 },
          name: "ticks_symbol",
        },
      ]);
      const tickCount = await ticks.estimatedDocumentCount();
      console.log("option_chain_ticks count:", tickCount);
    } catch (e) {
      console.warn(
        "option_chain_ticks index/log skipped:",
        (e as any)?.message || e
      );
    }

    // ✅ Ensure cache indexes for oc_rows_cache
    try {
      await ensureOcRowsIndexes(db);
      // console.log("✅ oc_rows_cache indexes ensured");
    } catch (e) {
      console.warn(
        "oc_rows_cache index ensure skipped:",
        (e as any)?.message || e
      );
    }

    // Mount GEX cache router (canonical + alias)
    app.use("/api", gexCacheRouter); // /api/gex/nifty/cache
    app.use("/", gexCacheRouter); // /gex/nifty/cache

    await ensureCalendarIndexes(db).catch(() => {});

    /* ================== PUBLIC routes (no JWT required) ================== */
    app.use("/api/auth", authRoutes);
    app.use("/api/otp", otpRoutes);
    app.use("/api/products", productsRoutes);
    app.use("/api/payments", paymentRoutes);

    app.use("/api/instruments", instrumentRouter);
    app.use("/api/ltp", ltpRoutes);

    registerContactRoutes(app, db);
    app.use("/api/careers", registerCareersRoutes(db));
    app.use("/api/feedback", registerFeedbackRoutes(db));

    Orderbook(app, db);
    registerInvoiceRoutes(app, "/api/invoice");

    // Option Chain public endpoints
    registerOptionChainExpiries(app, db);
    registerOptionChainSnapshot(app, db);

    // OC Row APIs (includes cached and materialize endpoints)
    registerOcRow(app);
    registerOcRowsBulk(app, db); // ✅ NEW bulk cached endpoint
    setInstrumentDatabase(db);
    registerFutstkOhlcRoutes(app);

    // Legacy alias
    app.get(
      "/api/orders/triggered",
      (req: Request, _res: Response, next: NextFunction) => {
        const origQs = new URLSearchParams(req.url.split("?")[1] || "");
        const from = origQs.get("from") || "";
        const to = origQs.get("to") || "";
        const userId = origQs.get("userId") || "";
        const target = `/api/trades/list?from=${encodeURIComponent(
          from
        )}&to=${encodeURIComponent(to)}${
          userId ? `&userId=${encodeURIComponent(userId)}` : ""
        }`;
        req.url = target;
        next();
      }
    );

    AnalysisRoutes(app, db);
    registerNiftyRoutes(app, db);
    cash_dataRoutes(app, db);
    ClientRoutes(app, db);
    DIIRoutes(app, db);
    FIIRoutes(app, db);
    ProRoutes(app, db);
    summaryRoutes(app, db);
    Stocks(app, db);
    AdvDec(app, db);
    Heatmap(app, db);

    // 🔓 PUBLIC admin routes (leave here if intended public)
    app.use("/api/admin", registerAdminRoutes(db));

    /* ---------- PUBLIC: /api/ads with OTP verification ---------- */
    app.post("/api/ads", async (req: Request, res: Response) => {
      try {
        const { firstName, lastName, phone, message, otp } = req.body || {};
        const digits = String(phone || "").replace(/\D/g, "");
        if (!String(firstName || "").trim() || !String(lastName || "").trim()) {
          res.status(400).json({ error: "First and last name are required" });
          return;
        }
        if (digits.length !== 10) {
          res.status(400).json({ error: "Phone must be 10 digits" });
          return;
        }

        const rec = otpStore[digits];
        if (!rec) {
          res.status(401).json({ error: "No OTP request found for this number" });
          return;
        }

        const now = Math.floor(Date.now() / 1000);
        if (rec.expiresAt < now) {
          delete otpStore[digits];
          res.status(410).json({ error: "OTP expired. Please request a new one." });
          return;
        }

        const ok = rec.verified || String(rec.otp) === String(otp || "");
        if (!ok) {
          res.status(401).json({ error: "OTP not verified / incorrect" });
          return;
        }

        // Consume OTP to prevent reuse
        delete otpStore[digits];

        await db.collection("ads").insertOne({
          firstName: String(firstName).trim(),
          lastName: String(lastName).trim(),
          phone: digits,
          message: String(message || "").trim(),
          createdAt: new Date(),
        });

        res.json({ success: true, message: "Enquiry submitted" });
      } catch (e: any) {
        console.error("POST /api/ads error:", e?.message || e);
        res.status(500).json({ error: "Internal Server Error" });
      }
    });

    /* ================== Auth gate ================== */
    app.use(authenticate);

    app.use(
      "/api/journal",
      requireEntitlement("journaling", "journaling_solo")
    );
    app.use(
      "/api/daily-journal",
      requireEntitlement("journaling", "journaling_solo")
    );

    app.use("/api/fii", requireEntitlement("fii_dii_data"));
    app.use("/api/dii", requireEntitlement("fii_dii_data"));
    app.use("/api/pro", requireEntitlement("fii_dii_data"));
    app.use("/api/main-fii-dii", requireEntitlement("fii_dii_data"));

    // ✅ Call these register functions directly (do NOT pass them to app.use)
    registerTradeJournalRoutes(app, db);
    registerDailyJournalRoutes(app, db);
    registerTradeCalendarRoutes(app, db);

    app.use("/api/users", userRoutes);
    app.use("/api", routes);

    /* ================== Data boot + schedulers ================== */
    await fetchAndStoreInstruments();
    // startMarketQuotePolling(); // optional
    startFutstkOhlcRefresher(); // reads FUTSTK_REFRESH_* from .env
    ocWatcherHandle = await startOptionChainWatcher(db);

    // 🚀 Start the OC rows cache materializer (keeps 3/5/15/30m buckets hot; 24h backfill)
    try {
      ocRowsTimer = startOcRowsMaterializer({
        mongoUri,
        dbName: mongoDbName,
        underlyings: [
          {
            id: Number(process.env.OC_UNDERLYING_ID || 13),
            segment: process.env.OC_SEGMENT || "IDX_I",
          },
        ],
        intervals: [3, 5, 15, 30],
        sinceMs: 24 * 60 * 60 * 1000, // 24 hours
        scheduleMs: 60_000, // run every minute
        mode: "level",
        unit: "bps",
      });
      console.log("✅ OC rows materializer started (24h backfill)");
    } catch (e) {
      console.warn(
        "⚠️ Failed to start OC rows materializer:",
        (e as any)?.message || e
      );
    }

    /* ================== Error handler ================== */
    app.use((err: Error, req: Request, res: Response, next: NextFunction) => {
      errorMiddleware(err, req, res, next);
    });

    /* ================== Start HTTP + Socket.IO ================== */
    const PORT = Number(process.env.PORT) || 8000;
    httpServer.listen(PORT, () => {
      console.log(`🚀 Server running at http://localhost:${PORT}`);
      console.log(
        `🔗 Allowed CORS origin: ${
          process.env.CLIENT_URL || "http://localhost:5173"
        }`
      );
    });
  } catch (err) {
    console.error("❌ MongoDB connection error:", err);
    process.exit(1);
  }
};

connectDB();

/* ================== Socket.IO ================== */
const io = new SocketIOServer(httpServer, {
  cors: {
    origin: process.env.CLIENT_URL || "http://localhost:5173",
    methods: ["GET", "POST"],
  },
});
io.on("connection", (socket) => {
  console.log("🔌 New client connected:", socket.id);
  socket.on("disconnect", (reason) =>
    console.log(`Client disconnected (${socket.id}):`, reason)
  );
});

/* ================== Graceful shutdown ================== */
async function shutdown(code = 0) {
  console.log("🛑 Shutting down gracefully...");
  try {
    ocWatcherHandle?.stop();
  } catch {}
  try {
    if (ocRowsTimer) clearInterval(ocRowsTimer);
  } catch {}
  try {
    await mongoClient?.close();
  } catch {}
  httpServer.close(() => {
    console.log("✅ Server closed");
    process.exit(code);
  });
}
process.on("SIGINT", () => shutdown(0));
process.on("SIGTERM", () => shutdown(0));

export { io };
