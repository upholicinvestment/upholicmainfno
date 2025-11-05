// src/appnew.ts
import express, { Request, Response, NextFunction, Express } from "express";
import cors from "cors";
import dotenv from "dotenv";
import compression from "compression";
import { Db } from "mongodb";
import { createServer } from "http";
import { Server as SocketIOServer } from "socket.io";

dotenv.config();

/* ---------- DB helper (single place) ---------- */
import {
  connectPrimary,
  connectFno,
  getPrimaryDb,
  getFnoDb,
  closeAll,
} from "./utils/db";

/* ---------- Core routes & middlewares ---------- */
import routes from "./routes";
import authRoutes from "./routes/auth.routes";
import otpRoutes from "./routes/otp.routes";
import { errorMiddleware } from "./middleware/error.middleware";
import { setDatabase as setAuthControllerDb } from "./controllers/auth.controller";

/* ---------- Public analytics/data APIs (light) ---------- */
import AnalysisRoutes from "./api/analysis.api";
import registerNiftyRoutes from "./api/call_put"; // <-- should use fnoDb
import cash_dataRoutes from "./api/cash data.api";
import ClientRoutes from "./api/client.api";
import DIIRoutes from "./api/dii.api";
import FIIRoutes from "./api/fii.api";
import ProRoutes from "./api/pro.api";
import summaryRoutes from "./api/summary.api";
import { Stocks } from "./api/stocks";
import { AdvDec } from "./api/advdec"; // <-- should use fnoDb
import { Heatmap } from "./api/heatmap"; // <-- should use fnoDb

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

/* ---------- OC Row & FUTSTK OHLC APIs (light endpoints) ---------- */
import registerOcRow from "./api/oc_row.api";
import registerFutstkOhlcRoutes from "./api/futstk_ohlc.api";

/* ---------- GEX cache (Mongo-backed) ---------- */
import gexCacheRouter from "./routes/gex_cache.routes";
import { setGexCacheDb } from "./controllers/gex_cache.controller";

/* ---------- OC rows cache bulk endpoints (if present) ---------- */
import { ensureOcRowsIndexes, startOcRowsMaterializer } from "./services/oc_rows_cache";
import registerOcRowsBulk from "./api/oc_rows_bulk.api";

/* ---------- Admin / users ---------- */
import userRoutes from "./routes/user.routes";
import registerAdminRoutes from "./routes/admin.routes";

/* ---------- Misc ---------- */
import gexBulkRouter from "./routes/gex_bulk.routes";
import { setGexBulkDb } from "./controllers/gex_bulk.controller";

const app: Express = express();
const httpServer = createServer(app);

/* ===== new: ocRowsTimer ref so we can clear the materializer on shutdown ===== */
let ocRowsTimer: NodeJS.Timeout | null = null;

app.use(
  cors({
    origin: process.env.CLIENT_URL || "http://localhost:5173",
    credentials: true,
  })
);
app.use(compression());
app.use(express.json());

/**
 * Start function: connect to PRIMARY and FNO DBs,
 * wire routes & services and start the HTTP server.
 */
async function startServer() {
  try {
    // connect primary DB and obtain DB handle
    await connectPrimary();
    const primaryDb: Db = getPrimaryDb();
    console.log("✅ Connected to PRIMARY MongoDB (appnew.ts)");

    // connect FNO DB and obtain handle
    await connectFno();
    const fnoDb: Db = getFnoDb();
    console.log("✅ Connected to FNO MongoDB (appnew.ts)");

    /* ---- Inject DB into controllers / middleware (PRIMARY) ---- */
    setAuthControllerDb(primaryDb);
    setAuthDb(primaryDb);

    // commerce / payments / products -> primaryDb
    setProductsDb(primaryDb);
    setPaymentDatabase(primaryDb);
    setUserDatabase(primaryDb);
    setRequireEntitlementDb(primaryDb);

    // GEX cache / bulk should use FNO DB per your note
    setGexCacheDb(fnoDb);
    setGexBulkDb(fnoDb);

    /* ============ PUBLIC ROUTES (no JWT) ============ */
    app.use("/api/auth", authRoutes);
    app.use("/api/otp", otpRoutes);
    app.use("/api/products", productsRoutes);
    app.use("/api/payments", paymentRoutes);

    app.use("/api/instruments", instrumentRouter);

    registerContactRoutes(app, primaryDb);
    app.use("/api/careers", registerCareersRoutes(primaryDb));
    app.use("/api/feedback", registerFeedbackRoutes(primaryDb));

    // Lightweight analytics/public APIs
    AnalysisRoutes(app, primaryDb);
    cash_dataRoutes(app, primaryDb);
    ClientRoutes(app, primaryDb);
    DIIRoutes(app, primaryDb);
    FIIRoutes(app, primaryDb);
    ProRoutes(app, primaryDb);
    summaryRoutes(app, primaryDb);

    // Stocks likely belong in primary DB (keep as before)
    Stocks(app, primaryDb);

    // ===== FNO-backed endpoints (explicitly use fnoDb) =====
    // Option chain / call-put (call_put.ts) -> fnoDb
    registerNiftyRoutes(app, fnoDb);

    // AdvDec -> fnoDb
    AdvDec(app, fnoDb);

    // Heatmap (FUTSTK ohlc) -> fnoDb
    Heatmap(app, fnoDb);

    // FUTSTK OHLC / OC rows bulk are FNO data
    registerOcRow(app); // if this is a thin wrapper that reads oc_rows_cache it may not need db param
    registerOcRowsBulk(app, fnoDb);
    registerFutstkOhlcRoutes(app, fnoDb);

    // GEX cache routers (read-only endpoints) served from fnoDb
    app.use("/api", gexCacheRouter);
    app.use("/api", gexBulkRouter);

    // Invoice / Orderbook (keep primary unless your orderbook reads FNO)
    Orderbook(app, primaryDb);
    registerInvoiceRoutes(app, "/api/invoice");

    // Admin (protected or public depending on implementation)
    app.use("/api/admin", registerAdminRoutes(primaryDb));

    /* ================== Auth gate (JWT-protected below) ================== */
    app.use(authenticate);

    // Entitlement-protected APIs (primary)
    app.use("/api/journal", requireEntitlement("journaling", "journaling_solo"));
    app.use("/api/daily-journal", requireEntitlement("journaling", "journaling_solo"));

    app.use("/api/fii", requireEntitlement("fii_dii_data"));
    app.use("/api/dii", requireEntitlement("fii_dii_data"));
    app.use("/api/pro", requireEntitlement("fii_dii_data"));
    app.use("/api/main-fii-dii", requireEntitlement("fii_dii_data"));

    // Journals, calendar, users (primary DB)
    app.use("/api", registerTradeJournalRoutes(primaryDb));
    app.use("/api/daily-journal", registerDailyJournalRoutes(primaryDb));
    app.use("/api/trade-calendar", registerTradeCalendarRoutes(primaryDb));
    app.use("/api/users", userRoutes);
    app.use("/api", routes);

    /* ================== Helpful indexes / housekeeping ================== */
    try {
      await ensureCalendarIndexes(primaryDb);
    } catch (e) {
      console.warn("ensureCalendarIndexes failed:", (e as any)?.message || e);
    }

    // oc_rows_cache indexes live in FNO DB (ensure on fnoDb)
    try {
      await ensureOcRowsIndexes(fnoDb);
      console.log("✅ oc_rows_cache indexes ensured (appnew.ts)");
    } catch (e) {
      console.warn("ensureOcRowsIndexes skipped:", (e as any)?.message || e);
    }

    /* ============ START: oc_rows_cache materializer (background) ============ */
    try {
      // Prefer an explicit FNO URI if supplied to avoid writing to wrong DB
      const mongoUri = process.env.MONGO_FNO_URI || process.env.MONGO_URI || "mongodb://localhost:27017";
      const ocUnderlying = Number(process.env.OC_UNDERLYING_ID || 13);
      const ocSegment = process.env.OC_SEGMENT || "IDX_I";

      console.log(`▶️ Starting oc_rows materializer using mongoUri=${mongoUri} db=${fnoDb.databaseName}`);

      ocRowsTimer = startOcRowsMaterializer({
        mongoUri,
        dbName: fnoDb.databaseName,
        underlyings: [{ id: ocUnderlying, segment: ocSegment }],
        intervals: [3, 5, 15, 30],
        sinceMs: Number(process.env.OC_ROWS_SINCE_MS || 12 * 60 * 60 * 1000), // first-pass window
        scheduleMs: Number(process.env.OC_ROWS_SCHEDULE_MS || 60_000),
        mode: "level",
        unit: "bps",
      });
      console.log(`✅ oc_rows_cache materializer started (underlying=${ocUnderlying}/${ocSegment})`);
    } catch (e) {
      console.warn("⚠️ oc_rows_cache materializer failed to start:", (e as any)?.message || e);
    }
    /* ============ END: oc_rows_cache materializer ============ */

    /* ================== Error handler (last) ================== */
    app.use((err: Error, req: Request, res: Response, next: NextFunction) => {
      errorMiddleware(err, req, res, next);
    });

    /* ================== Start HTTP server ================== */
    const PORT = Number(process.env.PORT_APP) || Number(process.env.PORT) || 8100;
    httpServer.listen(PORT, () => {
      console.log(`🚀 appnew.ts running at http://localhost:${PORT}`);
      console.log(`🔗 Allowed CORS origin: ${process.env.CLIENT_URL || "http://localhost:5173"}`);
    });
  } catch (err) {
    console.error("❌ appnew.ts startup error:", err);
    try {
      await closeAll();
    } catch {}
    process.exit(1);
  }
}

startServer();

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
    await closeAll();
  } catch (e) {
    console.warn("mongo close failed:", (e as any)?.message || e);
  }

  try {
    if (ocRowsTimer) {
      clearInterval(ocRowsTimer);
      ocRowsTimer = null;
      console.log("✅ oc_rows_cache materializer timer cleared");
    }
  } catch (e) {
    console.warn("failed clearing ocRowsTimer:", (e as any)?.message || e);
  }

  httpServer.close(() => {
    console.log("✅ Server closed");
    process.exit(code);
  });
}
process.on("SIGINT", () => shutdown(0));
process.on("SIGTERM", () => shutdown(0));

export { io };
