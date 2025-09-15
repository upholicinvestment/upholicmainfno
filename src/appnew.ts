// server/src/appnew.ts
import express, { Request, Response, NextFunction } from "express";
import cors from "cors";
import dotenv from "dotenv";
import { MongoClient, Db } from "mongodb";
import { createServer } from "http";
import { Server as SocketIOServer } from "socket.io";

/* ---------- Market / data sockets & services ---------- */
import { DhanSocket } from "./socket/dhan.socket";
import { ltpRoutes } from "./routes/ltp.route";
import { setDatabase as setLtpDatabase } from "./services/ltp.service";
import {
  fetchMarketQuote,
  saveMarketQuote,
  fetchAndStoreInstruments,
  setDatabase as setQuoteDatabase,
} from "./services/quote.service";

/* ---------- Core routes & middlewares ---------- */
import routes from "./routes";
import authRoutes from "./routes/auth.routes";
import otpRoutes from "./routes/otp.routes";              // <-- mount PUBLIC
import { errorMiddleware } from "./middleware/error.middleware";
import { setDatabase } from "./controllers/auth.controller";

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
import userRoutes from "./routes/user.routes";

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

dotenv.config();

const app = express();
const httpServer = createServer(app);

/* ================== CORS + Body Parsing ================== */
app.use(
  cors({
    origin: process.env.CLIENT_URL || "http://localhost:5173",
    credentials: true,
  })
);
app.use(express.json());

/* ================== Global DB refs ================== */
let db: Db;
let mongoClient: MongoClient;

/* ================== Market helpers ================== */
function getISTDate(): Date {
  return new Date();
}
function isMarketOpen(): boolean {
  const now = getISTDate();
  const day = now.getDay(); // 0=Sun,6=Sat
  if (day === 0 || day === 6) return false;
  const totalMinutes = now.getHours() * 60 + now.getMinutes();
  return totalMinutes >= 9 * 60 + 15 && totalMinutes <= 15 * 60 + 30;
}
const securityIds = [35056, 35057, 35065, 35066, 35107, 35108, 35109, 35110, 35111, 35112, 35119, 35120];
const QUOTE_BATCH_SIZE = 1000;
const QUOTE_INTERVAL = 2500;

async function startMarketQuotePolling() {
  console.log("🚀 Starting Market Quote Polling...");
  let currentIndex = 0;
  setInterval(async () => {
    if (!isMarketOpen()) {
      console.log("⏳ Market closed. Skipping Market Quote Polling.");
      return;
    }
    try {
      const batch = securityIds.slice(currentIndex, currentIndex + QUOTE_BATCH_SIZE);
      if (batch.length > 0) {
        const data = await fetchMarketQuote(batch);
        await saveMarketQuote(data);
      }
      currentIndex += QUOTE_BATCH_SIZE;
      if (currentIndex >= securityIds.length) currentIndex = 0;
    } catch (err: any) {
      if (err.response?.status === 429) {
        console.warn("⚠ Rate limit hit (429). Skipping this cycle.");
      } else {
        console.error("❌ Error in Market Quote Polling:", err);
      }
    }
  }, QUOTE_INTERVAL);
}

/* ================== Dhan WebSocket (LTP) ================== */
const dhanSocket = new DhanSocket(
  process.env.DHAN_API_KEY!,
  process.env.DHAN_CLIENT_ID!
);
if (isMarketOpen()) {
  dhanSocket.connect(securityIds);
} else {
  console.log("⏳ Market is closed. Skipping WebSocket connection.");
}

/* ================== DB connect + server start ================== */
const connectDB = async () => {
  try {
    if (!process.env.MONGO_URI || !process.env.MONGO_DB_NAME) {
      throw new Error("❌ Missing MongoDB URI or DB Name in .env");
    }

    mongoClient = new MongoClient(process.env.MONGO_URI);
    await mongoClient.connect();
    db = mongoClient.db(process.env.MONGO_DB_NAME);
    console.log("✅ Connected to MongoDB");

    /* ---- Inject DB into modules ---- */
    setDatabase(db);
    setAuthDb(db);
    setLtpDatabase(db);
    setQuoteDatabase(db);
    setProductsDb(db);
    setPaymentDatabase(db);
    setUserDatabase(db);
    setRequireEntitlementDb(db);

    // Ensure journal/calendar indexes exist (idempotent)
    await ensureCalendarIndexes(db).catch(() => {});

    /* ================== PUBLIC routes (no JWT required) ================== */
    app.use("/api/auth", authRoutes);         // login/register/refresh etc.
    app.use("/api/otp", otpRoutes);           // <-- PUBLIC OTP endpoints
    app.use("/api/products", productsRoutes); // <-- PUBLIC catalog fetch for Register page
    app.use("/api/payments", paymentRoutes);  // <-- supports guest signupIntent create-order
    app.use("/api/instruments", instrumentRouter);
    app.use("/api/ltp", ltpRoutes);
    registerContactRoutes(app, db);

    // Misc public analytics/data routes
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

    /* ================== Auth gate: from here req.user is populated ================== */
    app.use(authenticate);

    /* ---- Entitlement-protected namespaces ---- */
    app.use("/api/journal", requireEntitlement("journaling", "journaling_solo"));
    app.use("/api/daily-journal", requireEntitlement("journaling", "journaling_solo"));

    app.use("/api/fii", requireEntitlement("fii_dii_data"));
    app.use("/api/dii", requireEntitlement("fii_dii_data"));
    app.use("/api/pro", requireEntitlement("fii_dii_data"));
    app.use("/api/summary", requireEntitlement("fii_dii_data"));
    app.use("/api/main-fii-dii", requireEntitlement("fii_dii_data"));

    /* ---- Authenticated routes (need req.user) ---- */
    app.use("/api", registerTradeJournalRoutes(db));             // /upload-orderbook, /stats
    app.use("/api/daily-journal", registerDailyJournalRoutes(db));
    app.use("/api/trade-calendar", registerTradeCalendarRoutes(db)); // calendar endpoints
    app.use("/api/users", userRoutes);

    // Central router (keep after authenticate if it relies on req.user)
    app.use("/api", routes);

    /* ================== Data boot + schedulers ================== */
    await fetchAndStoreInstruments();
    startMarketQuotePolling();

    /* ================== Error handler ================== */
    app.use((err: Error, req: Request, res: Response, next: NextFunction) => {
      errorMiddleware(err, req, res, next);
    });

    /* ================== Start HTTP + Socket.IO ================== */
    const PORT = Number(process.env.PORT) || 8000;
    httpServer.listen(PORT, () => {
      console.log(`🚀 Server running at http://localhost:${PORT}`);
      console.log(
        `🔗 Allowed CORS origin: ${process.env.CLIENT_URL || "http://localhost:5173"}`
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
process.on("SIGINT", async () => {
  console.log("🛑 Shutting down gracefully...");
  try {
    await mongoClient?.close();
  } catch {}
  httpServer.close(() => {
    console.log("✅ Server closed");
    process.exit(0);
  });
});

export { io };
