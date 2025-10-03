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
import otpRoutes from "./routes/otp.routes";
import { errorMiddleware } from "./middleware/error.middleware";
import { setDatabase } from "./controllers/auth.controller";

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

/* ---------- PUBLIC Orderbook bundle (summary/strategies) ---------- */
import { Orderbook } from "./api/orderbook";
import registerFeedbackRoutes from "./routes/registerFeedbackRoutes";

/* ------------ Vercel Analytics --------- */
// import { inject } from "@vercel/analytics";
// inject();

/* ------------ Invoice --------- */
import { registerInvoiceRoutes } from "./utils/invoice";

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

const securityIds = [
  37993, 37994, 37995, 37996, 37997, 37998, 37999, 38000, 38001, 38002, 38003, 38004,
  38005, 38006, 38007, 38008, 38009, 38010, 38011, 38012, 38016, 38017, 38019, 38020,
  38021, 38022, 38023, 38024, 38025, 38026, 38027, 38030, 38031, 38032, 38033, 38034,
  38035, 38036, 38037, 38038, 38039, 38040, 38041, 38042, 38043, 38044, 38045, 38046,
  38047, 38048, 38049, 38050, 38051, 38052, 38053, 38054, 38056, 38057, 38058, 38059,
  38063, 38064, 38065, 38066, 38067, 38068, 38069, 38070, 38071, 38072, 38073, 38074,
  38075, 38076, 38077, 38078, 38079, 38080, 38081, 38082, 38083, 38084, 38085, 38086,
  38087, 38088, 38089, 38090, 38093, 38094, 38095, 38097, 38099, 38100, 38101, 38102,
  38105, 38106, 38107, 38108, 38115, 38118, 38123, 38124, 38127, 38128, 38129, 38130,
  38131, 38132, 38133, 38134, 38135, 38136, 38137, 38138, 38139, 38140, 38141, 38142,
  38149, 38150, 38151, 38155, 38156, 38161, 38162, 38166, 38167, 38168, 38181, 38182,
  38183, 38184, 38185, 38186, 38187, 38188, 38189, 38190, 38201, 38202, 38203, 38204,
  38211, 38212, 38213, 38214, 38215, 38217, 38221, 38222, 38223, 38224, 38231, 38232,
  38233, 38234, 38235, 38236, 38237, 38238, 38241, 38242, 38243, 38244, 38247, 38248,
  38253, 38254, 38261, 38262, 38265, 38266, 38267, 38268, 38269, 38270, 38273, 38274,
  38275, 38276, 38279, 38280, 38281, 38282, 38283, 38284, 38285, 38286, 38287, 38288,
  38307, 38308, 38309, 38318, 38319, 38320, 38321, 38322, 38323, 38324, 38325, 38326,
  38341, 38342, 38347, 38348, 38351, 38352, 38357, 38358, 38359, 38360, 38361, 38362,
  38367, 38368, 38369, 38370, 38371, 38372, 38374, 38379, 38385, 38386, 38387, 38389,
  38393, 38394, 38395, 38396, 38397, 38398, 38399, 38400, 38401, 38402, 38403, 38404,
  38407, 38408, 38409, 38410, 38411, 38412, 38413, 38414, 38415, 38416, 38417, 38418,
  38419, 38420, 38421, 38422, 38424, 38429, 38430, 38431, 38433, 38434, 38437, 38438,
  38439, 38440, 38441, 38444, 38447, 38448, 38449, 38450, 38451, 38452, 38453, 38454,
  38455, 38456, 38457, 38458, 38459, 38460, 38461, 38462, 38463, 38464, 38465, 38466,
  38470, 38471, 38472, 38473, 38479, 38480, 38481, 38482, 38483, 38484, 38495, 38496,
  38497, 38498, 38501, 38502, 38503, 38504, 38505, 38506, 38507, 38512, 38514, 38521,
  38528, 38529, 38531, 38532, 38540, 38546, 38547, 38550, 38557, 38558, 38559, 38560,
  38563, 38564, 38565, 38566, 38571, 38572, 38573, 38574, 38575, 38576, 38577, 38579,
  38580, 38581, 38582, 38583, 38586, 38587, 38589, 38590, 38591, 38592, 38593, 38594,
  38595, 38596, 38598, 38599, 38600, 38601, 38602, 38604, 38605, 38606, 38611, 38612,
  38613, 38614, 38615, 38616, 38617, 38618, 38619, 38620, 38621, 38622, 38623, 38624,
  38625, 38626, 38627, 38628, 38629, 38630, 38631, 38632, 38633, 38634, 38635, 38636,
  38639, 38640, 38641, 38642, 38643, 38644, 38645, 38646, 38647, 38648, 38649, 38650,
  38651, 38652, 38653, 38655, 38656, 38659, 38660, 38661, 38662, 38664, 38665, 38666,
  38671, 38672, 38673, 38674, 38677, 38678, 38679, 38680, 38687, 38688, 38689, 38690,
  38693, 38694, 39499, 39506, 39507, 39512, 40065, 40066, 40067, 40068, 40550, 40595,
  40845, 40846, 42355, 42356, 42357, 42358, 43690, 43691, 43692, 43693, 44019, 44020,
  44021, 44022, 44785, 44786, 44789, 44790, 44793, 44794, 45048, 45051
];
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

    // Auth / OTP / catalog / payments
    app.use("/api/auth", authRoutes);
    app.use("/api/otp", otpRoutes);
    app.use("/api/products", productsRoutes);
    app.use("/api/payments", paymentRoutes);

    // Instruments & LTP
    app.use("/api/instruments", instrumentRouter);
    app.use("/api/ltp", ltpRoutes);

    // Contact & careers
    registerContactRoutes(app, db);
    app.use("/api/careers", registerCareersRoutes(db));
    app.use("/api/feedback", registerFeedbackRoutes(db));

    // PUBLIC Orderbook/summary/strategies endpoints (use X-User-Id or ?userId=)
    Orderbook(app, db);

    // Invoice
    registerInvoiceRoutes(app, "/api/invoice");

    // ---------- PUBLIC legacy alias ----------
    // Allow old client calls to /api/orders/triggered to work without JWT.
    // It forwards to the public /api/trades/list endpoint (same query + userId passthrough).
    app.get(
      "/api/orders/triggered",
      (req: Request, _res: Response, next: NextFunction) => {
        const origQs = new URLSearchParams(req.url.split("?")[1] || "");
        const from = origQs.get("from") || "";
        const to = origQs.get("to") || "";
        const userId = origQs.get("userId") || "";

        // Rebuild to /api/trades/list (public Orderbook route)
        const target =
          `/api/trades/list?from=${encodeURIComponent(from)}` +
          `&to=${encodeURIComponent(to)}` +
          (userId ? `&userId=${encodeURIComponent(userId)}` : "");

        // Mutate and continue through router stack so the mounted handler catches it
        req.url = target;
        next();
      }
    );

    // Misc analytics/data (keep these public if desired)
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

    /* ================== Auth gate: from here req.user is required ================== */
    app.use(authenticate);

    /* ---- Entitlement-protected namespaces ----
       DO NOT guard /api/summary here (the public one is already mounted). */
    app.use("/api/journal", requireEntitlement("journaling", "journaling_solo"));
    app.use("/api/daily-journal", requireEntitlement("journaling", "journaling_solo"));

    // Guard your paid datasets
    app.use("/api/fii", requireEntitlement("fii_dii_data"));
    app.use("/api/dii", requireEntitlement("fii_dii_data"));
    app.use("/api/pro", requireEntitlement("fii_dii_data"));
    app.use("/api/main-fii-dii", requireEntitlement("fii_dii_data"));

    /* ---- Authenticated routes ---- */
    app.use("/api", registerTradeJournalRoutes(db));
    app.use("/api/daily-journal", registerDailyJournalRoutes(db));
    app.use("/api/trade-calendar", registerTradeCalendarRoutes(db));
    app.use("/api/users", userRoutes);
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
