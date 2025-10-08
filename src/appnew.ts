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
  40072, 40073, 40074, 40075, 40847, 40848, 42359, 42360, 42361, 42362, 42363, 42364, 42365, 42366, 42367, 42368,
  42369, 42370, 42371, 42372, 42373, 42374, 42379, 42380, 42381, 42382, 42383, 42384, 42385, 42386, 42387, 42388,
  42389, 42390, 42391, 42392, 42393, 42394, 42395, 42396, 42397, 42398, 42399, 42400, 42401, 42402, 42405, 42406,
  42407, 42408, 42409, 42410, 42413, 42416, 42417, 42418, 42419, 42420, 42421, 42422, 42423, 42424, 42425, 42426,
  42427, 42428, 42429, 42430, 42431, 42432, 42433, 42434, 42439, 42442, 42443, 42444, 42445, 42449, 42450, 42452,
  42453, 42456, 42457, 42458, 42459, 42460, 42461, 42472, 42473, 42474, 42475, 42478, 42479, 42480, 42481, 42482,
  42483, 42484, 42485, 42486, 42487, 42488, 42489, 42490, 42491, 42492, 42495, 42496, 42497, 42498, 42499, 42500,
  42501, 42502, 42503, 42504, 42505, 42506, 42507, 42508, 42509, 42516, 42517, 42522, 42523, 42524, 42525, 42526,
  42527, 42528, 42529, 42530, 42531, 42532, 42533, 42534, 42535, 42536, 42537, 42538, 42539, 42540, 42541, 42542,
  42543, 42544, 42547, 42549, 42550, 42553, 42554, 42557, 42558, 42564, 42565, 42574, 42575, 42576, 42577, 42582,
  42583, 42584, 42585, 42586, 42587, 42588, 42589, 42590, 42591, 42594, 42595, 42596, 42597, 42600, 42601, 42602,
  42603, 42604, 42605, 42606, 42607, 42608, 42609, 42612, 42613, 42614, 42615, 42616, 42617, 42620, 42622, 42626,
  42627, 42628, 42629, 42630, 42631, 42632, 42633, 42640, 42641, 42644, 42645, 42646, 42647, 42648, 42649, 42650,
  42651, 42652, 42653, 42656, 42657, 42660, 42661, 42666, 42667, 42668, 42669, 42670, 42671, 42672, 42673, 42676,
  42677, 42678, 42679, 42684, 42685, 42686, 42687, 42690, 42691, 42692, 42693, 42694, 42695, 42702, 42703, 42704,
  42705, 42706, 42707, 42709, 42711, 42712, 42713, 42714, 42715, 42716, 42717, 42718, 42719, 42720, 42721, 42722,
  42723, 42724, 42725, 42730, 42731, 42732, 42733, 42734, 42735, 42738, 42739, 42740, 42741, 42742, 42743, 42744,
  42745, 42748, 42749, 42752, 42753, 42754, 42755, 42756, 42757, 42758, 42759, 42760, 42761, 42762, 42763, 42766,
  42767, 42768, 42769, 42770, 42771, 42772, 42773, 42774, 42775, 42776, 42777, 42780, 42781, 42782, 42783, 42784,
  42785, 42786, 42787, 42792, 42793, 42796, 42797, 42798, 42799, 42800, 42801, 42806, 42807, 42808, 42809, 42810,
  42811, 42816, 42817, 42820, 42821, 42822, 42823, 42824, 42825, 42826, 42827, 42828, 42829, 42830, 42831, 42832,
  42833, 42834, 42835, 42836, 42837, 42838, 42839, 42840, 42841, 42844, 42845, 42846, 42847, 42852, 42853, 42854,
  42855, 42856, 42857, 42860, 42861, 42864, 42865, 42866, 42867, 42868, 42869, 42870, 42871, 42874, 42875, 42876,
  42877, 42880, 42881, 42888, 42889, 42890, 42891, 42892, 42893, 42902, 42903, 42904, 42905, 42906, 42907, 42908,
  42909, 42910, 42911, 42912, 42913, 42918, 42919, 42922, 42923, 42926, 42927, 42932, 42933, 42934, 42935, 42936,
  42937, 42938, 42939, 42940, 42941, 42948, 42949, 42950, 42951, 42952, 42953, 42954, 42955, 42956, 42957, 42958,
  42959, 42960, 42961, 42962, 42963, 42964, 42965, 42966, 42967, 42968, 42969, 42970, 43694, 43695, 43696, 43697,
  44028, 44029, 44043, 44048, 44797, 44798, 44799, 44804, 44805, 44806, 45052, 45053, 47991, 47992, 47993, 47994,
  47995, 47996, 47997, 47998, 64615, 64616
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