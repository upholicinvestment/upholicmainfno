import express, { Request, Response, NextFunction } from "express";
import cors from "cors";
import dotenv from "dotenv";
import { MongoClient, Db } from "mongodb";
import { createServer } from "http";
import { Server as SocketIOServer } from "socket.io";

import { DhanSocket } from "./socket/dhan.socket";
import { ltpRoutes } from "./routes/ltp.route";
import { setDatabase as setLtpDatabase } from "./services/ltp.service";
import {
  fetchMarketQuote,
  saveMarketQuote,
  fetchAndStoreInstruments,
  setDatabase as setQuoteDatabase,
} from "./services/quote.service";

import routes from "./routes";
import authRoutes from "./routes/auth.routes";
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

import productsRoutes, { setProductsDb } from "./routes/products.routes";

import paymentRoutes from "./routes/payment.routes";
import { setPaymentDatabase } from "./controllers/payment.controller";


import tradeJournalRoutes from './routes/tradeJournal.routes';
import registerDailyJournalRoutes from './routes/dailyJournal.routes';
import instrumentRouter from "./routes/instruments"; // Path as needed
import registerTradeJournalRoutes from "./routes/tradeJournal.routes";

import registerContactRoutes from "./api/contact";
import { setUserDatabase } from "./controllers/user.controller";
import userRoutes from "./routes/user.routes";

import registerCareersRoutes from "./routes/Careers.routes";



dotenv.config();

const app = express();
const httpServer = createServer(app);

// CORS + Body parsing
app.use(
  cors({
    origin: process.env.CLIENT_URL || "http://localhost:5173",
    credentials: true,
  })
);
app.use(express.json());

// Global DB variables
let db: Db;
let mongoClient: MongoClient;

// -------------- websocket dhan data -----------------------
/*** Get IST Date (UTC +5:30) */
function getISTDate(): Date {
  return new Date(); // Server is already in IST
}

/*** Check if market is open (Mon–Fri, 09:15–15:30 IST) */
function isMarketOpen(): boolean {
  const now = getISTDate();

  // 0 = Sunday, 6 = Saturday
  const day = now.getDay();
  if (day === 0 || day === 6) return false; // Weekend closed

  const hours = now.getHours();
  const minutes = now.getMinutes();
  const totalMinutes = hours * 60 + minutes;

  // Regular session window
  return totalMinutes >= 9 * 60 + 15 && totalMinutes <= 15 * 60 + 30;
}

// Market Quote Polling (with rate limit handling) //
const securityIds = [
  35052, 35053, 35054, 35055, 35107, 35108, 35109, 35110, 35111, 35112, 35113,
  35114, 35179, 35183, 35189, 35190, 37102, 37116, 38340, 38341, 42509, 42510
]; // Add your full NSE_FNO instrument list here
const QUOTE_BATCH_SIZE = 1000;
const QUOTE_INTERVAL = 2500; // 1.2 seconds (slightly above 1s to avoid 429)

async function startMarketQuotePolling() {
  // console.log("🚀 Starting Market Quote Polling...");
  let currentIndex = 0;

  setInterval(async () => {
    if (!isMarketOpen()) {
      // console.log("⏳ Market closed. Skipping Market Quote Polling.");
      return;
    }

    try {
      const batch = securityIds.slice(
        currentIndex,
        currentIndex + QUOTE_BATCH_SIZE
      );
      if (batch.length > 0) {
        const data = await fetchMarketQuote(batch);
        await saveMarketQuote(data);
      }

      currentIndex += QUOTE_BATCH_SIZE;
      if (currentIndex >= securityIds.length) currentIndex = 0;
    } catch (err: any) {
      if (err.response?.status === 429) {
        console.warn(
          "⚠ Rate limit hit (429). Skipping this cycle to avoid being blocked."
        );
      } else {
        console.error("❌ Error in Market Quote Polling:", err);
      }
    }
  }, QUOTE_INTERVAL);
}

// WebSocket for LTP
const dhanSocket = new DhanSocket(
  process.env.DHAN_API_KEY!,
  process.env.DHAN_CLIENT_ID!
);

// Connect WebSocket only during market hours
if (isMarketOpen()) {
  dhanSocket.connect(securityIds);
} else {
  // console.log("⏳ Market is closed. Skipping WebSocket connection.");
}

// --------------------------------------------------------

// Connect to MongoDB and start server
const connectDB = async () => {
  try {
    if (!process.env.MONGO_URI || !process.env.MONGO_DB_NAME) {
      throw new Error("❌ Missing MongoDB URI or DB Name in .env");
    }

    mongoClient = new MongoClient(process.env.MONGO_URI);
    await mongoClient.connect();
    db = mongoClient.db(process.env.MONGO_DB_NAME);
    // console.log("✅ Connected to MongoDB");

    // Inject DB into controllers
    setDatabase(db);
    setLtpDatabase(db);
    setQuoteDatabase(db);
    setProductsDb(db);

    // Inject DB into all routes that need it
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

    registerContactRoutes(app, db);
    setPaymentDatabase(db);
    setUserDatabase(db);

    // mount specific routers first
    app.use("/api/auth", authRoutes);
    app.use("/api/payments", paymentRoutes);
    app.use("/api/products", productsRoutes);
    app.use("/api/ltp", ltpRoutes);

    // then mount the central router
    app.use("/api", routes);
    app.use("/api/instruments", instrumentRouter);
    app.use("/api", registerTradeJournalRoutes(db));
    app.use('/api/daily-journal', registerDailyJournalRoutes(db));
    app.use("/api/users", userRoutes);
    app.use("/api/careers", registerCareersRoutes(db));


    

    // Start Market Quote Polling
    await fetchAndStoreInstruments();
    startMarketQuotePolling();

    // Error handler
    app.use((err: Error, req: Request, res: Response, next: NextFunction) => {
      errorMiddleware(err, req, res, next);
    });

    // Start HTTP + WebSocket server
    const PORT = Number(process.env.PORT) || 8000;
    httpServer.listen(PORT, () => {
      // console.log(`🚀 Server running at http://localhost:${PORT}`);
      // console.log(
      //   `🔗 Allowed CORS origin: ${
      //     process.env.CLIENT_URL || "http://localhost:5173"
      //   }`
      // );
    });
  } catch (err) {
    console.error("❌ MongoDB connection error:", err);
    process.exit(1);
  }
};

connectDB();

// Setup Socket.IO
const io = new SocketIOServer(httpServer, {
  cors: {
    origin: process.env.CLIENT_URL || "http://localhost:5173",
    methods: ["GET", "POST"],
  },
  // connectionStateRecovery: {
  //   maxDisconnectionDuration: 2 * 60 * 1000,
  //   skipMiddlewares: true,
  // },
});

io.on("connection", (socket) => {
  // console.log("🔌 New client connected:", socket.id);
  socket.on("disconnect", (reason) =>
    console.log(`Client disconnected (${socket.id}):`, reason)
);
  // socket.on("error", (err) => console.error("Socket error:", err));
});

// Graceful shutdown
process.on("SIGINT", async () => {
  // console.log("🛑 Shutting down gracefully...");
  await mongoClient.close();
  httpServer.close(() => {
    // console.log("✅ Server closed");
    process.exit(0);
  });
});

export { io };