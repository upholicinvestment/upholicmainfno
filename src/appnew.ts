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
import { requireEntitlement, setRequireEntitlementDb } from "./middleware/requireEntitlement.middleware";



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

// Updated NSE_FNO instrument list
const securityIds = [
  35056, 35057, 35065, 35066, 35107, 35108, 35109, 35110, 35111, 35112, 35119, 35120,
  35121, 35122, 35123, 35124, 35208, 35209, 35213, 35214, 35269, 35277, 35593, 35596,
  35603, 35607, 35824, 35827, 35828, 35829, 35830, 35839, 35840, 35841, 35842, 35843,
  38510, 38511, 39528, 39529, 39572, 39573, 39574, 39575, 39576, 39577, 39578, 39579,
  39658, 39659, 39660, 39663, 39664, 39665, 40489, 40490, 43996, 43997, 44002, 44003,
  44004, 44005, 44008, 44009, 44019, 44020, 44021, 44022, 44028, 44029, 44043, 44048,
  44050, 44051, 44052, 44053, 44060, 44061, 44062, 44063, 44074, 44075, 44078, 44079,
  44080, 44081, 44082, 44083, 44088, 44089, 44090, 44091, 44094, 44095, 44096, 44097,
  44098, 44099, 44100, 44101, 44104, 44105, 44106, 44107, 44110, 44111, 44112, 44113,
  44116, 44117, 44118, 44119, 44120, 44121, 44122, 44123, 44128, 44129, 44130, 44131,
  44132, 44133, 44136, 44137, 44146, 44147, 44148, 44149, 44160, 44161, 44162, 44163,
  44164, 44165, 44174, 44175, 44176, 44177, 44182, 44183, 44184, 44185, 44194, 44195,
  44205, 44206, 44207, 44208, 44210, 44211, 44212, 44213, 44235, 44236, 44255, 44256,
  44272, 44273, 44274, 44275, 44288, 44293, 44297, 44300, 44313, 44314, 44319, 44320,
  44324, 44325, 44328, 44329, 44338, 44339, 44340, 44341, 44342, 44345, 44346, 44348,
  44351, 44352, 44370, 44371, 44372, 44373, 44374, 44375, 44379, 44380, 44384, 44385,
  44392, 44393, 44394, 44395, 44398, 44399, 44412, 44413, 44414, 44415, 44416, 44417,
  44418, 44419, 44422, 44423, 44428, 44429, 44436, 44437, 44438, 44439, 44440, 44441,
  44442, 44443, 44446, 44447, 44448, 44449, 44466, 44467, 44474, 44475, 44476, 44477,
  44478, 44479, 44480, 44481, 44482, 44483, 44484, 44485, 44502, 44503, 44516, 44517,
  44520, 44521, 44524, 44525, 44526, 44527, 44532, 44533, 44534, 44535, 44542, 44543,
  44544, 44545, 44546, 44547, 44548, 44549, 44574, 44575, 44580, 44581, 44582, 44583,
  44588, 44589, 44602, 44603, 44604, 44631, 44632, 44637, 44638, 44639, 44640, 44651,
  44652, 44661, 44662, 44667, 44668, 44687, 44688, 44706, 44713, 44727, 44728, 44729,
  44730, 44741, 44742, 44743, 44744, 44763, 44764, 44795, 44796, 44812, 44813, 44823,
  44824, 44827, 44828, 44829, 44830, 44831, 44832, 44833, 44834, 44835, 44836, 44837,
  44840, 44841, 44842, 44845, 44846, 44847, 44848, 44853, 44854, 44855, 44856, 44857,
  44858, 44864, 44865, 44867, 44868, 44869, 44870, 44881, 44882, 44885, 44886, 44887,
  44888, 44890, 44891, 44897, 44898, 44900, 44901, 44903, 44904, 44906, 44907, 44909,
  44912, 44913, 44914, 44917, 44918, 44919, 44920, 44922, 44923, 44925, 44926, 44928,
  44929, 44932, 44933, 44935, 44936, 44937, 44940, 44941, 44942, 44943, 44944, 44945,
  44946, 44953, 44954, 44955, 44958, 44963, 44964, 44965, 44966, 44971, 44972, 44975,
  44976, 44979, 44980, 44982, 44983, 44985, 44986, 44987, 44988, 44989, 44990, 44991,
  44992, 44993, 44994, 44999, 45000, 45001, 45002, 45003, 45004, 45005, 45006, 45009,
  45010, 45013, 45014, 45015, 45016, 45019, 45020, 45021, 45022, 45023, 45024, 45025,
  45026, 45027, 45028, 45031, 45032, 45035, 45036, 45037, 45038, 45039, 45040, 45043,
  45044, 45047, 45048, 45051, 45052, 45053, 45054, 45055, 45056, 45057, 45058, 45061,
  45062, 45063, 45064, 45070, 45071, 45072, 45073, 45074, 45075, 45076, 45077, 45078,
  45079, 45080, 45081, 45082, 45083, 45084, 45085, 45086, 45087, 45088, 45089, 45090,
  45091, 45092, 45093, 45096, 45098, 45099, 45100, 45102, 45103, 45108, 45109, 45111,
  45403, 45408, 45447, 45448, 46012, 46013, 46014, 46015, 46496, 46499, 46883, 46884,
  46885, 46886, 46887, 46888, 47003, 47004, 47177, 47180, 47185, 47186, 47187, 47200,
  47201, 47202, 47203, 47204, 47211, 47213, 47214, 47218, 47219, 47220, 47223, 47224,
  47225, 47226, 47227, 47228, 47229, 47230, 47233, 47234, 47235, 47236, 47237, 47238,
  47239, 47240, 47241, 47242, 47250, 47251, 47278, 47282, 47285, 47286, 47289, 47292,
  47293, 47294, 47328, 47329, 47330, 47331, 47333, 47334, 47335, 47336, 47337, 47341,
  47342, 47348, 47349, 47350, 47351, 47352, 47353, 47354, 47357, 47358, 47359, 47360,
  47361, 47364, 47365, 47366, 47367, 47368, 47369, 47370, 47371, 47374, 47375, 47376,
  47377, 47378, 47379, 47380, 47381, 47382, 47383, 47384, 47385, 47389, 47392, 47393,
  47396, 47397, 47402, 47403, 47408, 47409, 47415, 47416, 47420, 47421, 47422, 47423,
  47443, 47447, 47452, 47453, 47454, 47455, 47456, 47458, 47459, 47460, 47461, 47462,
  47463, 47465, 47466, 47467, 47468, 47469, 47472, 47473, 47474, 47475, 47476, 47478,
  47483, 47489, 47490, 47491, 47492, 47493, 47494, 47495, 47500, 47504, 47507, 47509,
  47516, 47517, 47518, 47519, 47520, 47522, 47523, 47525, 47528, 47529, 47530, 47531,
  47532, 47533, 47534, 47535, 47536, 47537, 47545, 47546, 47548, 47549, 47550, 47551,
  47554, 47555, 47556, 47557, 47558, 47559, 47560, 47561, 47566, 47567, 47568, 47569,
  47570, 47571, 47572, 47573, 47578, 47579, 47580, 47581, 47586, 47589, 47592, 47595,
  47598, 47599, 47606, 47607, 47608, 47609, 47610, 47611, 47620, 47621, 47622, 47623,
  47649, 47650, 47685, 47686, 47695, 47696, 47697, 47700, 47703, 47704, 47711, 47712,
  47717, 47718, 47723, 47724, 47733, 47734, 47751, 47752, 47753, 47754, 47755, 47756,
  47757, 47758, 47759, 47760, 47761, 47762, 47763, 47764, 47765, 47766, 47773, 47774,
  47777, 47778, 47779, 47780, 47781, 47782, 47787, 47788, 47793, 47794, 47797, 47798,
  47803, 47804, 47805, 47812, 47813, 47814, 47815, 47816, 47821, 47822, 47833, 47834,
  47836, 47837, 47839, 47840, 47841, 47842, 47843, 47844, 47849, 47850, 47851, 47852,
  47853, 47854, 47855, 47856, 47857, 47858, 47859, 47860, 47866, 47867, 47869, 47870,
  47871, 47872, 47874, 47875, 47879, 47880, 47881, 47882, 47883, 47884, 47885, 47886,
  47887, 47888, 47889, 47890, 47891, 47898, 47899, 47900, 47901, 47902, 47903, 47904,
  47905, 47907, 47908, 47909, 47911, 47912, 47915, 47916, 47917, 47918, 47919, 47920,
  47921, 47922, 47923, 47924, 47929, 47930, 47935, 47936, 47937, 47938, 47939, 47940,
  47941, 47942, 47943, 47944, 47945, 47946, 47947, 47948, 47949, 47950, 47953, 47954,
  47955, 47956, 47959, 47960, 47965, 47966, 47967, 47970, 47971, 47972, 47975, 47976,
  47977, 47978, 47979, 47980, 47983, 47984, 47987, 47988, 47989, 47990, 47991, 47992,
  47993, 47994, 47995, 47996, 47997, 47998, 48001, 48004, 48009, 48012, 48023, 48024,
  48025, 48027, 48029, 48030, 48033, 48034, 48038, 48041, 48042, 48044, 48045, 48046,
  48047, 48048, 48049, 48050, 48051, 48056, 48057, 48062, 48063, 48064, 48065, 48066,
  48067, 48068, 48069, 48070, 48071, 48072, 48073, 48074, 48075, 48076, 48077, 48078,
  48079, 48080, 48081, 48082, 48085, 48086, 48087, 48088, 48089, 48090, 48091, 48094,
  48095, 48096, 48097, 48100, 48103, 48104, 49173, 49175, 49176, 49179, 50732, 50733,
  50734, 50735, 50912, 50913, 52075, 52076, 52077, 52078, 52079, 52080, 52081, 52082,
  52083, 52084, 52085, 52086, 52087, 52088, 53001, 57096, 57097, 60133, 60134, 60135,
  60136, 60137, 60138, 60139, 60140, 60143, 60144, 60145, 60146, 60149, 60150, 60151,
  60152, 60153, 60154, 60155, 60156, 60157, 60158, 60159, 60160, 60161, 60162, 60171,
  60172, 60173, 60174, 60175, 60176, 60177, 60178, 60185, 60186, 60187, 60189, 60190,
  60191, 60192, 60195, 60196, 60202, 60203, 60204, 60205, 60206, 60219, 60220, 60221,
  60222, 60223, 60226, 60227, 60231, 60232, 60235, 60236, 60259, 60260, 60261, 60264,
  60266, 60271, 60274, 60275, 60276, 60277, 60278, 60279, 60280, 60281, 60282, 60283,
  60286, 60287, 60290, 60291, 60292, 60293, 60294, 60295, 60296, 60297, 60298, 60299,
  60300, 60301, 60302, 60303, 60304, 60305, 60306, 60307, 60308, 60309, 60310, 60311,
  60312, 60313, 60314, 60315, 60316, 60317, 60318, 60319, 60320, 60321, 60322, 60323,
  60324, 60325, 60326, 60327, 60328, 60331, 60332, 60333, 60334, 60335, 60336, 60337,
  60338, 60339, 60340, 60341, 60342, 60345, 60346, 60347, 60348, 60350, 60351, 60353,
  60356, 60357, 60358, 60359, 60362, 60363, 60372, 60373, 60374, 60377, 60381, 60382,
  60383, 60384, 60390, 60391, 60392, 60398, 60401, 60402, 60406, 60427, 60440, 60441,
  60442, 60443, 60450, 60451, 60452, 60453, 60454, 60455, 60456, 60457, 60460, 60461,
  60464, 60465, 60466, 60467, 60468, 60469, 60470, 60474, 60475, 60481, 60482, 60483,
  60484, 60485, 60486, 60487, 60488, 60489, 60490, 60491, 60492, 60493, 60494, 60495,
  60496, 60497, 60498, 60499, 60500, 60501, 60502, 60503, 60504, 60505, 60506, 60508,
  60509, 60510, 60511, 60512, 60513, 60514, 60515, 60516, 60517, 60518, 60547, 60548,
  60551, 60552, 60553, 60554, 60565, 60566, 60573, 60574, 60575, 60576, 60582, 60583,
  60584, 60585, 60586, 60587, 60588, 60589, 60590, 60591, 60592, 60593, 60594, 60595,
  60596, 60597, 60598, 60599, 60600, 60601, 60602, 60603, 60604, 60605, 60606, 60607,
  60608, 60609, 60610, 60611, 60612, 60613, 60614, 60615, 60618, 60619, 60620, 60621,
  60622, 60623, 60624, 60625, 60626, 60627, 60628, 60629, 60630, 60631, 60632, 60633,
  60634, 60635, 60636, 60637, 60638, 60639, 60640, 60641, 60642, 60643, 60644, 60645,
  60646, 60647, 60648, 60649, 60650, 60651, 60652, 60653, 60654, 60655, 60656, 60657,
  60658, 60659, 60660, 60661, 60666, 60667, 60728, 60729, 60730, 60731, 60732, 60733,
  60734, 60735, 60736, 60737, 60738, 60739, 60740, 60741, 60742, 60743, 60744, 60745,
  60746, 60747, 60748, 60749, 60751, 60752, 60753, 60754, 60755, 60756, 60757, 60758,
  60759, 60760, 60761, 60762, 60763, 60764, 60765, 60766, 60767, 60768, 60769, 60776,
  60779, 60780, 60783, 60784, 60785, 60786, 60789, 60790, 60791, 60792, 60793, 60794,
  60795, 60796, 60805, 60807, 60808, 60809, 60810, 60811, 60812, 60813, 60814, 60815,
  60816, 60817, 60818, 60819, 60820, 60821, 60822, 60823, 60824, 60825, 60826, 60827,
  60828, 60829, 60830, 60831, 60832, 60833, 60834, 60835, 60836, 60837, 60838, 60839,
  60840, 60841, 60842, 64666, 64667, 64668, 64669, 64670, 64671, 64672, 64673, 64674,
  64675, 64676, 64677, 64678, 64679, 64680, 64681, 64682, 64683, 64684, 64685, 64686,
  64687, 64688, 64689, 64690, 64691, 64692, 64693, 64694, 64695, 64696, 64697, 64698,
  64699, 64700, 64701, 64702, 64703, 64704, 64705, 64706, 64707, 64708, 64709, 64710,
  64711, 64737, 64738, 64739, 64754, 64755, 64756, 64763, 64764, 64768, 64769, 64770,
  64771
];// Add your full NSE_FNO instrument list here

const QUOTE_BATCH_SIZE = 1000;
const QUOTE_INTERVAL = 2500; // 1.2 seconds (slightly above 1s to avoid 429)

async function startMarketQuotePolling() {
  console.log("🚀 Starting Market Quote Polling...");
  let currentIndex = 0;

  setInterval(async () => {
    if (!isMarketOpen()) {
      console.log("⏳ Market closed. Skipping Market Quote Polling.");
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
  console.log("⏳ Market is closed. Skipping WebSocket connection.");
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
    console.log("✅ Connected to MongoDB");

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
    setRequireEntitlementDb(db);

    // ----------------- Protect paid data APIs FIRST -----------------
    // Journaling APIs (adjust paths to what your routers actually use)
    app.use("/api/journal", requireEntitlement("journaling", "journaling_solo"));
    app.use("/api/daily-journal", requireEntitlement("journaling", "journaling_solo"));

    // FII/DII related APIs (guard all namespaces that feed /main-fii-dii page)
    app.use("/api/fii", requireEntitlement("fii_dii_data"));
    app.use("/api/dii", requireEntitlement("fii_dii_data"));
    app.use("/api/pro", requireEntitlement("fii_dii_data"));
    app.use("/api/summary", requireEntitlement("fii_dii_data"));
    app.use("/api/main-fii-dii", requireEntitlement("fii_dii_data"));
    // If some of your FII/DII endpoints live on other prefixes, add them here too.

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
  console.log("🔌 New client connected:", socket.id);
  socket.on("disconnect", (reason) =>
    console.log(`Client disconnected (${socket.id}):`, reason)
);
  // socket.on("error", (err) => console.error("Socket error:", err));
});

// Graceful shutdown
process.on("SIGINT", async () => {
  console.log("🛑 Shutting down gracefully...");
  await mongoClient.close();
  httpServer.close(() => {
    console.log("✅ Server closed");
    process.exit(0);
  });
});

export { io };