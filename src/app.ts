import express, { Request, Response, NextFunction } from 'express';
import { MongoClient, Db } from 'mongodb';
import cors from 'cors';
import dotenv from 'dotenv';
import routes from './routes';
import { errorMiddleware } from './middleware/error.middleware';
import { createServer } from 'http';
import { Server as SocketIOServer } from 'socket.io';
const dayjs = require("dayjs");
require("dotenv").config();

// Load environment variables
dotenv.config();

// Initialize Express app
const app = express();
const httpServer = createServer(app);

// Middleware
app.use(cors({
  origin: process.env.CLIENT_URL || 'http://localhost:5173',
  credentials: true
}));
app.use(express.json());

// Setup MongoDB
let db: Db;
let mongoClient: MongoClient;

const connectDB = async () => {
  try {
    if (!process.env.MONGO_URI || !process.env.MONGO_DB_NAME) {
      console.error('Missing MongoDB configuration in .env');
      process.exit(1);
    }

    mongoClient = new MongoClient(process.env.MONGO_URI);
    await mongoClient.connect();
    db = mongoClient.db(process.env.MONGO_DB_NAME);
    console.log('✅ Connected to MongoDB');
  } catch (err) {
    console.error('❌ MongoDB connection error:', err);
    process.exit(1);
  }
};

// Connect to DB before starting server
connectDB();



// Routes
app.use('/api', routes);

// ✅ API: Fetch selected stocks with LTP and volume
// ✅ API: Fetch selected stocks with LTP and volume
app.get('/api/stocks', async (_req, res) => {
  try {
    const securityIds = [
      3499, 4306, 10604, 1363, 13538, 11723, 5097, 25, 2475, 1594, 2031,
      16669, 1964, 11483, 1232, 7229, 2885, 16675, 11536, 10999, 18143, 3432,
      3506, 467, 910, 3787, 15083, 21808, 1660, 3045, 157, 881, 4963, 383, 317,
      11532, 11630, 3351, 14977, 1922, 5258, 5900, 17963, 1394, 1333, 1348, 694,
      236, 3456
    ];

    const stocks = await db.collection('nse_equity')
      .find({ security_id: { $in: securityIds } })
      .project({
        _id: 0,
        security_id: 1,
        LTP: 1,
        volume: 1,
        open: 1,   // ADD THIS LINE
        close: 1
      })
      .toArray();

    res.json(stocks);
  } catch (err) {
    console.error('Error fetching stocks:', err);
    res.status(500).json({ error: 'Failed to fetch stocks' });
  }
});

app.get('/api/advdec', async (req: Request, res: Response): Promise<void> => {
  try {
    if (!db) throw new Error('Database not connected');
    const now = new Date();
    const marketOpen = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 9, 15, 0, 0);
    const marketClose = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 16, 30, 59, 999);

    // Helper: last completed slot time as string "HH:mm"
    function getLastCompletedSlot(dt: Date) {
      let h = dt.getHours(), m = dt.getMinutes();
      let slotM = m - (m % 5);
      if (h < 9 || (h === 9 && slotM < 15)) return "09:15";
      if (h > 15 || (h === 15 && slotM > 30)) return "15:30";
      return `${h.toString().padStart(2, '0')}:${slotM.toString().padStart(2, '0')}`;
    }

    // Helper: only valid market slot "HH:mm"
    function isMarketSlot(h: number, m: number) {
      if (h < 9 || (h === 9 && m < 15)) return false;
      if (h > 15 || (h === 15 && m > 30)) return false;
      return m % 5 === 0;
    }

    // Fetch all of today's records (between open and close)
    const records = await db.collection('nse_equity')
      .find({ timestamp: { $gte: marketOpen, $lte: marketClose } })
      .sort({ timestamp: 1 })
      .toArray();

    // Group records by exact 5-min slot time: "HH:mm"
    const grouped: Record<string, any[]> = {};
    for (const doc of records) {
      const dt = new Date(doc.timestamp);
      const h = dt.getHours(), m = dt.getMinutes();
      // Only include records at exact 5-min marks (09:15, 09:20, ...)
      if (!isMarketSlot(h, m)) continue;
      const slot = `${h.toString().padStart(2, '0')}:${m.toString().padStart(2, '0')}`;
      if (!grouped[slot]) grouped[slot] = [];
      grouped[slot].push(doc);
    }

    // Get the last completed slot (do NOT include in-progress slot!)
    const lastSlot = getLastCompletedSlot(now);

    // Build chartData array up to the last completed slot (inclusive)
    const chartData = Object.entries(grouped)
      .filter(([time]) => time <= lastSlot)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([time, group]) => {
        let advances = 0, declines = 0;
        for (const stock of group) {
          const ltp = parseFloat(stock.LTP);
          const close = parseFloat(stock.close);
          if (ltp > close) advances++;
          else if (ltp < close) declines++;
        }
        return { time, advances, declines };
      });

    const latest = chartData.at(-1);
    const current = {
      advances: latest?.advances ?? 0,
      declines: latest?.declines ?? 0,
      total: (latest?.advances ?? 0) + (latest?.declines ?? 0)
    };

    res.json({ current, chartData });
  } catch (err) {
    console.error('Error in /api/advdec:', err);
    res.status(500).json({
      error: 'Internal Server Error',
      message: err instanceof Error ? err.message : 'Unknown error'
    });
  }
});


app.get('/api/nifty/atm-strikes-timeline', async (req: Request, res: Response): Promise<void> => {
  try {
    const intervalParam = req.query.interval as string || '3';
    const interval = parseInt(intervalParam, 10); // in minutes

    const niftyCollection = db.collection('nse_fno_index');
    const optionChainCollection = db.collection('nse_fno_index');

    const docs = await niftyCollection
      .find({ security_id: 56785 }, { projection: { _id: 0, LTP: 1, timestamp: 1 } })
      .sort({ timestamp: -1 })
      .toArray();

    const results: {
      atmStrike: number;
      niftyLTP: number;
      timestamp: string;
      callOI: number | null;
      putOI: number | null;
      callTimestamp: string | null;
      putTimestamp: string | null;
    }[] = [];

    const intervalMap: Record<string, boolean> = {};

    for (const doc of docs) {
      const ts = new Date(doc.timestamp);
      const ltp = parseFloat(doc.LTP);
      if (isNaN(ltp)) continue;

      // Dynamically round timestamp based on selected interval
      const rounded = new Date(Math.floor(ts.getTime() / (interval * 60 * 1000)) * (interval * 60 * 1000));
      const roundedISO = rounded.toISOString();

      if (!intervalMap[roundedISO]) {
        intervalMap[roundedISO] = true;
        const atmStrike = Math.round(ltp / 50) * 50;

        // Fetch option data near this interval window
        const optionData = await optionChainCollection.findOne({
          strike_price: atmStrike,
          trading_symbol: { $regex: '^NIFTY-Jun2025' },
          option_type: "CE",
          timestamp: {
            $gte: new Date(rounded.getTime()),
            $lt: new Date(rounded.getTime() + interval * 60 * 1000)
          }
        });

        const optionData2 = await optionChainCollection.findOne({
          strike_price: atmStrike,
          trading_symbol: { $regex: '^NIFTY-Jun2025' },
          option_type: "PE",
          timestamp: {
            $gte: new Date(rounded.getTime()),
            $lt: new Date(rounded.getTime() + interval * 60 * 1000)
          }
        });

        results.push({
          atmStrike,
          niftyLTP: ltp,
          timestamp: roundedISO,
          callOI: optionData?.OI ?? null,
          putOI: optionData2?.OI ?? null,
          callTimestamp: optionData?.timestamp ?? null,
          putTimestamp: optionData2?.timestamp ?? null
        });
      }
    }

    res.json({ atmStrikes: results });
  } catch (error) {
    console.error('Error fetching ATM strikes timeline:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/api/nifty/near5', async (req: Request, res: Response): Promise<void> => {
  try {
    const collection = db.collection('nse_fno_index');
    const latestNifty = await collection.find({ security_id: 56785 })
      .sort({ timestamp: -1 })
      .limit(1)
      .toArray();

    if (!latestNifty.length || !latestNifty[0].LTP) {
      res.status(404).json({ error: 'Nifty LTP not found' });
      return;
    }

    const niftyLTP = parseFloat(latestNifty[0].LTP);
    const atmStrike = Math.round(niftyLTP / 50) * 50;
    const strikePrices = Array.from({ length: 5 }, (_, i) => atmStrike - 100 + i * 50);

    const collection2 = db.collection('nse_fno_index');

    const results = await Promise.all(
      strikePrices.map(async (strike) => {
        const CE_docs = await collection2.find({
          strike_price: strike,
          option_type: "CE",
          trading_symbol: { $regex: '^NIFTY-Jun2025' }
        }).toArray();

        const PE_docs = await collection2.find({
          strike_price: strike,
          option_type: "PE",
          trading_symbol: { $regex: '^NIFTY-Jun2025' }
        }).toArray();

        // Group CE and PE by timestamp
        const groupedByTimestamp: { [key: string]: any } = {};

        CE_docs.forEach(doc => {
          const ts = new Date(doc.timestamp).toISOString();
          if (!groupedByTimestamp[ts]) groupedByTimestamp[ts] = { strike_price: strike };
          groupedByTimestamp[ts].callOI = doc.OI || 0;
          groupedByTimestamp[ts].callTimestamp = doc.timestamp;
        });

        PE_docs.forEach(doc => {
          const ts = new Date(doc.timestamp).toISOString();
          if (!groupedByTimestamp[ts]) groupedByTimestamp[ts] = { strike_price: strike };
          groupedByTimestamp[ts].putOI = doc.OI || 0;
          groupedByTimestamp[ts].putTimestamp = doc.timestamp;
        });

        // Return array of merged records per strike
        return Object.values(groupedByTimestamp);
      })
    );

    // Flatten the results into one array
    const flattened = results.flat();

    // Fetch Nifty data
    const niftyCollection = db.collection('nse_fno_index');
    const niftyDocs = await niftyCollection
      .find({ security_id: 56785 }, { projection: { _id: 0, LTP: 1, timestamp: 1 } })
      .sort({ timestamp: 1 })
      .toArray();

    const nifty = niftyDocs.map(doc => ({
      value: doc.LTP,
      timestamp: doc.timestamp,
    }));

    res.json({ atmStrike, overall: flattened,nifty });
  } catch (error) {
    console.error('Error fetching NEAR5:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});
// ---------------------------------------------------------------------------------------------
app.get('/api/nifty/overall', async (req: Request, res: Response): Promise<void> => {
  try {
    const collection = db.collection('nse_fno_index');
    const latestNifty = await collection.find({ security_id: 56785 })
      .sort({ timestamp: -1 })
      .limit(1)
      .toArray();

    if (!latestNifty.length || !latestNifty[0].LTP) {
      res.status(404).json({ error: 'Nifty LTP not found' });
      return;
    }

    const niftyLTP = parseFloat(latestNifty[0].LTP);
    const atmStrike = Math.round(niftyLTP / 50) * 50;
    const strikePrices = Array.from({ length: 21 }, (_, i) => atmStrike - 500 + i * 50);

    const collection2 = db.collection('nse_fno_index');

    const results = await Promise.all(
      strikePrices.map(async (strike) => {
        const CE_docs = await collection2.find({
          strike_price: strike,
          option_type: "CE",
          trading_symbol: { $regex: '^NIFTY-Jun2025' }
        }).toArray();

        const PE_docs = await collection2.find({
          strike_price: strike,
          option_type: "PE",
          trading_symbol: { $regex: '^NIFTY-Jun2025' }
        }).toArray();

        // Group CE and PE by timestamp
        const groupedByTimestamp: { [key: string]: any } = {};

        CE_docs.forEach(doc => {
          const ts = new Date(doc.timestamp).toISOString();
          if (!groupedByTimestamp[ts]) groupedByTimestamp[ts] = { strike_price: strike };
          groupedByTimestamp[ts].callOI = doc.OI || 0;
          groupedByTimestamp[ts].callTimestamp = doc.timestamp;
        });

        PE_docs.forEach(doc => {
          const ts = new Date(doc.timestamp).toISOString();
          if (!groupedByTimestamp[ts]) groupedByTimestamp[ts] = { strike_price: strike };
          groupedByTimestamp[ts].putOI = doc.OI || 0;
          groupedByTimestamp[ts].putTimestamp = doc.timestamp;
        });

        // Return array of merged records per strike
        return Object.values(groupedByTimestamp);
      })
    );

    // Flatten the results into one array
    const flattened = results.flat();

    // Fetch Nifty data
    const niftyCollection = db.collection('nse_fno_index');
    const niftyDocs = await niftyCollection
      .find({ security_id: 56785 }, { projection: { _id: 0, LTP: 1, timestamp: 1 } })
      .sort({ timestamp: 1 })
      .toArray();

    const nifty = niftyDocs.map(doc => ({
      value: doc.LTP,
      timestamp: doc.timestamp,
    }));

    res.json({ atmStrike, overall: flattened,nifty });
  } catch (error) {
    console.error('Error fetching NEAR5:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});






const securities = [
  { name: "AARTIIND JUN FUT", security_id: 56787, sector: "Chemicals" },
  { name: "ABB JUN FUT", security_id: 56788, sector: "Capital Goods" },
  { name: "ABCAPITAL JUN FUT", security_id: 56789, sector: "Financial Services" },
  { name: "ABFRL JUN FUT", security_id: 56794, sector: "Consumer Discretionary" },
  { name: "ACC JUN FUT", security_id: 56795, sector: "Cement" },
  { name: "ADANIENSOL JUN FUT", security_id: 56798, sector: "Utilities" },
  { name: "ADANIENT JUN FUT", security_id: 56799, sector: "Conglomerate" },
  { name: "ADANIGREEN JUN FUT", security_id: 56800, sector: "Utilities" },
  { name: "ADANIPORTS JUN FUT", security_id: 56801, sector: "Logistics" },
  { name: "ALKEM JUN FUT", security_id: 56806, sector: "Pharmaceuticals" },
  { name: "AMBUJACEM JUN FUT", security_id: 56807, sector: "Cement" },
  { name: "ANGELONE JUN FUT", security_id: 56808, sector: "Financial Services" },
  { name: "APLAPOLLO JUN FUT", security_id: 56809, sector: "Metals" },
  { name: "APOLLOHOSP JUN FUT", security_id: 56810, sector: "Healthcare" },
  { name: "ASHOKLEY JUN FUT", security_id: 56811, sector: "Automotive" },
  { name: "ASIANPAINT JUN FUT", security_id: 56816, sector: "Paints" },
  { name: "ASTRAL JUN FUT", security_id: 56817, sector: "Industrials" },
  { name: "ATGL JUN FUT", security_id: 56818, sector: "Utilities" },
  { name: "AUBANK JUN FUT", security_id: 56819, sector: "Banking" },
  { name: "AUROPHARMA JUN FUT", security_id: 56822, sector: "Pharmaceuticals" },
  { name: "AXISBANK JUN FUT", security_id: 56823, sector: "Banking" },
  { name: "BAJAJ-AUTO JUN FUT", security_id: 56826, sector: "Automotive" },
  { name: "BAJAJFINSV JUN FUT", security_id: 56827, sector: "Financial Services" },
  { name: "BAJFINANCE JUN FUT", security_id: 56828, sector: "Financial Services" },
  { name: "BALKRISIND JUN FUT", security_id: 56829, sector: "Automotive" },
  { name: "BANDHANBNK JUN FUT", security_id: 56830, sector: "Banking" },
  { name: "BANKBARODA JUN FUT", security_id: 56831, sector: "Banking" },
  { name: "BANKINDIA JUN FUT", security_id: 56832, sector: "Banking" },
  { name: "BEL JUN FUT", security_id: 56833, sector: "Defence" },
  { name: "BHARATFORG JUN FUT", security_id: 56834, sector: "Automotive" },
  { name: "BHARTIARTL JUN FUT", security_id: 56835, sector: "Telecom" },
  { name: "BHEL JUN FUT", security_id: 56838, sector: "Capital Goods" },
  { name: "BIOCON JUN FUT", security_id: 56839, sector: "Pharmaceuticals" },
  { name: "BOSCHLTD JUN FUT", security_id: 56844, sector: "Automotive" },
  { name: "BPCL JUN FUT", security_id: 56845, sector: "Oil & Gas" },
  { name: "BRITANNIA JUN FUT", security_id: 56846, sector: "FMCG" },
  { name: "BSE JUN FUT", security_id: 56847, sector: "Financial Services" },
  { name: "BSOFT JUN FUT", security_id: 56848, sector: "IT" },
  { name: "CAMS JUN FUT", security_id: 56849, sector: "Financial Services" },
  { name: "CANBK JUN FUT", security_id: 56850, sector: "Banking" },
  { name: "CDSL JUN FUT", security_id: 56851, sector: "Financial Services" },
  { name: "CESC JUN FUT", security_id: 56852, sector: "Utilities" },
  { name: "CGPOWER JUN FUT", security_id: 56853, sector: "Capital Goods" },
  { name: "CHAMBLFERT JUN FUT", security_id: 56856, sector: "Fertilizers" },
  { name: "CHOLAFIN JUN FUT", security_id: 56857, sector: "Financial Services" },
  { name: "CIPLA JUN FUT", security_id: 56858, sector: "Pharmaceuticals" },
  { name: "COALINDIA JUN FUT", security_id: 56859, sector: "Metals" },
  { name: "COFORGE JUN FUT", security_id: 56860, sector: "IT" },
  { name: "COLPAL JUN FUT", security_id: 56861, sector: "FMCG" },
  { name: "CONCOR JUN FUT", security_id: 56862, sector: "Logistics" },
  { name: "CROMPTON JUN FUT", security_id: 56863, sector: "Consumer Durables" },
  { name: "CUMMINSIND JUN FUT", security_id: 56864, sector: "Capital Goods" },
  { name: "CYIENT JUN FUT", security_id: 56865, sector: "IT" },
   { name: "DABUR JUN FUT", security_id: 56900, sector: "FMCG" },
  { name: "DALBHARAT JUN FUT", security_id: 56901, sector: "Cement" },
  { name: "DELHIVERY JUN FUT", security_id: 56904, sector: "Logistics" },
  { name: "DIVISLAB JUN FUT", security_id: 56905, sector: "Pharmaceuticals" },
  { name: "DIXON JUN FUT", security_id: 56906, sector: "Consumer Durables" },
  { name: "DLF JUN FUT", security_id: 56907, sector: "Real Estate" },
  { name: "DMART JUN FUT", security_id: 56908, sector: "Retail" },
  { name: "DRREDDY JUN FUT", security_id: 56909, sector: "Pharmaceuticals" },
  { name: "EICHERMOT JUN FUT", security_id: 56910, sector: "Automotive" },
  { name: "EXIDEIND JUN FUT", security_id: 56911, sector: "Automotive" },
  { name: "FEDERALBNK JUN FUT", security_id: 56918, sector: "Banking" },
  { name: "GAIL JUN FUT", security_id: 56919, sector: "Oil & Gas" },
  { name: "GLENMARK JUN FUT", security_id: 56926, sector: "Pharmaceuticals" },
  { name: "GMRAIRPORT JUN FUT", security_id: 56927, sector: "Logistics" },
  { name: "GODREJCP JUN FUT", security_id: 56928, sector: "FMCG" },
  { name: "GODREJPROP JUN FUT", security_id: 56929, sector: "Real Estate" },
  { name: "GRANULES JUN FUT", security_id: 56930, sector: "Pharmaceuticals" },
  { name: "GRASIM JUN FUT", security_id: 56931, sector: "Cement" },
  { name: "HAL JUN FUT", security_id: 56932, sector: "Defence" },
  { name: "HAVELLS JUN FUT", security_id: 56933, sector: "Consumer Durables" },
  { name: "HCLTECH JUN FUT", security_id: 56940, sector: "IT" },
  { name: "HDFCAMC JUN FUT", security_id: 56941, sector: "Financial Services" },
  { name: "HDFCBANK JUN FUT", security_id: 56946, sector: "Banking" },
  { name: "HDFCLIFE JUN FUT", security_id: 56947, sector: "Insurance" },
  { name: "HEROMOTOCO JUN FUT", security_id: 56952, sector: "Automotive" },
  { name: "HFCL JUN FUT", security_id: 56953, sector: "Telecom" },
  { name: "HINDALCO JUN FUT", security_id: 56954, sector: "Metals" },
  { name: "HINDCOPPER JUN FUT", security_id: 56955, sector: "Metals" },
  { name: "HINDPETRO JUN FUT", security_id: 56956, sector: "Oil & Gas" },
  { name: "HINDUNILVR JUN FUT", security_id: 56957, sector: "FMCG" },
   { name: "HINDZINC JUN FUT", security_id: 56966, sector: "Metals" },
  { name: "HUDCO JUN FUT", security_id: 56967, sector: "Financial Services" },
  { name: "ICICIBANK JUN FUT", security_id: 56968, sector: "Banking" },
  { name: "ICICIGI JUN FUT", security_id: 56969, sector: "Insurance" },
  { name: "ICICIPRULI JUN FUT", security_id: 56970, sector: "Insurance" },
  { name: "IDEA JUN FUT", security_id: 56971, sector: "Telecom" },
  { name: "IDFCFIRSTB JUN FUT", security_id: 56972, sector: "Banking" },
  { name: "IEX JUN FUT", security_id: 56973, sector: "Utilities" },
  { name: "IGL JUN FUT", security_id: 56986, sector: "Oil & Gas" },
  { name: "IIFL JUN FUT", security_id: 56987, sector: "Financial Services" },
  { name: "INDHOTEL JUN FUT", security_id: 56988, sector: "Hospitality" },
  { name: "INDIANB JUN FUT", security_id: 56989, sector: "Banking" },
  { name: "INDIGO JUN FUT", security_id: 56990, sector: "Aviation" },
  { name: "INDUSINDBK JUN FUT", security_id: 56991, sector: "Banking" },
  { name: "INDUSTOWER JUN FUT", security_id: 56994, sector: "Telecom" },
  { name: "INFY JUN FUT", security_id: 56995, sector: "IT" },
  { name: "INOXWIND JUN FUT", security_id: 57002, sector: "Capital Goods" },
  { name: "IOC JUN FUT", security_id: 57003, sector: "Oil & Gas" },
  { name: "IRB JUN FUT", security_id: 57004, sector: "Infrastructure" },
  { name: "IRCTC JUN FUT", security_id: 57005, sector: "Tourism" },
  { name: "IREDA JUN FUT", security_id: 57010, sector: "Financial Services" },
  { name: "IRFC JUN FUT", security_id: 57011, sector: "Financial Services" },
  { name: "ITC JUN FUT", security_id: 57012, sector: "FMCG" },
   { name: "JINDALSTEL JUN FUT", security_id: 57013, sector: "Metals" },
  { name: "JIOFIN JUN FUT", security_id: 57020, sector: "Financial Services" },
  { name: "JSL JUN FUT", security_id: 57021, sector: "Metals" },
  { name: "JSWENERGY JUN FUT", security_id: 57024, sector: "Utilities" },
  { name: "JSWSTEEL JUN FUT", security_id: 57025, sector: "Metals" },
  { name: "JUBLFOOD JUN FUT", security_id: 57026, sector: "Quick Service Restaurant" },
  { name: "KALYANKJIL JUN FUT", security_id: 57027, sector: "Retail" },
  { name: "KEI JUN FUT", security_id: 57032, sector: "Capital Goods" },
  { name: "KOTAKBANK JUN FUT", security_id: 57033, sector: "Banking" },
  { name: "KPITTECH JUN FUT", security_id: 57034, sector: "IT" },
  { name: "LAURUSLABS JUN FUT", security_id: 57035, sector: "Pharmaceuticals" },
  { name: "LICHSGFIN JUN FUT", security_id: 57038, sector: "Financial Services" },
  { name: "LICI JUN FUT", security_id: 57039, sector: "Insurance" },
  { name: "LODHA JUN FUT", security_id: 57042, sector: "Real Estate" },
  { name: "LT JUN FUT", security_id: 57043, sector: "Infrastructure" },
  { name: "LTF JUN FUT", security_id: 57048, sector: "Financial Services" },
  { name: "LTIM JUN FUT", security_id: 57049, sector: "IT" },
  { name: "LUPIN JUN FUT", security_id: 57050, sector: "Pharmaceuticals" },
    { name: "M&M JUN FUT", security_id: 57051, sector: "Automotive" },
  { name: "M&MFIN JUN FUT", security_id: 57052, sector: "Financial Services" },
  { name: "MANAPPURAM JUN FUT", security_id: 57053, sector: "Financial Services" },
  { name: "MARICO JUN FUT", security_id: 57054, sector: "FMCG" },
  { name: "MARUTI JUN FUT", security_id: 57055, sector: "Automotive" },
  { name: "MAXHEALTH JUN FUT", security_id: 57056, sector: "Healthcare" },
  { name: "MCX JUN FUT", security_id: 57057, sector: "Financial Services" },
  { name: "MFSL JUN FUT", security_id: 57058, sector: "Insurance" },
  { name: "MGL JUN FUT", security_id: 57059, sector: "Oil & Gas" },
  { name: "MOTHERSON JUN FUT", security_id: 57060, sector: "Automotive" },
  { name: "MPHASIS JUN FUT", security_id: 57061, sector: "IT" },
  { name: "MUTHOOTFIN JUN FUT", security_id: 57062, sector: "Financial Services" },
  { name: "NATIONALUM JUN FUT", security_id: 57063, sector: "Metals" },
  { name: "NAUKRI JUN FUT", security_id: 57064, sector: "IT" },
  { name: "NBCC JUN FUT", security_id: 57065, sector: "Construction" },
  { name: "NCC JUN FUT", security_id: 57066, sector: "Construction" },
  { name: "NESTLEIND JUN FUT", security_id: 57067, sector: "FMCG" },
  { name: "NHPC JUN FUT", security_id: 57068, sector: "Utilities" },
   { name: "NMDC JUN FUT", security_id: 57069, sector: "Metals" },
  { name: "NTPC JUN FUT", security_id: 57070, sector: "Utilities" },
  { name: "NYKAA JUN FUT", security_id: 57071, sector: "Retail" },
  { name: "OBEROIRLTY JUN FUT", security_id: 57072, sector: "Real Estate" },
  { name: "OFSS JUN FUT", security_id: 57073, sector: "IT" },
  { name: "OIL JUN FUT", security_id: 57074, sector: "Oil & Gas" },
  { name: "ONGC JUN FUT", security_id: 57075, sector: "Oil & Gas" },
  { name: "PAGEIND JUN FUT", security_id: 57077, sector: "Textiles" },
  { name: "PATANJALI JUN FUT", security_id: 57079, sector: "FMCG" },
  { name: "PAYTM JUN FUT", security_id: 57080, sector: "IT" },
  { name: "PEL JUN FUT", security_id: 57081, sector: "Financial Services" },
  { name: "PERSISTENT JUN FUT", security_id: 57082, sector: "IT" },
    { name: "PETRONET JUN FUT", security_id: 57083, sector: "Oil & Gas" },
  { name: "PFC JUN FUT", security_id: 57084, sector: "Financial Services" },
  { name: "PHOENIXLTD JUN FUT", security_id: 57085, sector: "Real Estate" },
  { name: "PIDILITIND JUN FUT", security_id: 57086, sector: "Chemicals" },
  { name: "PIIND JUN FUT", security_id: 57087, sector: "Chemicals" },
  { name: "PNB JUN FUT", security_id: 57088, sector: "Banking" },
  { name: "PNBHOUSING JUN FUT", security_id: 57091, sector: "Financial Services" },
  { name: "POLICYBZR JUN FUT", security_id: 57092, sector: "IT" },
  { name: "POLYCAB JUN FUT", security_id: 57093, sector: "Capital Goods" },
   { name: "POONAWALLA JUN FUT", security_id: 57094, sector: "Financial Services" },
  { name: "POWERGRID JUN FUT", security_id: 57095, sector: "Utilities" },
  { name: "PRESTIGE JUN FUT", security_id: 57100, sector: "Real Estate" },
  { name: "RBLBANK JUN FUT", security_id: 57101, sector: "Banking" },
  { name: "RECLTD JUN FUT", security_id: 57104, sector: "Financial Services" },
  { name: "RELIANCE JUN FUT", security_id: 57105, sector: "Conglomerate" },
  { name: "SAIL JUN FUT", security_id: 57110, sector: "Metals" },
  { name: "SBICARD JUN FUT", security_id: 57111, sector: "Financial Services" },
  { name: "SBILIFE JUN FUT", security_id: 57112, sector: "Insurance" },
  { name: "SBIN JUN FUT", security_id: 57113, sector: "Banking" },
  { name: "SHREECEM JUN FUT", security_id: 57114, sector: "Cement" },
  { name: "SHRIRAMFIN JUN FUT", security_id: 57115, sector: "Financial Services" },
  { name: "SIEMENS JUN FUT", security_id: 57120, sector: "Capital Goods" },
  { name: "SJVN JUN FUT", security_id: 57121, sector: "Utilities" },
  { name: "SOLARINDS JUN FUT", security_id: 57122, sector: "Chemicals" },
  { name: "SONACOMS JUN FUT", security_id: 57123, sector: "Automotive" },
  { name: "SRF JUN FUT", security_id: 57128, sector: "Chemicals" },
  { name: "SUNPHARMA JUN FUT", security_id: 57129, sector: "Pharmaceuticals" },
  { name: "SUPREMEIND JUN FUT", security_id: 57200, sector: "Consumer Durables" },
  { name: "SYNGENE JUN FUT", security_id: 57201, sector: "Pharmaceuticals" },
  { name: "TATACHEM JUN FUT", security_id: 57222, sector: "Chemicals" },
  { name: "TATACOMM JUN FUT", security_id: 57223, sector: "Telecom" },
  { name: "TATACONSUM JUN FUT", security_id: 57224, sector: "FMCG" },
  { name: "TATAELXSI JUN FUT", security_id: 57225, sector: "IT" },
  { name: "TATAMOTORS JUN FUT", security_id: 57238, sector: "Automotive" },
  { name: "TATAPOWER JUN FUT", security_id: 57239, sector: "Utilities" },
  { name: "TATASTEEL JUN FUT", security_id: 57248, sector: "Metals" },
  { name: "TATATECH JUN FUT", security_id: 57249, sector: "IT" },
  { name: "TCS JUN FUT", security_id: 57250, sector: "IT" },
  { name: "TECHM JUN FUT", security_id: 57251, sector: "IT" },
  { name: "TIINDIA JUN FUT", security_id: 57252, sector: "Automotive" },
  { name: "TITAGARH JUN FUT", security_id: 57253, sector: "Capital Goods" },
  { name: "TITAN JUN FUT", security_id: 57254, sector: "Consumer Discretionary" },
   { name: "TORNTPHARM JUN FUT", security_id: 57255, sector: "Pharmaceuticals" },
  { name: "TORNTPOWER JUN FUT", security_id: 57256, sector: "Utilities" },
  { name: "TRENT JUN FUT", security_id: 57257, sector: "Retail" },
  { name: "TVSMOTOR JUN FUT", security_id: 57258, sector: "Automotive" },
  { name: "ULTRACEMCO JUN FUT", security_id: 57261, sector: "Cement" },
  { name: "UNIONBANK JUN FUT", security_id: 57262, sector: "Banking" },
  { name: "UNITDSPR JUN FUT", security_id: 57263, sector: "FMCG" },
  { name: "UPL JUN FUT", security_id: 57264, sector: "Chemicals" },
  { name: "VBL JUN FUT", security_id: 57273, sector: "FMCG" },
   { name: "VEDL JUN FUT", security_id: 57274, sector: "Metals" },
  { name: "VOLTAS JUN FUT", security_id: 57275, sector: "Consumer Durables" },
  { name: "WIPRO JUN FUT", security_id: 57276, sector: "IT" },
  { name: "YESBANK JUN FUT", security_id: 57277, sector: "Banking" },
  { name: "ETERNAL JUN FUT", security_id: 57278, sector: "Healthcare" },
  { name: "ZYDUSLIFE JUN FUT", security_id: 57283, sector: "Pharmaceuticals" },
   { name: "BDL JUN FUT", security_id: 64224, sector: "Defence" },
  { name: "BLUESTARCO JUN FUT", security_id: 64232, sector: "Consumer Durables" },
  { name: "FORTIS JUN FUT", security_id: 64411, sector: "Healthcare" },
  { name: "KAYNES JUN FUT", security_id: 64623, sector: "IT" },
  { name: "MANKIND JUN FUT", security_id: 64898, sector: "Pharmaceuticals" },
  { name: "MAZDOCK JUN FUT", security_id: 64906, sector: "Defence" },
  { name: "PPLPHARMA JUN FUT", security_id: 64987, sector: "Pharmaceuticals" },
  { name: "RVNL JUN FUT", security_id: 64996, sector: "Infrastructure" },
  { name: "UNOMINDA JUN FUT", security_id: 65236, sector: "Automotive" },

  // ...add more as needed
];

interface StockData {
  _id: string;
  trading_symbol: string;
  LTP: string;
  close: string;
  sector: string;
  security_id: number | string;
  change?: number;
  [key: string]: any;
}
app.get('/api/heatmap', async (req, res) => {
  try {
    const collection = db.collection('nse_fno');
    console.log('Connected to collection:', collection.collectionName);

    const securityIds = securities.map(s => s.security_id);
    console.log(`Looking for ${securityIds.length} securities:`, securityIds.slice(0, 5), '...'); // Show first 5 IDs

    // Query as any[]
    let items: any[] = await collection.find({ security_id: { $in: securityIds } })
      .sort({ _id: -1 })
      
      .toArray();
    console.log(`First query found ${items.length} items with numeric security_ids`);

    // If none found, try string IDs
    if (!items || items.length === 0) {
      console.log('Trying with string security_ids...');
      items = await collection.find({ security_id: { $in: securityIds.map(id => id.toString()) } })
        .sort({ _id: -1 })
        
        .toArray();
        console.log(`Second query found ${items.length} items with string security_ids`);
    }

    // Fallback: get latest 50 (unfiltered)
    let fallbackItems: any[] = [];
    if (!items || items.length === 0) {
      console.log('No items found with security_ids, falling back to latest 50');
      try {
        fallbackItems = await collection.find({}).sort({ _id: -1 }).limit(50).toArray();
        console.log(`Fallback query found ${fallbackItems.length} items`);
      } catch (fallbackError: unknown) {
        let msg = 'Unknown fallback error';
        if (fallbackError instanceof Error) {
          msg = fallbackError.message;
        }
        console.error('Error during fallback DB query:', msg);
        fallbackItems = [];
      }
    }

    const resultItems: any[] =
      (Array.isArray(items) && items.length > 0)
        ? items
        : (Array.isArray(fallbackItems) ? fallbackItems : []);
         console.log(`Total items to process: ${resultItems.length}`);

    // Attach trading_symbol and sector from securities list
    const processedItems: StockData[] = resultItems.map((item) => {
      const secIdNum = Number(item.security_id); // Always compare as number
      const found = securities.find(sec => Number(sec.security_id) === secIdNum);
      return {
        ...item,
        trading_symbol: found ? found.name : '',
        sector: found ? found.sector : (item.sector || 'Unknown')
      };
    });

    console.log('\nFinal processed items count:', processedItems.length);
    console.log('Sample of first 5 processed items:');
    console.log(JSON.stringify(processedItems.slice(0, 5), null, 2)); // Print first 5 for debug

    res.json(processedItems);
  } catch (error: unknown) {
    let msg = 'Unknown error';
    if (error instanceof Error) {
      msg = error.message;
    }
    console.error('\nError fetching heatmap data:', error);
    res.status(500).json({
      error: 'Internal server error',
      details: msg
    });
  }
});






// ---------------------------------------------------------------------fii dii----------------------------

// Helper: Normalize various date formats to "DD-MM-YYYY"

function normalizeDate(dateString: string, source: 'nse' | 'cash' | 'nifty'): string | null {
  if (!dateString) return null;

  try {
    // Initialize with default values
    let day = '';
    let month = '';
    let year = '';

    if (source === "nse") {
      // "YYYY-MM-DD" → "DD-MM-YYYY"
      const parts = dateString.split("-");
      if (parts.length !== 3) return null;
      [year, month, day] = parts;
    } else if (source === "cash") {
      // "17-Mar-25" → "DD-MM-YYYY"
      const parts = dateString.trim().split("-");
      if (parts.length !== 3) return null;
      
      const [d, mStr, yShort] = parts;
      const months: Record<string, string> = {
        Jan: "01", Feb: "02", Mar: "03", Apr: "04", May: "05", Jun: "06",
        Jul: "07", Aug: "08", Sep: "09", Oct: "10", Nov: "11", Dec: "12"
      };
      
      day = d.padStart(2, "0");
      month = months[mStr] || ''; // Provide fallback for invalid month
      year = yShort.length === 2 ? `20${yShort}` : yShort;
    } else if (source === "nifty") {
      // "02-APR-2025" → "DD-MM-YYYY"
      const parts = dateString.trim().split("-");
      if (parts.length !== 3) return null;
      
      const [d, mStr, y] = parts;
      const months: Record<string, string> = {
        JAN: "01", FEB: "02", MAR: "03", APR: "04", MAY: "05", JUN: "06",
        JUL: "07", AUG: "08", SEP: "09", OCT: "10", NOV: "11", DEC: "12"
      };
      
      day = d.padStart(2, "0");
      month = months[mStr.toUpperCase()] || ''; // Provide fallback for invalid month
      year = y;
    }

    // Validate all components were set
    if (!day || !month || !year) {
      return null;
    }

    return `${day}-${month}-${year}`;
  } catch (err) {
    console.error(`Failed to normalize ${source} date:`, dateString);
    return null;
  }
}

 




// ========== ROUTES ==========


// Fetch FII/DII cash data
app.get("/api/fii-dii-data", async (req, res) => {
  try {
    const collection = db.collection("cash_data");
    const rows = await collection

      .find({}, {
        projection: {
          Date: 1,
          "FII Net Purchase/Sales": 1,
          "DII Net Purchase/Sales": 1,
          _id: 0
        }
      })
      .sort({ Date: 1 })
      .toArray();

    const formattedData = rows.map(row => {
      const [yy, mm, dd] = row.Date.split("-");
      return {
        date: row.Date,
        month: mm,
        year: parseInt(dd, 10),
        FII: parseFloat(row["FII Net Purchase/Sales"]),
        DII: parseFloat(row["DII Net Purchase/Sales"]),
      };
    });

    res.json(formattedData);
  } catch (err) {
    console.error("Error in /api/fii-dii-data:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});


// Fetch FII Option Index Call/Put change & Nifty value
app.get("/api/data", async (req, res) => {
  try {
    const nseColl = db.collection("nse");
    const niftyColl = db.collection("Nifty");
 
    // Fetch all FII rows from NSE
    const nseRows = await nseColl
      .find({ "Client Type": "FII" }, {
        projection: {
          Date: 1,
          "Option Index Call Long": 1,
          "Option Index Call Short": 1,
          "Option Index Put Long": 1,
          "Option Index Put Short": 1,
          _id: 0
        }
      })
      .toArray();

    // Fetch all Nifty rows
    const niftyRows = await niftyColl
      .find({}, { projection: { Date: 1, Close: 1, _id: 0 } })
      .toArray();

    // Build a map from normalized Nifty date → Close
    const niftyMap: Record<string, number> = {};
    niftyRows.forEach(nifty => {
      const norm = dayjs(nifty.Date, "DD-MMM-YYYY").format("YYYY-MM-DD");
      niftyMap[norm] = Number(nifty.Close);
    });

    // Compute FII Call/Put changes and merge Nifty values
    const result = nseRows.map(row => ({
      Date: row.Date,
      FII_Call_Change:
        (row["Option Index Call Long"] || 0) - (row["Option Index Call Short"] || 0),
      FII_Put_Change:
        (row["Option Index Put Long"] || 0) - (row["Option Index Put Short"] || 0),
      NIFTY_Value: niftyMap[row.Date] ?? null,
    }));

    // Sort by Date ascending
    result.sort((a, b) => (a.Date < b.Date ? -1 : a.Date > b.Date ? 1 : 0));
    res.json(result);
  } catch (err) {
    console.error("Error in /api/data:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

// Fetch FII Index Futures change & Nifty value (with manualPrevNetOI for 2025-04-07)
app.get("/api/FII_Index_Fut/data", async (req, res) => {
  try {
    const nseColl = db.collection("nse");
    const niftyColl = db.collection("Nifty");

    // Fetch all FII rows from NSE
    const rows = await nseColl
      .find({ "Client Type": "FII" }, {
        projection: {
          Date: 1,
          "Future Index Long": 1,
          "Future Index Short": 1,
          _id: 0
        }
      })
      .sort({ Date: 1 })
      .toArray();

    // Fetch all Nifty rows for mapping
    const niftyRows = await niftyColl
      .find({}, { projection: { Date: 1, Close: 1, _id: 0 } })
      .toArray();

    const niftyMap: Record<string, number> = {};

    niftyRows.forEach(nifty => {
      const norm = dayjs(nifty.Date, "DD-MMM-YYYY").format("YYYY-MM-DD");
      niftyMap[norm] = Number(nifty.Close);
    });

    const manualPrevNetOI = -86592;
    let prevNetOI = manualPrevNetOI;

    const resultWithChange = rows.map(row => {
      const dateStr = row.Date;
      const currentNetOI =
        (row["Future Index Long"] || 0) - (row["Future Index Short"] || 0);
      const change =
        dateStr === "2025-04-07"
          ? currentNetOI - manualPrevNetOI
          : currentNetOI - prevNetOI;

      prevNetOI = currentNetOI;

      return {
        Date: dateStr,
        FII_Index_Futures: change,
        NIFTY_Value: niftyMap[dateStr] ?? null,
      };
    });

    res.json(resultWithChange);
  } catch (err) {
    console.error("Error in /api/FII_Index_Fut/data:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});


// Fetch FII Option Index OI & Nifty value
app.get("/api/OIFII_Index_Opt/data", async (req, res) => {
  try {
    const nseColl = db.collection("nse");
    const niftyColl = db.collection("Nifty");

    const rows = await nseColl
      .find({ "Client Type": "FII" }, {
        projection: {
          Date: 1,
          "Option Index Call Long": 1,
          "Option Index Call Short": 1,
          "Option Index Put Long": 1,
          "Option Index Put Short": 1,
          _id: 0
        }
      })
      .sort({ Date: 1 })
      .toArray();

    const niftyRows = await niftyColl
      .find({}, { projection: { Date: 1, Close: 1, _id: 0 } })
      .toArray();

    const niftyMap: Record<string, number> = {};

    niftyRows.forEach(nifty => {
      const norm = dayjs(nifty.Date, "DD-MMM-YYYY").format("YYYY-MM-DD");
      niftyMap[norm] = Number(nifty.Close);
    });

    const result = rows.map(row => ({
      Date: row.Date,
      FII_Call_OI:
        (row["Option Index Call Long"] || 0) - (row["Option Index Call Short"] || 0),
      FII_Put_OI:
        (row["Option Index Put Long"] || 0) - (row["Option Index Put Short"] || 0),
      NIFTY_Value: niftyMap[row.Date] ?? null,
    }));

    res.json(result);
  } catch (err) {
    console.error("Error in /api/OIFII_Index_Opt/data:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

// Fetch FII Futures OI & Nifty value
app.get("/api/OIFII_Index_Fut/data", async (req, res) => {
  try {
    const nseColl = db.collection("nse");
    const niftyColl = db.collection("Nifty");

    const rows = await nseColl
      .find({ "Client Type": "FII" }, {
        projection: {
          Date: 1,
          "Future Index Long": 1,
          "Future Index Short": 1,
          _id: 0
        }
      })

      .sort({ Date: 1 })
      .toArray();

    const niftyRows = await niftyColl
      .find({}, { projection: { Date: 1, Close: 1, _id: 0 } })
      .toArray();

    const niftyMap: Record<string, number> = {};

    niftyRows.forEach(nifty => {
      const norm = dayjs(nifty.Date, "DD-MMM-YYYY").format("YYYY-MM-DD");
      niftyMap[norm] = Number(nifty.Close);
    });

    const result = rows.map(row => ({
      Date: row.Date,
      FII_Futures_OI:
        (row["Future Index Long"] || 0) - (row["Future Index Short"] || 0),
      NIFTY_Value: niftyMap[row.Date] ?? null,
    }));

    res.json(result);
  } catch (err) {
    console.error("Error in /api/OIFII_Index_Fut/data:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

// Fetch DII Option Index change & Nifty value
app.get("/api/DII_Index_Opt/data", async (req, res) => {
  try {
    const nseColl = db.collection("nse");
    const niftyColl = db.collection("Nifty");

    const rows = await nseColl
      .find({ "Client Type": "DII" }, {
        projection: {
          Date: 1,
          "Option Index Call Long": 1,
          "Option Index Call Short": 1,
          "Option Index Put Long": 1,
          "Option Index Put Short": 1,
          _id: 0
        }
      })
      .sort({ Date: 1 })
      .toArray();

    const niftyRows = await niftyColl
      .find({}, { projection: { Date: 1, Close: 1, _id: 0 } })
      .toArray();

    const niftyMap: Record<string, number> = {};
    niftyRows.forEach(nifty => {
      const norm = dayjs(nifty.Date, "DD-MMM-YYYY").format("YYYY-MM-DD");
      niftyMap[norm] = Number(nifty.Close);
    });

    const result = rows.map(row => ({
      Date: row.Date,
      DII_Call_Change:
        (row["Option Index Call Long"] || 0) - (row["Option Index Call Short"] || 0),
      DII_Put_Change:
        (row["Option Index Put Long"] || 0) - (row["Option Index Put Short"] || 0),
      NIFTY_Value: niftyMap[row.Date] ?? null,
    }));

    res.json(result);
  } catch (err) {
    console.error("Error in /api/DII_Index_Opt/data:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

// Fetch DII Index Futures change & Nifty value (with manualPrevNetOI)
app.get("/api/DII_Index_Fut/data", async (req, res) => {
  try {
    const nseColl = db.collection("nse");
    const niftyColl = db.collection("Nifty");

    const rows = await nseColl
      .find({ "Client Type": "DII" }, {
        projection: {
          Date: 1,
          "Future Index Long": 1,
          "Future Index Short": 1,
          _id: 0
        }
      })
      .sort({ Date: 1 })
      .toArray();

    const niftyRows = await niftyColl
      .find({}, { projection: { Date: 1, Close: 1, _id: 0 } })
      .toArray();

    const niftyMap: Record<string, number> = {};
    niftyRows.forEach(nifty => {
      const norm = dayjs(nifty.Date, "DD-MMM-YYYY").format("YYYY-MM-DD");
      niftyMap[norm] = Number(nifty.Close);
    });

    const manualPrevNetOI = 75721;
    let prevNetOI = manualPrevNetOI;

    const resultWithChange = rows.map(row => {
      const dateStr = row.Date;
      const currentNetOI =
        (row["Future Index Long"] || 0) - (row["Future Index Short"] || 0);

      const change =
        dateStr === "2025-04-07"
          ? currentNetOI - manualPrevNetOI
          : currentNetOI - prevNetOI;
      prevNetOI = currentNetOI;

      return {
        Date: dateStr,
        DII_Index_Futures: change,
        NIFTY_Value: niftyMap[dateStr] ?? null,
      };
    });

    res.json(resultWithChange);
  } catch (err) {
    console.error("Error in /api/DII_Index_Fut/data:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

// Fetch DII Option Index OI & Nifty value
app.get("/api/OIDII_Index_Opt/data", async (req, res) => {
  try {
    const nseColl = db.collection("nse");
    const niftyColl = db.collection("Nifty");

    const rows = await nseColl
      .find({ "Client Type": "DII" }, {
        projection: {
          Date: 1,
          "Option Index Call Long": 1,
          "Option Index Call Short": 1,
          "Option Index Put Long": 1,
          "Option Index Put Short": 1,
          _id: 0
        }
      })
      .sort({ Date: 1 })
      .toArray();

    const niftyRows = await niftyColl
      .find({}, { projection: { Date: 1, Close: 1, _id: 0 } })
      .toArray();

    const niftyMap: Record<string, number> = {};

    niftyRows.forEach(nifty => {
      const norm = dayjs(nifty.Date, "DD-MMM-YYYY").format("YYYY-MM-DD");
      niftyMap[norm] = Number(nifty.Close);
    });

    const result = rows.map(row => ({
      Date: row.Date,
      DII_Call_OI:
        (row["Option Index Call Long"] || 0) - (row["Option Index Call Short"] || 0),
      DII_Put_OI:
        (row["Option Index Put Long"] || 0) - (row["Option Index Put Short"] || 0),
      NIFTY_Value: niftyMap[row.Date] ?? null,
    }));

    res.json(result);
  } catch (err) {
    console.error("Error in /api/OIDII_Index_Opt/data:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

// Fetch DII Futures OI & Nifty value
app.get("/api/OIDII_Index_Fut/data", async (req, res) => {
  try {
    const nseColl = db.collection("nse");
    const niftyColl = db.collection("Nifty");

    const rows = await nseColl
      .find({ "Client Type": "DII" }, {
        projection: {
          Date: 1,
          "Future Index Long": 1,
          "Future Index Short": 1,
          _id: 0
        }
      })
      .sort({ Date: 1 })
      .toArray();

    const niftyRows = await niftyColl
      .find({}, { projection: { Date: 1, Close: 1, _id: 0 } })
      .toArray();

    const niftyMap: Record<string, number> = {};

    niftyRows.forEach(nifty => {
      const norm = dayjs(nifty.Date, "DD-MMM-YYYY").format("YYYY-MM-DD");
      niftyMap[norm] = Number(nifty.Close);
    });

    const result = rows.map(row => ({
      Date: row.Date,
      DII_Futures_OI:
        (row["Future Index Long"] || 0) - (row["Future Index Short"] || 0),
      NIFTY_Value: niftyMap[row.Date] ?? null,
    }));

    res.json(result);
  } catch (err) {
    console.error("Error in /api/OIDII_Index_Fut/data:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

// Fetch Pro Option Index change & Nifty value
app.get("/api/Pro_Index_Opt/data", async (req, res) => {
  try {
    const nseColl = db.collection("nse");
    const niftyColl = db.collection("Nifty");

    const rows = await nseColl
      .find({ "Client Type": "Pro" }, {
        projection: {
          Date: 1,
          "Option Index Call Long": 1,
          "Option Index Call Short": 1,
          "Option Index Put Long": 1,
          "Option Index Put Short": 1,
          _id: 0
        }

      })

      .sort({ Date: 1 })

      .toArray();

 

    const niftyRows = await niftyColl

      .find({}, { projection: { Date: 1, Close: 1, _id: 0 } })

      .toArray();

 

    const niftyMap: Record<string, number> = {};

    niftyRows.forEach(nifty => {

      const norm = dayjs(nifty.Date, "DD-MMM-YYYY").format("YYYY-MM-DD");

      niftyMap[norm] = Number(nifty.Close);

    });

 

    const result = rows.map(row => ({

      Date: row.Date,

      Pro_Call_Change:

        (row["Option Index Call Long"] || 0) - (row["Option Index Call Short"] || 0),

      Pro_Put_Change:

        (row["Option Index Put Long"] || 0) - (row["Option Index Put Short"] || 0),

      NIFTY_Value: niftyMap[row.Date] ?? null,

    }));

 

    res.json(result);

  } catch (err) {

    console.error("Error in /api/Pro_Index_Opt/data:", err);

    res.status(500).json({ error: "Internal Server Error" });

  }

});

 

// Fetch Pro Index Futures change & Nifty value (with manualPrevNetOI)

app.get("/api/Pro_Index_Fut/data", async (req, res) => {

  try {

    const nseColl = db.collection("nse");

    const niftyColl = db.collection("Nifty");

 

    const rows = await nseColl

      .find({ "Client Type": "Pro" }, {

        projection: {

          Date: 1,

          "Future Index Long": 1,

          "Future Index Short": 1,

          _id: 0

        }

      })

      .sort({ Date: 1 })

      .toArray();

 

    const niftyRows = await niftyColl

      .find({}, { projection: { Date: 1, Close: 1, _id: 0 } })

      .toArray();

 

    const niftyMap: Record<string, number> = {};

    niftyRows.forEach(nifty => {

      const norm = dayjs(nifty.Date, "DD-MMM-YYYY").format("YYYY-MM-DD");

      niftyMap[norm] = Number(nifty.Close);

    });

 

    const manualPrevNetOI = -29643;

    let prevNetOI = manualPrevNetOI;

 

    const resultWithChange = rows.map(row => {

      const dateStr = row.Date;

      const currentNetOI =

        (row["Future Index Long"] || 0) - (row["Future Index Short"] || 0);

 

      const change =

        dateStr === "2025-04-07"

          ? currentNetOI - manualPrevNetOI

          : currentNetOI - prevNetOI;

 

      prevNetOI = currentNetOI;

 

      return {

        Date: dateStr,

        Pro_Index_Futures: change,

        NIFTY_Value: niftyMap[dateStr] ?? null,

      };

    });

 

    res.json(resultWithChange);

  } catch (err) {

    console.error("Error in /api/Pro_Index_Fut/data:", err);

    res.status(500).json({ error: "Internal Server Error" });

  }

});

 

// Fetch Pro Option Index OI & Nifty value

app.get("/api/OIPro_Index_Opt/data", async (req, res) => {

  try {

    const nseColl = db.collection("nse");

    const niftyColl = db.collection("Nifty");

 

    const rows = await nseColl

      .find({ "Client Type": "Pro" }, {

        projection: {

          Date: 1,

          "Option Index Call Long": 1,

          "Option Index Call Short": 1,

          "Option Index Put Long": 1,

          "Option Index Put Short": 1,

          _id: 0

        }

      })

      .sort({ Date: 1 })

      .toArray();

 

    const niftyRows = await niftyColl

      .find({}, { projection: { Date: 1, Close: 1, _id: 0 } })

      .toArray();

 

    const niftyMap: Record<string, number> = {};

    niftyRows.forEach(nifty => {

      const norm = dayjs(nifty.Date, "DD-MMM-YYYY").format("YYYY-MM-DD");

      niftyMap[norm] = Number(nifty.Close);

    });

 

    const result = rows.map(row => ({

      Date: row.Date,

      Pro_Call_OI:

        (row["Option Index Call Long"] || 0) - (row["Option Index Call Short"] || 0),

      Pro_Put_OI:

        (row["Option Index Put Long"] || 0) - (row["Option Index Put Short"] || 0),

      NIFTY_Value: niftyMap[row.Date] ?? null,

    }));

 

    res.json(result);

  } catch (err) {

    console.error("Error in /api/OIPro_Index_Opt/data:", err);

    res.status(500).json({ error: "Internal Server Error" });

  }

});

 

// Fetch Pro Futures OI & Nifty value

app.get("/api/OIPro_Index_Fut/data", async (req, res) => {

  try {

    const nseColl = db.collection("nse");

    const niftyColl = db.collection("Nifty");

 

    const rows = await nseColl

      .find({ "Client Type": "Pro" }, {

        projection: {

          Date: 1,

          "Future Index Long": 1,

          "Future Index Short": 1,

          _id: 0

        }

      })

      .sort({ Date: 1 })

      .toArray();

 

    const niftyRows = await niftyColl

      .find({}, { projection: { Date: 1, Close: 1, _id: 0 } })

      .toArray();

 

    const niftyMap: Record<string, number> = {};

    niftyRows.forEach(nifty => {

      const norm = dayjs(nifty.Date, "DD-MMM-YYYY").format("YYYY-MM-DD");

      niftyMap[norm] = Number(nifty.Close);

    });

 

    const result = rows.map(row => ({

      Date: row.Date,

      Pro_Futures_OI:

        (row["Future Index Long"] || 0) - (row["Future Index Short"] || 0),

      NIFTY_Value: niftyMap[row.Date] ?? null,

    }));

 

    res.json(result);

  } catch (err) {

    console.error("Error in /api/OIPro_Index_Fut/data:", err);

    res.status(500).json({ error: "Internal Server Error" });

  }

});

 

// Fetch Client Option Index change & Nifty value

app.get("/api/Client_Index_Opt/data", async (req, res) => {

  try {

    const nseColl = db.collection("nse");

    const niftyColl = db.collection("Nifty");

 

    const rows = await nseColl

      .find({ "Client Type": "Client" }, {

        projection: {

          Date: 1,

          "Option Index Call Long": 1,

          "Option Index Call Short": 1,

          "Option Index Put Long": 1,

          "Option Index Put Short": 1,

          _id: 0

        }

      })

      .sort({ Date: 1 })

      .toArray();

 

    const niftyRows = await niftyColl

      .find({}, { projection: { Date: 1, Close: 1, _id: 0 } })

      .toArray();

 

    const niftyMap: Record<string, number> = {};

    niftyRows.forEach(nifty => {

      const norm = dayjs(nifty.Date, "DD-MMM-YYYY").format("YYYY-MM-DD");

      niftyMap[norm] = Number(nifty.Close);

    });

 

    const result = rows.map(row => ({

      Date: row.Date,

      Client_Call_Change:

        (row["Option Index Call Long"] || 0) - (row["Option Index Call Short"] || 0),

      Client_Put_Change:

        (row["Option Index Put Long"] || 0) - (row["Option Index Put Short"] || 0),

      NIFTY_Value: niftyMap[row.Date] ?? null,

    }));

 

    res.json(result);

  } catch (err) {

    console.error("Error in /api/Client_Index_Opt/data:", err);

    res.status(500).json({ error: "Internal Server Error" });

  }

});

 

// Fetch Client Index Futures change & Nifty value (with manualPrevNetOI)

app.get("/api/Client_Index_Fut/data", async (req, res) => {

  try {

    const nseColl = db.collection("nse");

    const niftyColl = db.collection("Nifty");

 

    const rows = await nseColl

      .find({ "Client Type": "Client" }, {

        projection: {

          Date: 1,

          "Future Index Long": 1,

          "Future Index Short": 1,

          _id: 0

        }

      })

      .sort({ Date: 1 })

      .toArray();

 

    const niftyRows = await niftyColl

      .find({}, { projection: { Date: 1, Close: 1, _id: 0 } })

      .toArray();

 

    const niftyMap: Record<string, number> = {};

    niftyRows.forEach(nifty => {

      const norm = dayjs(nifty.Date, "DD-MMM-YYYY").format("YYYY-MM-DD");

      niftyMap[norm] = Number(nifty.Close);

    });

 

    const manualPrevNetOI = 40514;

    let prevNetOI = manualPrevNetOI;

 

    const resultWithChange = rows.map(row => {

      const dateStr = row.Date;

      const currentNetOI =

        (row["Future Index Long"] || 0) - (row["Future Index Short"] || 0);

 

      const change =

        dateStr === "2025-04-07"

          ? currentNetOI - manualPrevNetOI

          : currentNetOI - prevNetOI;

 

      prevNetOI = currentNetOI;

 

      return {

        Date: dateStr,

        Client_Index_Futures: change,

        NIFTY_Value: niftyMap[dateStr] ?? null,

      };

    });

 

    res.json(resultWithChange);

  } catch (err) {

    console.error("Error in /api/Client_Index_Fut/data:", err);

    res.status(500).json({ error: "Internal Server Error" });

  }

});

 

// Fetch Client Option Index OI & Nifty value

app.get("/api/OIClient_Index_Opt/data", async (req, res) => {

  try {

    const nseColl = db.collection("nse");

    const niftyColl = db.collection("Nifty");

 

    const rows = await nseColl

      .find({ "Client Type": "Client" }, {

        projection: {

          Date: 1,

          "Option Index Call Long": 1,

          "Option Index Call Short": 1,

          "Option Index Put Long": 1,

          "Option Index Put Short": 1,

          _id: 0

        }

      })

      .sort({ Date: 1 })

      .toArray();

 

    const niftyRows = await niftyColl

      .find({}, { projection: { Date: 1, Close: 1, _id: 0 } })

      .toArray();

 

    const niftyMap: Record<string, number> = {};

    niftyRows.forEach(nifty => {

      const norm = dayjs(nifty.Date, "DD-MMM-YYYY").format("YYYY-MM-DD");

      niftyMap[norm] = Number(nifty.Close);

    });

 

    const result = rows.map(row => ({

      Date: row.Date,

      Client_Call_OI:

        (row["Option Index Call Long"] || 0) - (row["Option Index Call Short"] || 0),

      Client_Put_OI:

        (row["Option Index Put Long"] || 0) - (row["Option Index Put Short"] || 0),

      NIFTY_Value: niftyMap[row.Date] ?? null,

    }));

 

    res.json(result);

  } catch (err) {

    console.error("Error in /api/OIClient_Index_Opt/data:", err);

    res.status(500).json({ error: "Internal Server Error" });

  }

});

 

// Fetch Client Futures OI & Nifty value

app.get("/api/OIClient_Index_Fut/data", async (req, res) => {

  try {

    const nseColl = db.collection("nse");

    const niftyColl = db.collection("Nifty");

 

    const rows = await nseColl

      .find({ "Client Type": "Client" }, {

        projection: {

          Date: 1,

          "Future Index Long": 1,

          "Future Index Short": 1,

          _id: 0

        }

      })

      .sort({ Date: 1 })

      .toArray();

 

    const niftyRows = await niftyColl

      .find({}, { projection: { Date: 1, Close: 1, _id: 0 } })

      .toArray();

 

    const niftyMap: Record<string, number> = {};

    niftyRows.forEach(nifty => {

      const norm = dayjs(nifty.Date, "DD-MMM-YYYY").format("YYYY-MM-DD");

      niftyMap[norm] = Number(nifty.Close);

    });

 

    const result = rows.map(row => ({

      Date: row.Date,

      Client_Futures_OI:

        (row["Future Index Long"] || 0) - (row["Future Index Short"] || 0),

      NIFTY_Value: niftyMap[row.Date] ?? null,

    }));

   

    res.json(result);

  } catch (err) {

    console.error("Error in /api/OIClient_Index_Fut/data:", err);

    res.status(500).json({ error: "Internal Server Error" });

  }

});

 

// Fetch all available distinct dates from NSE (excluding "TOTAL")

app.get("/available-dates", async (req, res) => {

  try {

    const nseColl = db.collection("nse");

    const dates = await nseColl.distinct("Date", { "Client Type": { $ne: "TOTAL" } });

    dates.sort(); // Sort lexicographically (works for "YYYY-MM-DD")

    res.json(dates);

  } catch (err) {

    console.error("Error in /available-dates:", err);

    res.status(500).json({ error: "Internal Server Error" });

  }

});

 

// Summary route: compare selectedDate vs previous available date (excluding "TOTAL")

app.get("/summary", (req: Request, res: Response): void => {
  const selectedDate = req.query.date;

  if (!selectedDate) {
    res.status(400).json({ error: 'Missing "date" query parameter' });
    return;
  }

  const nseColl = db.collection("nse");

  // Fetch all rows for the selected date (excluding "TOTAL")
  nseColl
    .find({ Date: selectedDate, "Client Type": { $ne: "TOTAL" } })
    .toArray()
    .then(rows => {
      if (rows.length === 0) {
        res.json({ available: false });
        return;
      }

      const currentNetOI: Record<
        string,
        { "Index Futures": number; "Stock Futures": number; "Index Options": number }
      > = {};

      rows.forEach(row => {
        const participant = row["Client Type"];
        if (!currentNetOI[participant]) {
          currentNetOI[participant] = {
            "Index Futures": 0,
            "Stock Futures": 0,
            "Index Options": 0,
          };
        }
        currentNetOI[participant]["Index Futures"] =
          (Number(row["Future Index Long"]) || 0) - (Number(row["Future Index Short"]) || 0);

        currentNetOI[participant]["Stock Futures"] =
          (Number(row["Future Stock Long"]) || 0) - (Number(row["Future Stock Short"]) || 0);

        currentNetOI[participant]["Index Options"] =
          ((Number(row["Option Index Call Long"]) || 0) - (Number(row["Option Index Call Short"]) || 0)) -
          ((Number(row["Option Index Put Long"]) || 0) - (Number(row["Option Index Put Short"]) || 0));
      });

      let prevNetOI: Record<
        string,
        { "Index Futures": number; "Stock Futures": number; "Index Options": number }
      > = {};

      let previousDate: string | undefined;

      const manualPrevData = {
        "FII": { "Index Futures": -86592, "Stock Futures": 1529000, "Index Options": -73818 },
        "Pro": { "Index Futures": -29643, "Stock Futures": 400000, "Index Options": -66112 },
        "Client": { "Index Futures": 40514, "Stock Futures": 1882000, "Index Options": 213000 },
        "DII": { "Index Futures": 75721, "Stock Futures": -3810000, "Index Options": -73116 }
      };

      if (selectedDate === "2025-04-07") {
        prevNetOI = manualPrevData;
        finishResponse();
      } else {
        nseColl
          .find({ Date: { $lt: selectedDate }, "Client Type": { $ne: "TOTAL" } }, { projection: { Date: 1, _id: 0 } })
          .sort({ Date: -1 })
          .limit(1)
          .toArray()
          .then(prevDocArr => {
            previousDate = prevDocArr[0]?.Date || null;
            if (previousDate) {
              nseColl
                .find({ Date: previousDate, "Client Type": { $ne: "TOTAL" } })
                .toArray()
                .then(prevRows => {
                  prevRows.forEach(row => {
                    const participant = row["Client Type"];
                    if (!prevNetOI[participant]) {
                      prevNetOI[participant] = {
                        "Index Futures": 0,
                        "Stock Futures": 0,
                        "Index Options": 0,
                      };
                    }
                    prevNetOI[participant]["Index Futures"] =
                      (Number(row["Future Index Long"]) || 0) - (Number(row["Future Index Short"]) || 0);

                    prevNetOI[participant]["Stock Futures"] =
                      (Number(row["Future Stock Long"]) || 0) - (Number(row["Future Stock Short"]) || 0);

                    prevNetOI[participant]["Index Options"] =
                      ((Number(row["Option Index Call Long"]) || 0) - (Number(row["Option Index Call Short"]) || 0)) -
                      ((Number(row["Option Index Put Long"]) || 0) - (Number(row["Option Index Put Short"]) || 0));
                  });
                  finishResponse();
                })
                .catch(err => {
                  console.error("Error fetching previous rows:", err);
                  res.status(500).json({ error: "Internal Server Error" });
                });
            } else {
              // No previous date found, treat as zeros
              finishResponse();
            }
          })
          .catch(err => {
            console.error("Error fetching previous date:", err);
            res.status(500).json({ error: "Internal Server Error" });
          });
      }

      // Final response builder function
      function finishResponse() {
        const response = Object.entries(currentNetOI).map(([participant, segments]) => {
          const rows = Object.entries(segments).map(([segment, netOI]) => {
            const prev = prevNetOI[participant]?.[
              segment as "Index Futures" | "Stock Futures" | "Index Options"
            ] || 0;
            const change = netOI - prev;
            return { segment, netOI, change };
          });
          return { participant, rows };
        });
        res.json({ available: true, data: response });
      }
    })
    .catch(err => {
      console.error("Error in /summary:", err);
      res.status(500).json({ error: "Internal Server Error" });
    });
});

 

// Fetch combined market data (combined_market_chart2)

app.get("/api/market-data", async (req, res) => {

  try {

    const coll = db.collection("combined_market_chart2");

    const rows = await coll.find({}).sort({ Date: 1 }).toArray();

    res.json(rows);

  } catch (err) {

    console.error("Error in /api/market-data:", err);

    res.status(500).send("Internal Server Error");

  }

});

 

// Fetch merged Net OI data across FII/Client/Nifty/Cash

app.get("/api/net-oi", async (req, res) => {

  try {

    const nseColl = db.collection("nse");

    const niftyColl = db.collection("Nifty");

    const cashColl = db.collection("cash_data_nse");

 

    // Fetch all FII & Client rows

    const nseData = await nseColl

      .find({ "Client Type": { $in: ["FII", "Client"] } })

      .toArray();

 

    // Fetch all Nifty rows

    const niftyData = await niftyColl.find({}).toArray();

 

    // Fetch all cash data for FII/FPI * and DII **

    const cashData = await cashColl

      .find({ CATEGORY: { $in: ["FII/FPI *", "DII **"] } })

      .sort({ DATE: 1 }) // optional

      .toArray();

 

    // const netOIMap = {};

    const netOIMap: Record<
  string,
  {
    date: string;
    FII_Index_Futures: number;
    FII_Index_Options: number;
    Client_Index_Futures: number;
    Client_Index_Options: number;
    Nifty_Close: number;
    FII_Cash_Net: number;
    DII_Cash_Net: number;
  }
> = {};

 

    // Process NSE data (FII & Client)

    nseData.forEach(row => {

      // const date = normalizeDate(row["Date"], "nse");

      const raw = normalizeDate(row["Date"], "nse");
if (!raw) return; // or `continue` if you’re in a `for`‌loop
const date = raw;

      const clientType = row["Client Type"];

 

      if (!netOIMap[date]) {

        netOIMap[date] = {

          date,

          FII_Index_Futures: 0,

          FII_Index_Options: 0,

          Client_Index_Futures: 0,

          Client_Index_Options: 0,

          Nifty_Close: 0,

          FII_Cash_Net: 0,

          DII_Cash_Net: 0

        };

      }

 

      const indexFutures =

        (row["Future Index Long"] || 0) - (row["Future Index Short"] || 0);

      const indexOptions =

        ((row["Option Index Call Long"] || 0) - (row["Option Index Call Short"] || 0)) -

        ((row["Option Index Put Long"] || 0) - (row["Option Index Put Short"] || 0));

 

      if (clientType === "FII") {

        netOIMap[date].FII_Index_Futures = indexFutures;

        netOIMap[date].FII_Index_Options = indexOptions;

      } else if (clientType === "Client") {

        netOIMap[date].Client_Index_Futures = indexFutures;

        netOIMap[date].Client_Index_Options = indexOptions;

      }

    });

 

    // Process Nifty data

     niftyData.forEach(row => {
        const raw = normalizeDate(row["Date"], "nifty");
        if (!raw) return;
        const date = raw;
        if (netOIMap[date]) {
          netOIMap[date].Nifty_Close = Number(row["Close"] || 0);
        }
      });

    
 
    cashData.forEach(row => {
      const raw = normalizeDate(row["DATE"], "cash");
      if (!raw) return;
      const date = raw;
      if (netOIMap[date]) {
        if (row["CATEGORY"] === "FII/FPI *") {
          netOIMap[date].FII_Cash_Net = Number(row["NET VALUE"] || 0);
        } else if (row["CATEGORY"] === "DII **") {
          netOIMap[date].DII_Cash_Net = Number(row["NET VALUE"] || 0);
        }
      }
    });
 

    // Convert map to sorted array by actual date

    const mergedData = Object.values(netOIMap).sort((a, b) => {

      return (

        dayjs(a.date, "DD-MM-YYYY").toDate() - dayjs(b.date, "DD-MM-YYYY").toDate()

      );

    });

 

    res.json(mergedData);

  } catch (err) {

    console.error("Error in /api/net-oi:", err);

    res.status(500).send("Internal Server Error");

  }

});

// ---------------------------------------------------------------------------------------------




// Error handling middleware (must be after routes)
app.use((err: Error, req: Request, res: Response, next: NextFunction) => {
  errorMiddleware(err, req, res, next);
});

// Socket.IO setup
const io = new SocketIOServer(httpServer, {
  cors: {
    origin: process.env.CLIENT_URL || 'http://localhost:5173',
    methods: ['GET', 'POST']
  },
  connectionStateRecovery: {
    maxDisconnectionDuration: 2 * 60 * 1000, // 2 minutes
    skipMiddlewares: true
  }
});

// Socket.IO connection handling
io.on('connection', (socket) => {
  console.log('🔌 New client connected:', socket.id);

  socket.on('disconnect', (reason) => {
    console.log(`Client disconnected (${socket.id}):`, reason);
  });

  socket.on('error', (err) => {   
    console.error('Socket error:', err);
  });
});

const PORT = Number(process.env.PORT) || 8000;
const HOST = process.env.HOST || '0.0.0.0';

httpServer.listen(PORT,
  HOST,
  () => {
    console.log(`🚀 Server running at http://${HOST}:${PORT}`);
    console.log(`🔗 Allowed CORS origin: ${process.env.CLIENT_URL || 'http://localhost:5173'}`);
  });


// const PORT = Number(process.env.PORT) || 8000;
// const HOST = process.env.HOST || '0.0.0.0';

// httpServer.listen(PORT,
  // HOST,
  // () => {
    // console.log(`🚀 Server running at http://${HOST}:${PORT}`);
    // console.log(`🔗 Allowed CORS origin: ${process.env.CLIENT_URL || 'http://localhost:5173'}`);
  // });

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('🛑 Shutting down gracefully...');
  await mongoClient.close();
  httpServer.close(() => {
    console.log('Server closed');
    process.exit(0);
  });
});

export { io };