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
app.get('/api/stocks', async (_req, res) => {
  try {
    const securityIds = [
      53454, 53435, 53260, 53321, 53461, 53359, 53302, 53224, 53405, 53343,
      53379, 53251, 53469, 53375, 53311, 53314, 53429, 53252, 53460, 53383,
      53354, 53450, 53466, 53317, 53301, 53480, 53226, 53432, 53352, 53433,
      53241, 53300, 53327, 53258, 53253, 53471, 53398, 53441, 53425, 53369,
      53341, 53250, 53395, 53324, 53316, 53318, 53279, 53245, 53280, 53452
    ]

    const stocks = await db.collection('nse_fno_stock')
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
    const now = new Date();
    const marketOpen = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 9, 15, 0, 0);
    const marketClose = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 15, 30, 59, 999);

    const pipeline = [
      {
        $match: {
          received_at: { $gte: marketOpen, $lte: marketClose },
          type: "Full Data"
        }
      },
      {
        $addFields: {
          slot: {
            $dateTrunc: {
              date: "$received_at",
              unit: "minute",
              binSize: 5,
              timezone: "Asia/Kolkata"
            }
          }
        }
      },
      {
        $sort: { received_at: -1 }
      },
      {
        $group: {
          _id: { slot: "$slot", security_id: "$security_id" },
          latest: { $first: "$$ROOT" }
        }
      },
      {
        $group: {
          _id: "$_id.slot",
          stocks: { $push: "$latest" }
        }
      },
      {
        $sort: { _id: 1 }
      }
    ];

    const result = await db.collection('nse_fno_stock').aggregate(pipeline).toArray();

    const chartData = result
      .filter(slotData => {
        const slotTime = new Date(slotData._id);
        return slotTime <= now;
      })
      .map(slotData => {
        const time = new Date(slotData._id).toLocaleTimeString("en-IN", {
          hour: "2-digit",
          minute: "2-digit",
          hour12: false,
          timeZone: "Asia/Kolkata"
        });

        const stocks = slotData.stocks.slice(0, 220);

        let advances = 0;
        let declines = 0;

        for (const stock of stocks) {
          const ltp = parseFloat(stock.LTP);
          const close = parseFloat(stock.close);
          if (ltp > close) advances++;
          else if (ltp < close) declines++;
        }

        return {
          time,
          advances,
          declines
        };
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
      message: err instanceof Error ? err.message : 'Unknown error',
      details: err
    });
  }
});



app.get('/api/nifty/atm-strikes-timeline', async (req: Request, res: Response): Promise<void> => {
  try {
    const intervalParam = req.query.interval as string || '3';
    const interval = parseInt(intervalParam, 10); // in minutes

    const niftyCollection = db.collection('all_nse_fno');
    const optionChainCollection = db.collection('all_nse_fno');

    const docs = await niftyCollection
      .find({
        security_id: 56785, timestamp: {
          $gte: new Date(new Date().setUTCHours(0, 0, 0, 0)),
          $lt: new Date(new Date().setUTCHours(24, 0, 0, 0))
        }
      }, { projection: { _id: 0, LTP: 1, timestamp: 1 } })
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
          // timestamp: {
          //   $gte: new Date(rounded.getTime()),
          //   $lt: new Date(rounded.getTime() + interval * 60 * 1000)
          // }
          timestamp: {
            $gte: new Date(new Date().setUTCHours(0, 0, 0, 0)),
            $lt: new Date(new Date().setUTCHours(24, 0, 0, 0))
          }
        });

        const optionData2 = await optionChainCollection.findOne({
          strike_price: atmStrike,
          trading_symbol: { $regex: '^NIFTY-Jun2025' },
          option_type: "PE",
          // timestamp: {
          //   $gte: new Date(rounded.getTime()),
          //   $lt: new Date(rounded.getTime() + interval * 60 * 1000)
          // }
          timestamp: {
            $gte: new Date(new Date().setUTCHours(0, 0, 0, 0)),
            $lt: new Date(new Date().setUTCHours(24, 0, 0, 0))
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
    const collection = db.collection('all_nse_fno');
    const latestNifty = await collection.find({
      security_id: 56785, timestamp: {
        $gte: new Date(new Date().setUTCHours(0, 0, 0, 0)),
        $lt: new Date(new Date().setUTCHours(24, 0, 0, 0))
      }
    })
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

    const collection2 = db.collection('all_nse_fno');

    const results = await Promise.all(
      strikePrices.map(async (strike) => {
        const CE_docs = await collection2.find({
          strike_price: strike,
          option_type: "CE",
          trading_symbol: { $regex: '^NIFTY-Jun2025' },
          timestamp: {
            $gte: new Date(new Date().setUTCHours(0, 0, 0, 0)),
            $lt: new Date(new Date().setUTCHours(24, 0, 0, 0))
          }
        }).toArray();

        const PE_docs = await collection2.find({
          strike_price: strike,
          option_type: "PE",
          trading_symbol: { $regex: '^NIFTY-Jun2025' },
          timestamp: {
            $gte: new Date(new Date().setUTCHours(0, 0, 0, 0)),
            $lt: new Date(new Date().setUTCHours(24, 0, 0, 0))
          }
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
    const niftyCollection = db.collection('all_nse_fno');
    const niftyDocs = await niftyCollection
      .find({
        security_id: 56785,
        timestamp: {
          $gte: new Date(new Date().setUTCHours(0, 0, 0, 0)),
          $lt: new Date(new Date().setUTCHours(24, 0, 0, 0))
        }
      }, { projection: { _id: 0, LTP: 1, timestamp: 1 } })
      .sort({ timestamp: 1 })
      .toArray();

    const nifty = niftyDocs.map(doc => ({
      value: doc.LTP,
      timestamp: doc.timestamp,
    }));

    res.json({ atmStrike, overall: flattened, nifty });
  } catch (error) {
    console.error('Error fetching NEAR5:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});
// ---------------------------------------------------------------------------------------------
app.get('/api/nifty/overall', async (req: Request, res: Response): Promise<void> => {
  try {
    const collection = db.collection('all_nse_fno');
    const latestNifty = await collection.find({
      security_id: 56785,
      timestamp: {
        $gte: new Date(new Date().setUTCHours(0, 0, 0, 0)),
        $lt: new Date(new Date().setUTCHours(24, 0, 0, 0))
      }
    })
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

    const collection2 = db.collection('all_nse_fno');

    const results = await Promise.all(
      strikePrices.map(async (strike) => {
        const CE_docs = await collection2.find({
          strike_price: strike,
          option_type: "CE",
          trading_symbol: { $regex: '^NIFTY-Jun2025' },
          timestamp: {
            $gte: new Date(new Date().setUTCHours(0, 0, 0, 0)),
            $lt: new Date(new Date().setUTCHours(24, 0, 0, 0))
          }
        }).toArray();

        const PE_docs = await collection2.find({
          strike_price: strike,
          option_type: "PE",
          trading_symbol: { $regex: '^NIFTY-Jun2025' },
          timestamp: {
            $gte: new Date(new Date().setUTCHours(0, 0, 0, 0)),
            $lt: new Date(new Date().setUTCHours(24, 0, 0, 0))
          }
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
    const niftyCollection = db.collection('all_nse_fno');
    const niftyDocs = await niftyCollection
      .find({
        security_id: 56785,
        timestamp: {
          $gte: new Date(new Date().setUTCHours(0, 0, 0, 0)),
          $lt: new Date(new Date().setUTCHours(24, 0, 0, 0))
        }
      }, { projection: { _id: 0, LTP: 1, timestamp: 1 } })
      .sort({ timestamp: 1 })
      .toArray();

    const nifty = niftyDocs.map(doc => ({
      value: doc.LTP,
      timestamp: doc.timestamp,
    }));

    res.json({ atmStrike, overall: flattened, nifty });
  } catch (error) {
    console.error('Error fetching NEAR5:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});





const securities = [
  { name: "360ONE JUL FUT", security_id: 53003, sector: "Financial Services" },
  { name: "AMBER JUL FUT", security_id: 53027, sector: "Chemicals" },
  { name: "AARTIIND JUL FUT", security_id: 53218, sector: "Chemicals" },
  { name: "ABB JUL FUT", security_id: 53219, sector: "Capital Goods" },
  { name: "ABCAPITAL JUL FUT", security_id: 53220, sector: "Financial Services" },
  { name: "ABFRL JUL FUT", security_id: 53221, sector: "Consumer Discretionary" },
  { name: "ACC JUL FUT", security_id: 53222, sector: "Cement" },
  { name: "ADANIENSOL JUL FUT", security_id: 53223, sector: "Utilities" },
  { name: "ADANIENT JUL FUT", security_id: 53224, sector: "Conglomerate" },
  { name: "ADANIGREEN JUL FUT", security_id: 53225, sector: "Utilities" },
  { name: "ADANIPORTS JUL FUT", security_id: 53226, sector: "Logistics" },
  { name: "ALKEM JUL FUT", security_id: 53227, sector: "Pharmaceuticals" },
  { name: "AMBUJACEM JUL FUT", security_id: 53235, sector: "Cement" },
  { name: "ANGELONE JUL FUT", security_id: 53236, sector: "Financial Services" },
  { name: "APLAPOLLO JUL FUT", security_id: 53240, sector: "Metals" },
  { name: "APOLLOHOSP JUL FUT", security_id: 53241, sector: "Healthcare" },
  { name: "ASHOKLEY JUL FUT", security_id: 53244, sector: "Automotive" },
  { name: "ASIANPAINT JUL FUT", security_id: 53245, sector: "Paints" },
  { name: "ASTRAL JUL FUT", security_id: 53246, sector: "Industrials" },
  { name: "ATGL JUL FUT", security_id: 53247, sector: "Utilities" },
  { name: "AUBANK JUL FUT", security_id: 53248, sector: "Banking" },
  { name: "AUROPHARMA JUL FUT", security_id: 53249, sector: "Pharmaceuticals" },
  { name: "AXISBANK JUL FUT", security_id: 53250, sector: "Banking" },
  { name: "BAJAJ-AUTO JUL FUT", security_id: 53251, sector: "Automotive" },
  { name: "BAJAJFINSV JUL FUT", security_id: 53252, sector: "Financial Services" },
  { name: "BAJFINANCE JUL FUT", security_id: 53253, sector: "Financial Services" },
  { name: "BALKRISIND JUL FUT", security_id: 53254, sector: "Automotive" },
  { name: "BANDHANBNK JUL FUT", security_id: 53255, sector: "Banking" },
  { name: "BANKBARODA JUL FUT", security_id: 53256, sector: "Banking" },
  { name: "BANKINDIA JUL FUT", security_id: 53257, sector: "Banking" },
  { name: "BEL JUL FUT", security_id: 53258, sector: "Defence" },
  { name: "BHARATFORG JUL FUT", security_id: 53259, sector: "Automotive" },
  { name: "BHARTIARTL JUL FUT", security_id: 53260, sector: "Telecom" },
  { name: "BHEL JUL FUT", security_id: 53261, sector: "Capital Goods" },
  { name: "BIOCON JUL FUT", security_id: 53262, sector: "Pharmaceuticals" },
  { name: "BOSCHLTD JUL FUT", security_id: 53263, sector: "Automotive" },
  { name: "BPCL JUL FUT", security_id: 53264, sector: "Oil & Gas" },
  { name: "BRITANNIA JUL FUT", security_id: 53265, sector: "FMCG" },
  { name: "BSE JUL FUT", security_id: 53268, sector: "Financial Services" },
  { name: "BSOFT JUL FUT", security_id: 53269, sector: "IT" },
  { name: "CAMS JUL FUT", security_id: 53270, sector: "Financial Services" },
  { name: "CANBK JUL FUT", security_id: 53273, sector: "Banking" },
  { name: "CDSL JUL FUT", security_id: 53274, sector: "Financial Services" },
  { name: "CESC JUL FUT", security_id: 53275, sector: "Utilities" },
  { name: "CGPOWER JUL FUT", security_id: 53276, sector: "Capital Goods" },
  { name: "CHAMBLFERT JUL FUT", security_id: 53277, sector: "Fertilizers" },
  { name: "CHOLAFIN JUL FUT", security_id: 53278, sector: "Financial Services" },
  { name: "CIPLA JUL FUT", security_id: 53279, sector: "Pharmaceuticals" },
  { name: "COALINDIA JUL FUT", security_id: 53280, sector: "Metals" },
  { name: "COFORGE JUL FUT", security_id: 53281, sector: "IT" },
  { name: "COLPAL JUL FUT", security_id: 53284, sector: "FMCG" },
  { name: "CONCOR JUL FUT", security_id: 53286, sector: "Logistics" },
  { name: "CROMPTON JUL FUT", security_id: 53289, sector: "Consumer Durables" },
  { name: "CUMMINSIND JUL FUT", security_id: 53290, sector: "Capital Goods" },
  { name: "CYIENT JUL FUT", security_id: 53291, sector: "IT" },
  { name: "DABUR JUL FUT", security_id: 53292, sector: "FMCG" },
  { name: "DALBHARAT JUL FUT", security_id: 53293, sector: "Cement" },
  { name: "DELHIVERY JUL FUT", security_id: 53294, sector: "Logistics" },
  { name: "DIVISLAB JUL FUT", security_id: 53295, sector: "Pharmaceuticals" },
  { name: "DIXON JUL FUT", security_id: 53296, sector: "Consumer Durables" },
  { name: "DLF JUL FUT", security_id: 53297, sector: "Real Estate" },
  { name: "DMART JUL FUT", security_id: 53298, sector: "Retail" },
  { name: "DRREDDY JUL FUT", security_id: 53299, sector: "Pharmaceuticals" },
  { name: "EICHERMOT JUL FUT", security_id: 53300, sector: "Automotive" },
  { name: "ETERNAL JUL FUT", security_id: 53302, sector: "Healthcare" },
  { name: "EXIDEIND JUL FUT", security_id: 53303, sector: "Automotive" },
  { name: "FEDERALBNK JUL FUT", security_id: 53304, sector: "Banking" },
  { name: "GAIL JUL FUT", security_id: 53305, sector: "Oil & Gas" },
  { name: "GLENMARK JUL FUT", security_id: 53306, sector: "Pharmaceuticals" },
  { name: "GMRAIRPORT JUL FUT", security_id: 53307, sector: "Logistics" },
  { name: "GODREJCP JUL FUT", security_id: 53308, sector: "FMCG" },
  { name: "GODREJPROP JUL FUT", security_id: 53309, sector: "Real Estate" },
  { name: "GRANULES JUL FUT", security_id: 53310, sector: "Pharmaceuticals" },
  { name: "GRASIM JUL FUT", security_id: 53311, sector: "Cement" },
  { name: "HAL JUL FUT", security_id: 53312, sector: "Defence" },
  { name: "HAVELLS JUL FUT", security_id: 53313, sector: "Consumer Durables" },
  { name: "HCLTECH JUL FUT", security_id: 53314, sector: "IT" },
  { name: "HDFCAMC JUL FUT", security_id: 53315, sector: "Financial Services" },
  { name: "HDFCBANK JUL FUT", security_id: 53316, sector: "Banking" },
  { name: "HDFCLIFE JUL FUT", security_id: 53317, sector: "Insurance" },
  { name: "HEROMOTOCO JUL FUT", security_id: 53318, sector: "Automotive" },
  { name: "HFCL JUL FUT", security_id: 53319, sector: "Telecom" },
  { name: "HINDALCO JUL FUT", security_id: 53321, sector: "Metals" },
  { name: "HINDCOPPER JUL FUT", security_id: 53322, sector: "Metals" },
  { name: "HINDPETRO JUL FUT", security_id: 53323, sector: "Oil & Gas" },
  { name: "HINDUNILVR JUL FUT", security_id: 53324, sector: "FMCG" },
  { name: "HINDZINC JUL FUT", security_id: 53325, sector: "Metals" },
  { name: "HUDCO JUL FUT", security_id: 53326, sector: "Financial Services" },
  { name: "ICICIBANK JUL FUT", security_id: 53327, sector: "Banking" },
  { name: "ICICIGI JUL FUT", security_id: 53328, sector: "Insurance" },
  { name: "ICICIPRULI JUL FUT", security_id: 53329, sector: "Insurance" },
  { name: "IDEA JUL FUT", security_id: 53330, sector: "Telecom" },
  { name: "IDFCFIRSTB JUL FUT", security_id: 53334, sector: "Banking" },
  { name: "IEX JUL FUT", security_id: 53335, sector: "Utilities" },
  { name: "IGL JUL FUT", security_id: 53336, sector: "Oil & Gas" },
  { name: "IIFL JUL FUT", security_id: 53337, sector: "Financial Services" },
  { name: "INDHOTEL JUL FUT", security_id: 53338, sector: "Hospitality" },
  { name: "INDIANB JUL FUT", security_id: 53339, sector: "Banking" },
  { name: "INDIGO JUL FUT", security_id: 53340, sector: "Aviation" },
  { name: "INDUSINDBK JUL FUT", security_id: 53341, sector: "Banking" },
  { name: "INDUSTOWER JUL FUT", security_id: 53342, sector: "Telecom" },
  { name: "INFY JUL FUT", security_id: 53343, sector: "IT" },
  { name: "INOXWIND JUL FUT", security_id: 53344, sector: "Capital Goods" },
  { name: "IOC JUL FUT", security_id: 53345, sector: "Oil & Gas" },
  { name: "IRB JUL FUT", security_id: 53346, sector: "Infrastructure" },
  { name: "IRCTC JUL FUT", security_id: 53347, sector: "Tourism" },
  { name: "IREDA JUL FUT", security_id: 53348, sector: "Financial Services" },
  { name: "IRFC JUL FUT", security_id: 53351, sector: "Financial Services" },
  { name: "ITC JUL FUT", security_id: 53352, sector: "FMCG" },
  { name: "JINDALSTEL JUL FUT", security_id: 53353, sector: "Metals" },
  { name: "JIOFIN JUL FUT", security_id: 53354, sector: "Financial Services" },
  { name: "JSL JUL FUT", security_id: 53355, sector: "Metals" },
  { name: "JSWENERGY JUL FUT", security_id: 53358, sector: "Utilities" },
  { name: "JSWSTEEL JUL FUT", security_id: 53359, sector: "Metals" },
  { name: "JUBLFOOD JUL FUT", security_id: 53366, sector: "Quick Service Restaurant" },
  { name: "KALYANKJIL JUL FUT", security_id: 53367, sector: "Retail" },
  { name: "KEI JUL FUT", security_id: 53368, sector: "Capital Goods" },
  { name: "KOTAKBANK JUL FUT", security_id: 53369, sector: "Banking" },
  { name: "KPITTECH JUL FUT", security_id: 53370, sector: "IT" },
  { name: "LAURUSLABS JUL FUT", security_id: 53371, sector: "Pharmaceuticals" },
  { name: "LICHSGFIN JUL FUT", security_id: 53372, sector: "Financial Services" },
  { name: "LICI JUL FUT", security_id: 53373, sector: "Insurance" },
  { name: "LODHA JUL FUT", security_id: 53374, sector: "Real Estate" },
  { name: "LT JUL FUT", security_id: 53375, sector: "Infrastructure" },
  { name: "LTF JUL FUT", security_id: 53376, sector: "Financial Services" },
  { name: "LTIM JUL FUT", security_id: 53377, sector: "IT" },
  { name: "LUPIN JUL FUT", security_id: 53378, sector: "Pharmaceuticals" },
  { name: "M&M JUL FUT", security_id: 53379, sector: "Automotive" },
  { name: "M&MFIN JUL FUT", security_id: 53380, sector: "Financial Services" },
  { name: "MANAPPURAM JUL FUT", security_id: 53381, sector: "Financial Services" },
  { name: "MARICO JUL FUT", security_id: 53382, sector: "FMCG" },
  { name: "MARUTI JUL FUT", security_id: 53383, sector: "Automotive" },
  { name: "MAXHEALTH JUL FUT", security_id: 53384, sector: "Healthcare" },
  { name: "MCX JUL FUT", security_id: 53385, sector: "Financial Services" },
  { name: "MFSL JUL FUT", security_id: 53386, sector: "Insurance" },
  { name: "MGL JUL FUT", security_id: 53387, sector: "Oil & Gas" },
  { name: "MOTHERSON JUL FUT", security_id: 53388, sector: "Automotive" },
  { name: "MPHASIS JUL FUT", security_id: 53389, sector: "IT" },
  { name: "MUTHOOTFIN JUL FUT", security_id: 53390, sector: "Financial Services" },
  { name: "NATIONALUM JUL FUT", security_id: 53391, sector: "Metals" },
  { name: "NAUKRI JUL FUT", security_id: 53392, sector: "IT" },
  { name: "NBCC JUL FUT", security_id: 53393, sector: "Construction" },
  { name: "NCC JUL FUT", security_id: 53394, sector: "Construction" },
  { name: "NESTLEIND JUL FUT", security_id: 53395, sector: "FMCG" },
  { name: "NHPC JUL FUT", security_id: 53396, sector: "Utilities" },
  { name: "NMDC JUL FUT", security_id: 53397, sector: "Metals" },
  { name: "NTPC JUL FUT", security_id: 53398, sector: "Utilities" },
  { name: "NYKAA JUL FUT", security_id: 53399, sector: "Retail" },
  { name: "OBEROIRLTY JUL FUT", security_id: 53402, sector: "Real Estate" },
  { name: "OFSS JUL FUT", security_id: 53403, sector: "IT" },
  { name: "OIL JUL FUT", security_id: 53404, sector: "Oil & Gas" },
  { name: "ONGC JUL FUT", security_id: 53405, sector: "Oil & Gas" },
  { name: "PAGEIND JUL FUT", security_id: 53406, sector: "Textiles" },
  { name: "PATANJALI JUL FUT", security_id: 53407, sector: "FMCG" },
  { name: "PAYTM JUL FUT", security_id: 53408, sector: "IT" },
  { name: "PEL JUL FUT", security_id: 53409, sector: "Financial Services" },
  { name: "PERSISTENT JUL FUT", security_id: 53413, sector: "IT" },
  { name: "PETRONET JUL FUT", security_id: 53414, sector: "Oil & Gas" },
  { name: "PFC JUL FUT", security_id: 53415, sector: "Financial Services" },
  { name: "PHOENIXLTD JUL FUT", security_id: 53416, sector: "Real Estate" },
  { name: "PIDILITIND JUL FUT", security_id: 53418, sector: "Chemicals" },
  { name: "PIIND JUL FUT", security_id: 53419, sector: "Chemicals" },
  { name: "PNB JUL FUT", security_id: 53420, sector: "Banking" },
  { name: "PNBHOUSING JUL FUT", security_id: 53421, sector: "Financial Services" },
  { name: "POLICYBZR JUL FUT", security_id: 53422, sector: "IT" },
  { name: "POLYCAB JUL FUT", security_id: 53423, sector: "Capital Goods" },
  { name: "POONAWALLA JUL FUT", security_id: 53424, sector: "Financial Services" },
  { name: "POWERGRID JUL FUT", security_id: 53425, sector: "Utilities" },
  { name: "PRESTIGE JUL FUT", security_id: 53426, sector: "Real Estate" },
  { name: "RBLBANK JUL FUT", security_id: 53427, sector: "Banking" },
  { name: "RECLTD JUL FUT", security_id: 53428, sector: "Financial Services" },
  { name: "RELIANCE JUL FUT", security_id: 53429, sector: "Conglomerate" },
  { name: "SAIL JUL FUT", security_id: 53430, sector: "Metals" },
  { name: "SBICARD JUL FUT", security_id: 53431, sector: "Financial Services" },
  { name: "SBILIFE JUL FUT", security_id: 53432, sector: "Insurance" },
  { name: "SBIN JUL FUT", security_id: 53433, sector: "Banking" },
  { name: "SHREECEM JUL FUT", security_id: 53434, sector: "Cement" },
  { name: "SHRIRAMFIN JUL FUT", security_id: 53435, sector: "Financial Services" },
  { name: "SIEMENS JUL FUT", security_id: 53436, sector: "Capital Goods" },
  { name: "SJVN JUL FUT", security_id: 53437, sector: "Utilities" },
  { name: "SOLARINDS JUL FUT", security_id: 53438, sector: "Chemicals" },
  { name: "SONACOMS JUL FUT", security_id: 53439, sector: "Automotive" },
  { name: "SRF JUL FUT", security_id: 53440, sector: "Chemicals" },
  { name: "SUNPHARMA JUL FUT", security_id: 53441, sector: "Pharmaceuticals" },
  { name: "SUPREMEIND JUL FUT", security_id: 53442, sector: "Consumer Durables" },
  { name: "SYNGENE JUL FUT", security_id: 53443, sector: "Pharmaceuticals" },
  { name: "TATACHEM JUL FUT", security_id: 53448, sector: "Chemicals" },
  { name: "TATACOMM JUL FUT", security_id: 53449, sector: "Telecom" },
  { name: "TATACONSUM JUL FUT", security_id: 53450, sector: "FMCG" },
  { name: "TATAELXSI JUL FUT", security_id: 53451, sector: "IT" },
  { name: "TATAMOTORS JUL FUT", security_id: 53452, sector: "Automotive" },
  { name: "TATAPOWER JUL FUT", security_id: 53453, sector: "Utilities" },
  { name: "TATASTEEL JUL FUT", security_id: 53454, sector: "Metals" },
  { name: "TATATECH JUL FUT", security_id: 53455, sector: "IT" },
  { name: "TCS JUL FUT", security_id: 53460, sector: "IT" },
  { name: "TECHM JUL FUT", security_id: 53461, sector: "IT" },
  { name: "TIINDIA JUL FUT", security_id: 53464, sector: "Automotive" },
  { name: "TITAGARH JUL FUT", security_id: 53465, sector: "Capital Goods" },
  { name: "TITAN JUL FUT", security_id: 53466, sector: "Consumer Discretionary" },
  { name: "TORNTPHARM JUL FUT", security_id: 53467, sector: "Pharmaceuticals" },
  { name: "TORNTPOWER JUL FUT", security_id: 53468, sector: "Utilities" },
  { name: "TRENT JUL FUT", security_id: 53469, sector: "Retail" },
  { name: "TVSMOTOR JUL FUT", security_id: 53470, sector: "Automotive" },
  { name: "ULTRACEMCO JUL FUT", security_id: 53471, sector: "Cement" },
  { name: "UNIONBANK JUL FUT", security_id: 53472, sector: "Banking" },
  { name: "UNITDSPR JUL FUT", security_id: 53473, sector: "FMCG" },
  { name: "UPL JUL FUT", security_id: 53474, sector: "Chemicals" },
  { name: "VBL JUL FUT", security_id: 53475, sector: "FMCG" },
  { name: "VEDL JUL FUT", security_id: 53478, sector: "Metals" },
  { name: "VOLTAS JUL FUT", security_id: 53479, sector: "Consumer Durables" },
  { name: "WIPRO JUL FUT", security_id: 53480, sector: "IT" },
  { name: "YESBANK JUL FUT", security_id: 53481, sector: "Banking" },
  { name: "ZYDUSLIFE JUL FUT", security_id: 53484, sector: "Pharmaceuticals" },
  { name: "PGEL JUL FUT", security_id: 53763, sector: "Utilities" },
  { name: "BDL JUL FUT", security_id: 64225, sector: "Defence" },
  { name: "BLUESTARCO JUL FUT", security_id: 64233, sector: "Consumer Durables" },
  { name: "FORTIS JUL FUT", security_id: 64412, sector: "Healthcare" },
  { name: "KAYNES JUL FUT", security_id: 64624, sector: "IT" },
  { name: "MANKIND JUL FUT", security_id: 64901, sector: "Pharmaceuticals" },
  { name: "MAZDOCK JUL FUT", security_id: 64907, sector: "Defence" },
  { name: "PPLPHARMA JUL FUT", security_id: 64988, sector: "Pharmaceuticals" },
  { name: "RVNL JUL FUT", security_id: 64997, sector: "Infrastructure" },
  { name: "UNOMINDA JUL FUT", security_id: 65239, sector: "Automotive" }
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
    const collection = db.collection('nse_fno_stock');
    
    // Pre-process security data for faster lookups
    const securityIdMap = new Map<string, { name: string; sector: string }>();
    const securityIdStrings = new Set<string>();
    
    securities.forEach(sec => {
      const idStr = sec.security_id.toString();
      securityIdMap.set(idStr, { 
        name: sec.name, 
        sector: sec.sector 
      });
      securityIdStrings.add(idStr);
    });

    // Optimized aggregation pipeline
    const pipeline = [
      {
        $match: {
          security_id: { $in: Array.from(securityIdStrings) }
        }
      },
      {
        $sort: { received_at: -1 }
      },
      {
        $group: {
          _id: "$security_id",
          latestDoc: { $first: "$$ROOT" }
        }
      },
      {
        $replaceRoot: { newRoot: "$latestDoc" }
      },
      {
        $project: {
          security_id: 1,
          LTP: 1,
          close: 1,
          received_at: 1,
          // Include any other fields you need
        }
      }
    ];

    // Use cursor for better memory efficiency
    const cursor = collection.aggregate(pipeline);
    const items = await cursor.toArray();

    // Process items in a single pass
    const processedItems = items.map(item => {
      const securityInfo = securityIdMap.get(item.security_id.toString());
      const ltp = parseFloat(item.LTP);
      const close = parseFloat(item.close);
      const change = !isNaN(ltp) && !isNaN(close) 
        ? ((ltp - close) / close) * 100 
        : undefined;

      return {
        ...item,
        trading_symbol: securityInfo?.name || '',
        sector: securityInfo?.sector || 'Unknown',
        change: change
      };
    });

    // Cache control with conditional GET support
    res.setHeader('Cache-Control', 'public, max-age=60, stale-while-revalidate=30');
    res.setHeader('Vary', 'Accept-Encoding');
    res.json(processedItems);
  } catch (error) {
    console.error('Error fetching heatmap data:', error);
    res.status(500).json({
      error: 'Internal server error',
      details: error instanceof Error ? error.message : 'Unknown error'
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


// const PORT = Number(process.env.PORT) || 8000;
// const HOST = process.env.HOST || '0.0.0.0';

// httpServer.listen(PORT,
//   HOST,
//   () => {
//     console.log(`🚀 Server running at http://${HOST}:${PORT}`);
//     console.log(`🔗 Allowed CORS origin: ${process.env.CLIENT_URL || 'http://localhost:5173'}`);
//   });


const PORT = Number(process.env.PORT) || 8000;
// const HOST = process.env.HOST || '0.0.0.0';

httpServer.listen(PORT,
  // HOST,
  () => {
    // console.log(`🚀 Server running at http://${HOST}:${PORT}`);
    console.log(`🔗 Allowed CORS origin: ${process.env.CLIENT_URL || 'http://localhost:5173'}`);
  });

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