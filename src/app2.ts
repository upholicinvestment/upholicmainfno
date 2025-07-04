import express, { Request, Response, NextFunction } from 'express';
import { MongoClient, Db } from 'mongodb';
import cors from 'cors';
import dotenv from 'dotenv';
import routes from './routes';
import { errorMiddleware } from './middleware/error.middleware';
import { createServer } from 'http';
import { Server as SocketIOServer } from 'socket.io';
import AnalysisRoutes from './api/analysis.api';
import registerNiftyRoutes from './api/call_put.api';
import cash_dataRoutes from './api/cash data.api';
import ClientRoutes from './api/client.api';
import DIIRoutes from './api/dii.api';
import FIIRoutes from './api/fii.api';
import ProRoutes from './api/pro.api';
import summaryRoutes from './api/summary.api';
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
    AnalysisRoutes(app,db)
    registerNiftyRoutes(app,db)
    cash_dataRoutes(app,db)
    ClientRoutes(app,db)
    DIIRoutes(app,db)
    FIIRoutes(app,db)
    ProRoutes(app,db)
    summaryRoutes(app,db)
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