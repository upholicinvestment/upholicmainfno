// src/api/heatmap.ts
import { Express, Request, Response } from "express";
import { Db } from "mongodb";
import crypto from "crypto";

type SecurityMeta = { name: string; sector: string };

type StockData = {
  _id: string;
  trading_symbol: string;
  LTP: string;
  close: string;
  sector: string;
  security_id: number;
  change?: number;
  received_at?: string;
};

function buildETag(identity: unknown) {
  return `"heatmap-${crypto
    .createHash("md5")
    .update(JSON.stringify(identity))
    .digest("hex")}"`;
}

const securities: { name: string; security_id: number; sector: string }[] = [
  {
    name: "SAMMAANCAP OCT FUT",
    security_id: 49082,
    sector: "Financial Services",
  },
  { name: "POWERINDIA OCT FUT", security_id: 49974, sector: "Capital Goods" },
  { name: "360ONE OCT FUT", security_id: 52170, sector: "Financial Services" },
  { name: "ABB OCT FUT", security_id: 52171, sector: "Capital Goods" },
  {
    name: "ABCAPITAL OCT FUT",
    security_id: 52172,
    sector: "Financial Services",
  },
  { name: "ADANIENSOL OCT FUT", security_id: 52173, sector: "Utilities" },
  { name: "ADANIENT OCT FUT", security_id: 52174, sector: "Conglomerate" },
  { name: "ADANIGREEN OCT FUT", security_id: 52175, sector: "Utilities" },
  { name: "ADANIPORTS OCT FUT", security_id: 52176, sector: "Logistics" },
  { name: "ALKEM OCT FUT", security_id: 52177, sector: "Pharmaceuticals" },
  { name: "AMBER OCT FUT", security_id: 52178, sector: "Chemicals" },
  { name: "AMBUJACEM OCT FUT", security_id: 52179, sector: "Cement" },
  {
    name: "ANGELONE OCT FUT",
    security_id: 52200,
    sector: "Financial Services",
  },
  { name: "APLAPOLLO OCT FUT", security_id: 52201, sector: "Metals" },
  { name: "APOLLOHOSP OCT FUT", security_id: 52214, sector: "Healthcare" },
  { name: "ASHOKLEY OCT FUT", security_id: 52215, sector: "Automotive" },
  { name: "ASIANPAINT OCT FUT", security_id: 52216, sector: "Paints" },
  { name: "ASTRAL OCT FUT", security_id: 52217, sector: "Industrials" },
  { name: "AUBANK OCT FUT", security_id: 52218, sector: "Banking" },
  { name: "AUROPHARMA OCT FUT", security_id: 52219, sector: "Pharmaceuticals" },
  { name: "AXISBANK OCT FUT", security_id: 52223, sector: "Banking" },
  { name: "BAJAJ-AUTO OCT FUT", security_id: 52224, sector: "Automotive" },
  {
    name: "BAJAJFINSV OCT FUT",
    security_id: 52240,
    sector: "Financial Services",
  },
  {
    name: "BAJFINANCE OCT FUT",
    security_id: 52241,
    sector: "Financial Services",
  },
  { name: "BANDHANBNK OCT FUT", security_id: 52255, sector: "Banking" },
  { name: "BANKBARODA OCT FUT", security_id: 52256, sector: "Banking" },
  { name: "BANKINDIA OCT FUT", security_id: 52266, sector: "Banking" },
  { name: "BDL OCT FUT", security_id: 52267, sector: "Defence" },
  { name: "BEL OCT FUT", security_id: 52274, sector: "Defence" },
  { name: "BHARATFORG OCT FUT", security_id: 52275, sector: "Automotive" },
  { name: "BHARTIARTL OCT FUT", security_id: 52276, sector: "Telecom" },
  { name: "BHEL OCT FUT", security_id: 52277, sector: "Capital Goods" },
  { name: "BIOCON OCT FUT", security_id: 52288, sector: "Pharmaceuticals" },
  {
    name: "BLUESTARCO OCT FUT",
    security_id: 52289,
    sector: "Consumer Durables",
  },
  { name: "BOSCHLTD OCT FUT", security_id: 52290, sector: "Automotive" },
  { name: "BPCL OCT FUT", security_id: 52291, sector: "Oil & Gas" },
  { name: "BRITANNIA OCT FUT", security_id: 52292, sector: "FMCG" },
  { name: "BSE OCT FUT", security_id: 52301, sector: "Financial Services" },
  { name: "CAMS OCT FUT", security_id: 52302, sector: "Financial Services" },
  { name: "CANBK OCT FUT", security_id: 52303, sector: "Banking" },
  { name: "CDSL OCT FUT", security_id: 52304, sector: "Financial Services" },
  { name: "CGPOWER OCT FUT", security_id: 52305, sector: "Capital Goods" },
  {
    name: "CHOLAFIN OCT FUT",
    security_id: 52306,
    sector: "Financial Services",
  },
  { name: "CIPLA OCT FUT", security_id: 52307, sector: "Pharmaceuticals" },
  { name: "COALINDIA OCT FUT", security_id: 52308, sector: "Metals" },
  { name: "COFORGE OCT FUT", security_id: 52309, sector: "IT" },
  { name: "COLPAL OCT FUT", security_id: 52310, sector: "FMCG" },
  { name: "CONCOR OCT FUT", security_id: 52311, sector: "Logistics" },
  { name: "CROMPTON OCT FUT", security_id: 52314, sector: "Consumer Durables" },
  { name: "CUMMINSIND OCT FUT", security_id: 52315, sector: "Capital Goods" },
  { name: "CYIENT OCT FUT", security_id: 52316, sector: "IT" },
  { name: "DABUR OCT FUT", security_id: 52317, sector: "FMCG" },
  { name: "DALBHARAT OCT FUT", security_id: 52318, sector: "Cement" },
  { name: "DELHIVERY OCT FUT", security_id: 52319, sector: "Logistics" },
  { name: "DIVISLAB OCT FUT", security_id: 52326, sector: "Pharmaceuticals" },
  { name: "DIXON OCT FUT", security_id: 52327, sector: "Consumer Durables" },
  { name: "DLF OCT FUT", security_id: 52328, sector: "Real Estate" },
  { name: "DMART OCT FUT", security_id: 52329, sector: "Retail" },
  { name: "DRREDDY OCT FUT", security_id: 52336, sector: "Pharmaceuticals" },
  { name: "EICHERMOT OCT FUT", security_id: 52337, sector: "Automotive" },
  { name: "ETERNAL OCT FUT", security_id: 52338, sector: "Healthcare" },
  { name: "EXIDEIND OCT FUT", security_id: 52339, sector: "Automotive" },
  { name: "FEDERALBNK OCT FUT", security_id: 52340, sector: "Banking" },
  { name: "FORTIS OCT FUT", security_id: 52341, sector: "Healthcare" },
  { name: "GAIL OCT FUT", security_id: 52342, sector: "Oil & Gas" },
  { name: "GLENMARK OCT FUT", security_id: 52343, sector: "Pharmaceuticals" },
  { name: "GMRAIRPORT OCT FUT", security_id: 52344, sector: "Logistics" },
  { name: "GODREJCP OCT FUT", security_id: 52345, sector: "FMCG" },
  { name: "GODREJPROP OCT FUT", security_id: 52350, sector: "Real Estate" },
  { name: "GRASIM OCT FUT", security_id: 52351, sector: "Cement" },
  { name: "HAL OCT FUT", security_id: 52352, sector: "Defence" },
  { name: "HAVELLS OCT FUT", security_id: 52353, sector: "Consumer Durables" },
  { name: "HCLTECH OCT FUT", security_id: 52362, sector: "IT" },
  { name: "HDFCAMC OCT FUT", security_id: 52363, sector: "Financial Services" },
  { name: "HDFCBANK OCT FUT", security_id: 52364, sector: "Banking" },
  { name: "HDFCLIFE OCT FUT", security_id: 52365, sector: "Insurance" },
  { name: "HEROMOTOCO OCT FUT", security_id: 52366, sector: "Automotive" },
  { name: "HFCL OCT FUT", security_id: 52367, sector: "Telecom" },
  { name: "HINDALCO OCT FUT", security_id: 52368, sector: "Metals" },
  { name: "HINDPETRO OCT FUT", security_id: 52369, sector: "Oil & Gas" },
  { name: "HINDUNILVR OCT FUT", security_id: 52370, sector: "FMCG" },
  { name: "HINDZINC OCT FUT", security_id: 52371, sector: "Metals" },
  { name: "HUDCO OCT FUT", security_id: 52372, sector: "Financial Services" },
  { name: "ICICIBANK OCT FUT", security_id: 52374, sector: "Banking" },
  { name: "ICICIGI OCT FUT", security_id: 52375, sector: "Insurance" },
  { name: "ICICIPRULI OCT FUT", security_id: 52378, sector: "Insurance" },
  { name: "IDEA OCT FUT", security_id: 52379, sector: "Telecom" },
  { name: "IDFCFIRSTB OCT FUT", security_id: 52380, sector: "Banking" },
  { name: "IEX OCT FUT", security_id: 52381, sector: "Utilities" },
  { name: "IGL OCT FUT", security_id: 52382, sector: "Oil & Gas" },
  { name: "IIFL OCT FUT", security_id: 52383, sector: "Financial Services" },
  { name: "INDHOTEL OCT FUT", security_id: 52384, sector: "Hospitality" },
  { name: "INDIANB OCT FUT", security_id: 52390, sector: "Banking" },
  { name: "INDIGO OCT FUT", security_id: 52391, sector: "Aviation" },
  { name: "INDUSINDBK OCT FUT", security_id: 52394, sector: "Banking" },
  { name: "INDUSTOWER OCT FUT", security_id: 52395, sector: "Telecom" },
  { name: "INFY OCT FUT", security_id: 52398, sector: "IT" },
  { name: "INOXWIND OCT FUT", security_id: 52399, sector: "Capital Goods" },
  { name: "IOC OCT FUT", security_id: 52406, sector: "Oil & Gas" },
  { name: "IRCTC OCT FUT", security_id: 52407, sector: "Tourism" },
  { name: "IREDA OCT FUT", security_id: 52410, sector: "Financial Services" },
  { name: "IRFC OCT FUT", security_id: 52411, sector: "Financial Services" },
  { name: "ITC OCT FUT", security_id: 52414, sector: "FMCG" },
  { name: "JINDALSTEL OCT FUT", security_id: 52415, sector: "Metals" },
  { name: "JIOFIN OCT FUT", security_id: 52418, sector: "Financial Services" },
  { name: "JSWENERGY OCT FUT", security_id: 52419, sector: "Utilities" },
  { name: "JSWSTEEL OCT FUT", security_id: 52422, sector: "Metals" },
  {
    name: "JUBLFOOD OCT FUT",
    security_id: 52423,
    sector: "Quick Service Restaurant",
  },
  { name: "KALYANKJIL OCT FUT", security_id: 52424, sector: "Retail" },
  { name: "KAYNES OCT FUT", security_id: 52425, sector: "IT" },
  { name: "KEI OCT FUT", security_id: 52428, sector: "Capital Goods" },
  {
    name: "KFINTECH OCT FUT",
    security_id: 52429,
    sector: "Financial Services",
  },
  { name: "KOTAKBANK OCT FUT", security_id: 52430, sector: "Banking" },
  { name: "KPITTECH OCT FUT", security_id: 52431, sector: "IT" },
  { name: "LAURUSLABS OCT FUT", security_id: 52432, sector: "Pharmaceuticals" },
  {
    name: "LICHSGFIN OCT FUT",
    security_id: 52433,
    sector: "Financial Services",
  },
  { name: "LICI OCT FUT", security_id: 52434, sector: "Insurance" },
  { name: "LODHA OCT FUT", security_id: 52441, sector: "Real Estate" },
  { name: "LT OCT FUT", security_id: 52442, sector: "Infrastructure" },
  { name: "LTF OCT FUT", security_id: 52443, sector: "Financial Services" },
  { name: "LTIM OCT FUT", security_id: 52444, sector: "IT" },
  { name: "LUPIN OCT FUT", security_id: 52445, sector: "Pharmaceuticals" },
  { name: "M&M OCT FUT", security_id: 52446, sector: "Automotive" },
  {
    name: "MANAPPURAM OCT FUT",
    security_id: 52448,
    sector: "Financial Services",
  },
  { name: "MANKIND OCT FUT", security_id: 52449, sector: "Pharmaceuticals" },
  { name: "MARICO OCT FUT", security_id: 52453, sector: "FMCG" },
  { name: "MARUTI OCT FUT", security_id: 52454, sector: "Automotive" },
  { name: "MAXHEALTH OCT FUT", security_id: 52455, sector: "Healthcare" },
  { name: "MAZDOCK OCT FUT", security_id: 52456, sector: "Defence" },
  { name: "MCX OCT FUT", security_id: 52457, sector: "Financial Services" },
  { name: "MFSL OCT FUT", security_id: 52458, sector: "Insurance" },
  { name: "MOTHERSON OCT FUT", security_id: 52459, sector: "Automotive" },
  { name: "MPHASIS OCT FUT", security_id: 52460, sector: "IT" },
  {
    name: "MUTHOOTFIN OCT FUT",
    security_id: 52461,
    sector: "Financial Services",
  },
  { name: "NATIONALUM OCT FUT", security_id: 52462, sector: "Metals" },
  { name: "NAUKRI OCT FUT", security_id: 52463, sector: "IT" },
  { name: "NBCC OCT FUT", security_id: 52464, sector: "Construction" },
  { name: "NCC OCT FUT", security_id: 52465, sector: "Construction" },
  { name: "NESTLEIND OCT FUT", security_id: 52466, sector: "FMCG" },
  { name: "NHPC OCT FUT", security_id: 52470, sector: "Utilities" },
  { name: "NMDC OCT FUT", security_id: 52471, sector: "Metals" },
  { name: "NTPC OCT FUT", security_id: 52474, sector: "Utilities" },
  { name: "NUVAMA OCT FUT", security_id: 52484, sector: "Financial Services" },
  { name: "NYKAA OCT FUT", security_id: 52485, sector: "Retail" },
  { name: "OBEROIRLTY OCT FUT", security_id: 52486, sector: "Real Estate" },
  { name: "OFSS OCT FUT", security_id: 52487, sector: "IT" },
  { name: "OIL OCT FUT", security_id: 52488, sector: "Oil & Gas" },
  { name: "ONGC OCT FUT", security_id: 52489, sector: "Oil & Gas" },
  { name: "PAGEIND OCT FUT", security_id: 52490, sector: "Textiles" },
  { name: "PATANJALI OCT FUT", security_id: 52491, sector: "FMCG" },
  { name: "PAYTM OCT FUT", security_id: 52492, sector: "IT" },
  { name: "PERSISTENT OCT FUT", security_id: 52493, sector: "IT" },
  { name: "PETRONET OCT FUT", security_id: 52494, sector: "Oil & Gas" },
  { name: "PFC OCT FUT", security_id: 52495, sector: "Financial Services" },
  { name: "PGEL OCT FUT", security_id: 52496, sector: "Utilities" },
  { name: "PHOENIXLTD OCT FUT", security_id: 52497, sector: "Real Estate" },
  { name: "PIDILITIND OCT FUT", security_id: 52498, sector: "Chemicals" },
  { name: "PIIND OCT FUT", security_id: 52499, sector: "Chemicals" },
  { name: "PNB OCT FUT", security_id: 52500, sector: "Banking" },
  {
    name: "PNBHOUSING OCT FUT",
    security_id: 52501,
    sector: "Financial Services",
  },
  { name: "POLICYBZR OCT FUT", security_id: 52502, sector: "IT" },
  { name: "POLYCAB OCT FUT", security_id: 52503, sector: "Capital Goods" },
  { name: "POWERGRID OCT FUT", security_id: 52504, sector: "Utilities" },
  { name: "PPLPHARMA OCT FUT", security_id: 52505, sector: "Pharmaceuticals" },
  { name: "PRESTIGE OCT FUT", security_id: 52506, sector: "Real Estate" },
  { name: "RBLBANK OCT FUT", security_id: 52507, sector: "Banking" },
  { name: "RECLTD OCT FUT", security_id: 52508, sector: "Financial Services" },
  { name: "RELIANCE OCT FUT", security_id: 52509, sector: "Conglomerate" },
  { name: "RVNL OCT FUT", security_id: 52510, sector: "Infrastructure" },
  { name: "SAIL OCT FUT", security_id: 52511, sector: "Metals" },
  { name: "SBICARD OCT FUT", security_id: 52512, sector: "Financial Services" },
  { name: "SBILIFE OCT FUT", security_id: 52513, sector: "Insurance" },
  { name: "SBIN OCT FUT", security_id: 52514, sector: "Banking" },
  { name: "SHREECEM OCT FUT", security_id: 52515, sector: "Cement" },
  {
    name: "SHRIRAMFIN OCT FUT",
    security_id: 52516,
    sector: "Financial Services",
  },
  { name: "SIEMENS OCT FUT", security_id: 52517, sector: "Capital Goods" },
  { name: "SOLARINDS OCT FUT", security_id: 52518, sector: "Chemicals" },
  { name: "SONACOMS OCT FUT", security_id: 52519, sector: "Automotive" },
  { name: "SRF OCT FUT", security_id: 52520, sector: "Chemicals" },
  { name: "SUNPHARMA OCT FUT", security_id: 52521, sector: "Pharmaceuticals" },
  {
    name: "SUPREMEIND OCT FUT",
    security_id: 52522,
    sector: "Consumer Durables",
  },
  { name: "SUZLON OCT FUT", security_id: 52525, sector: "Capital Goods" },
  { name: "SYNGENE OCT FUT", security_id: 52526, sector: "Pharmaceuticals" },
  { name: "TATACONSUM OCT FUT", security_id: 52527, sector: "FMCG" },
  { name: "TATAELXSI OCT FUT", security_id: 52531, sector: "IT" },
  { name: "TATAMOTORS OCT FUT", security_id: 52532, sector: "Automotive" },
  { name: "TATAPOWER OCT FUT", security_id: 52533, sector: "Utilities" },
  { name: "TATASTEEL OCT FUT", security_id: 52534, sector: "Metals" },
  { name: "TATATECH OCT FUT", security_id: 52538, sector: "IT" },
  { name: "TCS OCT FUT", security_id: 52539, sector: "IT" },
  { name: "TECHM OCT FUT", security_id: 52542, sector: "IT" },
  { name: "TIINDIA OCT FUT", security_id: 52543, sector: "Automotive" },
  { name: "TITAGARH OCT FUT", security_id: 52544, sector: "Capital Goods" },
  {
    name: "TITAN OCT FUT",
    security_id: 52545,
    sector: "Consumer Discretionary",
  },
  { name: "TORNTPHARM OCT FUT", security_id: 52548, sector: "Pharmaceuticals" },
  { name: "TORNTPOWER OCT FUT", security_id: 52549, sector: "Utilities" },
  { name: "TRENT OCT FUT", security_id: 52555, sector: "Retail" },
  { name: "TVSMOTOR OCT FUT", security_id: 52556, sector: "Automotive" },
  { name: "ULTRACEMCO OCT FUT", security_id: 52558, sector: "Cement" },
  { name: "UNIONBANK OCT FUT", security_id: 52559, sector: "Banking" },
  { name: "UNITDSPR OCT FUT", security_id: 52560, sector: "FMCG" },
  { name: "UNOMINDA OCT FUT", security_id: 52561, sector: "Automotive" },
  { name: "UPL OCT FUT", security_id: 52562, sector: "Chemicals" },
  { name: "VBL OCT FUT", security_id: 52563, sector: "FMCG" },
  { name: "VEDL OCT FUT", security_id: 52566, sector: "Metals" },
  { name: "VOLTAS OCT FUT", security_id: 52567, sector: "Consumer Durables" },
  { name: "WIPRO OCT FUT", security_id: 52568, sector: "IT" },
  { name: "YESBANK OCT FUT", security_id: 52569, sector: "Banking" },
  { name: "ZYDUSLIFE OCT FUT", security_id: 52570, sector: "Pharmaceuticals" },
];

export function Heatmap(app: Express, db: Db) {
  const collection = db.collection("nse_futstk_ohlc");

  const securityIdMap = new Map<number, SecurityMeta>();
  const securityIds: number[] = [];
  securities.forEach((sec) => {
    securityIdMap.set(sec.security_id, { name: sec.name, sector: sec.sector });
    securityIds.push(sec.security_id);
  });

  /** Shared aggregation that returns the latest doc (within cutoff) per security_id */
  async function latestPerSecurity(sinceMin: number) {
    const cutoff = new Date(Date.now() - Math.max(1, sinceMin) * 60_000);

    const pipeline = [
      {
        $match: {
          security_id: { $in: securityIds },
          received_at: { $gte: cutoff },
        },
      },
      { $sort: { received_at: -1 } },
      { $group: { _id: "$security_id", latestDoc: { $first: "$$ROOT" } } },
      { $replaceRoot: { newRoot: "$latestDoc" } },
      {
        $project: {
          _id: 1,
          security_id: 1,
          LTP: 1,
          close: 1,
          received_at: 1,
        },
      },
    ];

    const items = await collection
      .aggregate(pipeline, { allowDiskUse: true })
      .toArray();

    const processed: StockData[] = items.map((item: any) => {
      const securityId = Number(item.security_id);
      const meta = securityIdMap.get(securityId);

      const ltp = parseFloat(item.LTP ?? "0");
      const close = parseFloat(item.close ?? "0");
      const change =
        ltp &&
        close &&
        !Number.isNaN(ltp) &&
        !Number.isNaN(close) &&
        close !== 0
          ? ((ltp - close) / close) * 100
          : undefined;

      return {
        _id: item._id?.toString() ?? "",
        trading_symbol: meta?.name ?? "",
        LTP: item.LTP ?? "",
        close: item.close ?? "",
        sector: meta?.sector ?? "Unknown",
        security_id: securityId,
        change,
        received_at: item.received_at
          ? new Date(item.received_at).toISOString()
          : undefined,
      };
    });

    // identity for ETag: count + max timestamp + rough sums
    let lastISO: string | null = null;
    let sumL = 0;
    let sumC = 0;
    for (const it of processed) {
      const l = parseFloat(it.LTP ?? "0");
      const c = parseFloat(it.close ?? "0");
      if (Number.isFinite(l)) sumL += l;
      if (Number.isFinite(c)) sumC += c;
      const t = it.received_at ? new Date(it.received_at).getTime() : 0;
      if (!lastISO || (t && t > new Date(lastISO).getTime()))
        lastISO = it.received_at!;
    }

    return {
      processed,
      lastISO,
      sumL: Math.round(sumL),
      sumC: Math.round(sumC),
    };
  }

  /**
   * Legacy/simple endpoint (kept): GET /api/heatmap
   * Returns array of stocks (latest per security). 60s cache headers.
   */
  app.get("/api/heatmap", async (_req: Request, res: Response) => {
    try {
      const { processed } = await latestPerSecurity(1440); // 24h cutoff by default
      res.setHeader(
        "Cache-Control",
        "public, max-age=60, stale-while-revalidate=30"
      );
      res.setHeader("Vary", "Accept-Encoding");
      res.json(processed);
    } catch (error) {
      console.error("Error fetching heatmap data:", error);
      res.status(500).json({
        error: "Internal server error",
        details: error instanceof Error ? error.message : "Unknown error",
      });
    }
  });

  /**
   * NEW: Bulk + ETag + 24h backfill by default
   * GET /api/heatmap/bulk?sinceMin=1440
   * Response: { stocks: StockData[], lastISO: string|null }
   */
  app.get("/api/heatmap/bulk", async (req: Request, res: Response) => {
    try {
      const sinceMin = Math.max(1, Number(req.query.sinceMin) || 1440);
      const { processed, lastISO, sumL, sumC } = await latestPerSecurity(
        sinceMin
      );

      const identity = { cnt: processed.length, lastISO, sumL, sumC, sinceMin };
      const etag = buildETag(identity);

      const ifNoneMatch = req.headers["if-none-match"];
      if (ifNoneMatch && ifNoneMatch === etag) {
        res.status(304).end();
        return;
      }

      res.setHeader("ETag", etag);
      res.setHeader("Cache-Control", "no-store");
      res.json({ stocks: processed, lastISO });
    } catch (error) {
      console.error("Error in /api/heatmap/bulk:", error);
      res.status(500).json({
        error: "Internal server error",
        details: error instanceof Error ? error.message : "Unknown error",
      });
    }
  });
}
