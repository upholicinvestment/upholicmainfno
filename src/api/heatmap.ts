// src/api/heatmap.ts
import { Express, Request, Response } from 'express';
import { Db } from 'mongodb';

type SecurityMeta = { name: string; sector: string };

export function Heatmap(app: Express, db: Db) {
  // Keep your static securities list here (or move to a service/constants file)
// OCT 2025 NSE Single-Stock Futures
const securities: { name: string; security_id: number; sector: string }[] = [
  { name: "SAMMAANCAP OCT FUT", security_id: 49082, sector: "Financial Services" },
  { name: "POWERINDIA OCT FUT", security_id: 49974, sector: "Capital Goods" },
  { name: "360ONE OCT FUT", security_id: 52170, sector: "Financial Services" },
  { name: "ABB OCT FUT", security_id: 52171, sector: "Capital Goods" },
  { name: "ABCAPITAL OCT FUT", security_id: 52172, sector: "Financial Services" },
  { name: "ADANIENSOL OCT FUT", security_id: 52173, sector: "Utilities" },
  { name: "ADANIENT OCT FUT", security_id: 52174, sector: "Conglomerate" },
  { name: "ADANIGREEN OCT FUT", security_id: 52175, sector: "Utilities" },
  { name: "ADANIPORTS OCT FUT", security_id: 52176, sector: "Logistics" },
  { name: "ALKEM OCT FUT", security_id: 52177, sector: "Pharmaceuticals" },
  { name: "AMBER OCT FUT", security_id: 52178, sector: "Chemicals" },
  { name: "AMBUJACEM OCT FUT", security_id: 52179, sector: "Cement" },
  { name: "ANGELONE OCT FUT", security_id: 52200, sector: "Financial Services" },
  { name: "APLAPOLLO OCT FUT", security_id: 52201, sector: "Metals" },
  { name: "APOLLOHOSP OCT FUT", security_id: 52214, sector: "Healthcare" },
  { name: "ASHOKLEY OCT FUT", security_id: 52215, sector: "Automotive" },
  { name: "ASIANPAINT OCT FUT", security_id: 52216, sector: "Paints" },
  { name: "ASTRAL OCT FUT", security_id: 52217, sector: "Industrials" },
  { name: "AUBANK OCT FUT", security_id: 52218, sector: "Banking" },
  { name: "AUROPHARMA OCT FUT", security_id: 52219, sector: "Pharmaceuticals" },
  { name: "AXISBANK OCT FUT", security_id: 52223, sector: "Banking" },
  { name: "BAJAJ-AUTO OCT FUT", security_id: 52224, sector: "Automotive" },
  { name: "BAJAJFINSV OCT FUT", security_id: 52240, sector: "Financial Services" },
  { name: "BAJFINANCE OCT FUT", security_id: 52241, sector: "Financial Services" },
  { name: "BANDHANBNK OCT FUT", security_id: 52255, sector: "Banking" },
  { name: "BANKBARODA OCT FUT", security_id: 52256, sector: "Banking" },
  { name: "BANKINDIA OCT FUT", security_id: 52266, sector: "Banking" },
  { name: "BDL OCT FUT", security_id: 52267, sector: "Defence" },
  { name: "BEL OCT FUT", security_id: 52274, sector: "Defence" },
  { name: "BHARATFORG OCT FUT", security_id: 52275, sector: "Automotive" },
  { name: "BHARTIARTL OCT FUT", security_id: 52276, sector: "Telecom" },
  { name: "BHEL OCT FUT", security_id: 52277, sector: "Capital Goods" },
  { name: "BIOCON OCT FUT", security_id: 52288, sector: "Pharmaceuticals" },
  { name: "BLUESTARCO OCT FUT", security_id: 52289, sector: "Consumer Durables" },
  { name: "BOSCHLTD OCT FUT", security_id: 52290, sector: "Automotive" },
  { name: "BPCL OCT FUT", security_id: 52291, sector: "Oil & Gas" },
  { name: "BRITANNIA OCT FUT", security_id: 52292, sector: "FMCG" },
  { name: "BSE OCT FUT", security_id: 52301, sector: "Financial Services" },
  { name: "CAMS OCT FUT", security_id: 52302, sector: "Financial Services" },
  { name: "CANBK OCT FUT", security_id: 52303, sector: "Banking" },
  { name: "CDSL OCT FUT", security_id: 52304, sector: "Financial Services" },
  { name: "CGPOWER OCT FUT", security_id: 52305, sector: "Capital Goods" },
  { name: "CHOLAFIN OCT FUT", security_id: 52306, sector: "Financial Services" },
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
  { name: "JUBLFOOD OCT FUT", security_id: 52423, sector: "Quick Service Restaurant" },
  { name: "KALYANKJIL OCT FUT", security_id: 52424, sector: "Retail" },
  { name: "KAYNES OCT FUT", security_id: 52425, sector: "IT" },
  { name: "KEI OCT FUT", security_id: 52428, sector: "Capital Goods" },
  { name: "KFINTECH OCT FUT", security_id: 52429, sector: "Financial Services" },
  { name: "KOTAKBANK OCT FUT", security_id: 52430, sector: "Banking" },
  { name: "KPITTECH OCT FUT", security_id: 52431, sector: "IT" },
  { name: "LAURUSLABS OCT FUT", security_id: 52432, sector: "Pharmaceuticals" },
  { name: "LICHSGFIN OCT FUT", security_id: 52433, sector: "Financial Services" },
  { name: "LICI OCT FUT", security_id: 52434, sector: "Insurance" },
  { name: "LODHA OCT FUT", security_id: 52441, sector: "Real Estate" },
  { name: "LT OCT FUT", security_id: 52442, sector: "Infrastructure" },
  { name: "LTF OCT FUT", security_id: 52443, sector: "Financial Services" },
  { name: "LTIM OCT FUT", security_id: 52444, sector: "IT" },
  { name: "LUPIN OCT FUT", security_id: 52445, sector: "Pharmaceuticals" },
  { name: "M&M OCT FUT", security_id: 52446, sector: "Automotive" },
  { name: "MANAPPURAM OCT FUT", security_id: 52448, sector: "Financial Services" },
  { name: "MANKIND OCT FUT", security_id: 52449, sector: "Pharmaceuticals" },
  { name: "MARICO OCT FUT", security_id: 52453, sector: "FMCG" },
  { name: "MARUTI OCT FUT", security_id: 52454, sector: "Automotive" },
  { name: "MAXHEALTH OCT FUT", security_id: 52455, sector: "Healthcare" },
  { name: "MAZDOCK OCT FUT", security_id: 52456, sector: "Defence" },
  { name: "MCX OCT FUT", security_id: 52457, sector: "Financial Services" },
  { name: "MFSL OCT FUT", security_id: 52458, sector: "Insurance" },
  { name: "MOTHERSON OCT FUT", security_id: 52459, sector: "Automotive" },
  { name: "MPHASIS OCT FUT", security_id: 52460, sector: "IT" },
  { name: "MUTHOOTFIN OCT FUT", security_id: 52461, sector: "Financial Services" },
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
  { name: "PNBHOUSING OCT FUT", security_id: 52501, sector: "Financial Services" },
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
  { name: "SHRIRAMFIN OCT FUT", security_id: 52516, sector: "Financial Services" },
  { name: "SIEMENS OCT FUT", security_id: 52517, sector: "Capital Goods" },
  { name: "SOLARINDS OCT FUT", security_id: 52518, sector: "Chemicals" },
  { name: "SONACOMS OCT FUT", security_id: 52519, sector: "Automotive" },
  { name: "SRF OCT FUT", security_id: 52520, sector: "Chemicals" },
  { name: "SUNPHARMA OCT FUT", security_id: 52521, sector: "Pharmaceuticals" },
  { name: "SUPREMEIND OCT FUT", security_id: 52522, sector: "Consumer Durables" },
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
  { name: "TITAN OCT FUT", security_id: 52545, sector: "Consumer Discretionary" },
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


  interface StockData {
    _id: string;
    trading_symbol: string;
    LTP: string;
    close: string;
    sector: string;
    security_id: number;
    change?: number;
    [key: string]: any;
  }

  app.get("/api/heatmap", async (_req: Request, res: Response) => {
    try {
      const collection = db.collection("nse_futstk_ohlc");

      const securityIdMap = new Map<number, SecurityMeta>();
      const securityIds: number[] = [];

      securities.forEach((sec) => {
        securityIdMap.set(sec.security_id, { name: sec.name, sector: sec.sector });
        securityIds.push(sec.security_id);
      });

      const pipeline = [
        { $match: { security_id: { $in: securityIds } } },
        { $sort: { received_at: -1 } },
        {
          $group: {
            _id: "$security_id",
            latestDoc: { $first: "$$ROOT" },
          },
        },
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

      const items = await collection.aggregate(pipeline).toArray();

      const processedItems: StockData[] = items.map((item: any) => {
        const securityId = Number(item.security_id);
        const meta = securityIdMap.get(securityId);

        const ltp = parseFloat(item.LTP ?? "0");
        const close = parseFloat(item.close ?? "0");

        const change =
          ltp && close && !isNaN(ltp) && !isNaN(close) && close !== 0
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
        };
      });

      res.setHeader("Cache-Control", "public, max-age=60, stale-while-revalidate=30");
      res.setHeader("Vary", "Accept-Encoding");
      res.json(processedItems);
    } catch (error) {
      console.error("Error fetching heatmap data:", error);
      res.status(500).json({
        error: "Internal server error",
        details: error instanceof Error ? error.message : "Unknown error",
      });
    }
  });
}







    // const securities = [
    //   {
    //     name: "360ONE JUL FUT",
    //     security_id: 53003,
    //     sector: "Financial Services",
    //   },
    //   { name: "AMBER JUL FUT", security_id: 53027, sector: "Chemicals" },
    //   { name: "AARTIIND JUL FUT", security_id: 53218, sector: "Chemicals" },
    //   { name: "ABB JUL FUT", security_id: 53219, sector: "Capital Goods" },
    //   {
    //     name: "ABCAPITAL JUL FUT",
    //     security_id: 53220,
    //     sector: "Financial Services",
    //   },
    //   {
    //     name: "ABFRL JUL FUT",
    //     security_id: 53221,
    //     sector: "Consumer Discretionary",
    //   },
    //   { name: "ACC JUL FUT", security_id: 53222, sector: "Cement" },
    //   { name: "ADANIENSOL JUL FUT", security_id: 53223, sector: "Utilities" },
    //   { name: "ADANIENT JUL FUT", security_id: 53224, sector: "Conglomerate" },
    //   { name: "ADANIGREEN JUL FUT", security_id: 53225, sector: "Utilities" },
    //   { name: "ADANIPORTS JUL FUT", security_id: 53226, sector: "Logistics" },
    //   { name: "ALKEM JUL FUT", security_id: 53227, sector: "Pharmaceuticals" },
    //   { name: "AMBUJACEM JUL FUT", security_id: 53235, sector: "Cement" },
    //   {
    //     name: "ANGELONE JUL FUT",
    //     security_id: 53236,
    //     sector: "Financial Services",
    //   },
    //   { name: "APLAPOLLO JUL FUT", security_id: 53240, sector: "Metals" },
    //   { name: "APOLLOHOSP JUL FUT", security_id: 53241, sector: "Healthcare" },
    //   { name: "ASHOKLEY JUL FUT", security_id: 53244, sector: "Automotive" },
    //   { name: "ASIANPAINT JUL FUT", security_id: 53245, sector: "Paints" },
    //   { name: "ASTRAL JUL FUT", security_id: 53246, sector: "Industrials" },
    //   { name: "ATGL JUL FUT", security_id: 53247, sector: "Utilities" },
    //   { name: "AUBANK JUL FUT", security_id: 53248, sector: "Banking" },
    //   {
    //     name: "AUROPHARMA JUL FUT",
    //     security_id: 53249,
    //     sector: "Pharmaceuticals",
    //   },
    //   { name: "AXISBANK JUL FUT", security_id: 53250, sector: "Banking" },
    //   { name: "BAJAJ-AUTO JUL FUT", security_id: 53251, sector: "Automotive" },
    //   {
    //     name: "BAJAJFINSV JUL FUT",
    //     security_id: 53252,
    //     sector: "Financial Services",
    //   },
    //   {
    //     name: "BAJFINANCE JUL FUT",
    //     security_id: 53253,
    //     sector: "Financial Services",
    //   },
    //   { name: "BALKRISIND JUL FUT", security_id: 53254, sector: "Automotive" },
    //   { name: "BANDHANBNK JUL FUT", security_id: 53255, sector: "Banking" },
    //   { name: "BANKBARODA JUL FUT", security_id: 53256, sector: "Banking" },
    //   { name: "BANKINDIA JUL FUT", security_id: 53257, sector: "Banking" },
    //   { name: "BEL JUL FUT", security_id: 53258, sector: "Defence" },
    //   { name: "BHARATFORG JUL FUT", security_id: 53259, sector: "Automotive" },
    //   { name: "BHARTIARTL JUL FUT", security_id: 53260, sector: "Telecom" },
    //   { name: "BHEL JUL FUT", security_id: 53261, sector: "Capital Goods" },
    //   { name: "BIOCON JUL FUT", security_id: 53262, sector: "Pharmaceuticals" },
    //   { name: "BOSCHLTD JUL FUT", security_id: 53263, sector: "Automotive" },
    //   { name: "BPCL JUL FUT", security_id: 53264, sector: "Oil & Gas" },
    //   { name: "BRITANNIA JUL FUT", security_id: 53265, sector: "FMCG" },
    //   { name: "BSE JUL FUT", security_id: 53268, sector: "Financial Services" },
    //   { name: "BSOFT JUL FUT", security_id: 53269, sector: "IT" },
    //   {
    //     name: "CAMS JUL FUT",
    //     security_id: 53270,
    //     sector: "Financial Services",
    //   },
    //   { name: "CANBK JUL FUT", security_id: 53273, sector: "Banking" },
    //   {
    //     name: "CDSL JUL FUT",
    //     security_id: 53274,
    //     sector: "Financial Services",
    //   },
    //   { name: "CESC JUL FUT", security_id: 53275, sector: "Utilities" },
    //   { name: "CGPOWER JUL FUT", security_id: 53276, sector: "Capital Goods" },
    //   { name: "CHAMBLFERT JUL FUT", security_id: 53277, sector: "Fertilizers" },
    //   {
    //     name: "CHOLAFIN JUL FUT",
    //     security_id: 53278,
    //     sector: "Financial Services",
    //   },
    //   { name: "CIPLA JUL FUT", security_id: 53279, sector: "Pharmaceuticals" },
    //   { name: "COALINDIA JUL FUT", security_id: 53280, sector: "Metals" },
    //   { name: "COFORGE JUL FUT", security_id: 53281, sector: "IT" },
    //   { name: "COLPAL JUL FUT", security_id: 53284, sector: "FMCG" },
    //   { name: "CONCOR JUL FUT", security_id: 53286, sector: "Logistics" },
    //   {
    //     name: "CROMPTON JUL FUT",
    //     security_id: 53289,
    //     sector: "Consumer Durables",
    //   },
    //   {
    //     name: "CUMMINSIND JUL FUT",
    //     security_id: 53290,
    //     sector: "Capital Goods",
    //   },
    //   { name: "CYIENT JUL FUT", security_id: 53291, sector: "IT" },
    //   { name: "DABUR JUL FUT", security_id: 53292, sector: "FMCG" },
    //   { name: "DALBHARAT JUL FUT", security_id: 53293, sector: "Cement" },
    //   { name: "DELHIVERY JUL FUT", security_id: 53294, sector: "Logistics" },
    //   {
    //     name: "DIVISLAB JUL FUT",
    //     security_id: 53295,
    //     sector: "Pharmaceuticals",
    //   },
    //   {
    //     name: "DIXON JUL FUT",
    //     security_id: 53296,
    //     sector: "Consumer Durables",
    //   },
    //   { name: "DLF JUL FUT", security_id: 53297, sector: "Real Estate" },
    //   { name: "DMART JUL FUT", security_id: 53298, sector: "Retail" },
    //   {
    //     name: "DRREDDY JUL FUT",
    //     security_id: 53299,
    //     sector: "Pharmaceuticals",
    //   },
    //   { name: "EICHERMOT JUL FUT", security_id: 53300, sector: "Automotive" },
    //   { name: "ETERNAL JUL FUT", security_id: 53302, sector: "Healthcare" },
    //   { name: "EXIDEIND JUL FUT", security_id: 53303, sector: "Automotive" },
    //   { name: "FEDERALBNK JUL FUT", security_id: 53304, sector: "Banking" },
    //   { name: "GAIL JUL FUT", security_id: 53305, sector: "Oil & Gas" },
    //   {
    //     name: "GLENMARK JUL FUT",
    //     security_id: 53306,
    //     sector: "Pharmaceuticals",
    //   },
    //   { name: "GMRAIRPORT JUL FUT", security_id: 53307, sector: "Logistics" },
    //   { name: "GODREJCP JUL FUT", security_id: 53308, sector: "FMCG" },
    //   { name: "GODREJPROP JUL FUT", security_id: 53309, sector: "Real Estate" },
    //   {
    //     name: "GRANULES JUL FUT",
    //     security_id: 53310,
    //     sector: "Pharmaceuticals",
    //   },
    //   { name: "GRASIM JUL FUT", security_id: 53311, sector: "Cement" },
    //   { name: "HAL JUL FUT", security_id: 53312, sector: "Defence" },
    //   {
    //     name: "HAVELLS JUL FUT",
    //     security_id: 53313,
    //     sector: "Consumer Durables",
    //   },
    //   { name: "HCLTECH JUL FUT", security_id: 53314, sector: "IT" },
    //   {
    //     name: "HDFCAMC JUL FUT",
    //     security_id: 53315,
    //     sector: "Financial Services",
    //   },
    //   { name: "HDFCBANK JUL FUT", security_id: 53316, sector: "Banking" },
    //   { name: "HDFCLIFE JUL FUT", security_id: 53317, sector: "Insurance" },
    //   { name: "HEROMOTOCO JUL FUT", security_id: 53318, sector: "Automotive" },
    //   { name: "HFCL JUL FUT", security_id: 53319, sector: "Telecom" },
    //   { name: "HINDALCO JUL FUT", security_id: 53321, sector: "Metals" },
    //   { name: "HINDCOPPER JUL FUT", security_id: 53322, sector: "Metals" },
    //   { name: "HINDPETRO JUL FUT", security_id: 53323, sector: "Oil & Gas" },
    //   { name: "HINDUNILVR JUL FUT", security_id: 53324, sector: "FMCG" },
    //   { name: "HINDZINC JUL FUT", security_id: 53325, sector: "Metals" },
    //   {
    //     name: "HUDCO JUL FUT",
    //     security_id: 53326,
    //     sector: "Financial Services",
    //   },
    //   { name: "ICICIBANK JUL FUT", security_id: 53327, sector: "Banking" },
    //   { name: "ICICIGI JUL FUT", security_id: 53328, sector: "Insurance" },
    //   { name: "ICICIPRULI JUL FUT", security_id: 53329, sector: "Insurance" },
    //   { name: "IDEA JUL FUT", security_id: 53330, sector: "Telecom" },
    //   { name: "IDFCFIRSTB JUL FUT", security_id: 53334, sector: "Banking" },
    //   { name: "IEX JUL FUT", security_id: 53335, sector: "Utilities" },
    //   { name: "IGL JUL FUT", security_id: 53336, sector: "Oil & Gas" },
    //   {
    //     name: "IIFL JUL FUT",
    //     security_id: 53337,
    //     sector: "Financial Services",
    //   },
    //   { name: "INDHOTEL JUL FUT", security_id: 53338, sector: "Hospitality" },
    //   { name: "INDIANB JUL FUT", security_id: 53339, sector: "Banking" },
    //   { name: "INDIGO JUL FUT", security_id: 53340, sector: "Aviation" },
    //   { name: "INDUSINDBK JUL FUT", security_id: 53341, sector: "Banking" },
    //   { name: "INDUSTOWER JUL FUT", security_id: 53342, sector: "Telecom" },
    //   { name: "INFY JUL FUT", security_id: 53343, sector: "IT" },
    //   { name: "INOXWIND JUL FUT", security_id: 53344, sector: "Capital Goods" },
    //   { name: "IOC JUL FUT", security_id: 53345, sector: "Oil & Gas" },
    //   { name: "IRB JUL FUT", security_id: 53346, sector: "Infrastructure" },
    //   { name: "IRCTC JUL FUT", security_id: 53347, sector: "Tourism" },
    //   {
    //     name: "IREDA JUL FUT",
    //     security_id: 53348,
    //     sector: "Financial Services",
    //   },
    //   {
    //     name: "IRFC JUL FUT",
    //     security_id: 53351,
    //     sector: "Financial Services",
    //   },
    //   { name: "ITC JUL FUT", security_id: 53352, sector: "FMCG" },
    //   { name: "JINDALSTEL JUL FUT", security_id: 53353, sector: "Metals" },
    //   {
    //     name: "JIOFIN JUL FUT",
    //     security_id: 53354,
    //     sector: "Financial Services",
    //   },
    //   { name: "JSL JUL FUT", security_id: 53355, sector: "Metals" },
    //   { name: "JSWENERGY JUL FUT", security_id: 53358, sector: "Utilities" },
    //   { name: "JSWSTEEL JUL FUT", security_id: 53359, sector: "Metals" },
    //   {
    //     name: "JUBLFOOD JUL FUT",
    //     security_id: 53366,
    //     sector: "Quick Service Restaurant",
    //   },
    //   { name: "KALYANKJIL JUL FUT", security_id: 53367, sector: "Retail" },
    //   { name: "KEI JUL FUT", security_id: 53368, sector: "Capital Goods" },
    //   { name: "KOTAKBANK JUL FUT", security_id: 53369, sector: "Banking" },
    //   { name: "KPITTECH JUL FUT", security_id: 53370, sector: "IT" },
    //   {
    //     name: "LAURUSLABS JUL FUT",
    //     security_id: 53371,
    //     sector: "Pharmaceuticals",
    //   },
    //   {
    //     name: "LICHSGFIN JUL FUT",
    //     security_id: 53372,
    //     sector: "Financial Services",
    //   },
    //   { name: "LICI JUL FUT", security_id: 53373, sector: "Insurance" },
    //   { name: "LODHA JUL FUT", security_id: 53374, sector: "Real Estate" },
    //   { name: "LT JUL FUT", security_id: 53375, sector: "Infrastructure" },
    //   { name: "LTF JUL FUT", security_id: 53376, sector: "Financial Services" },
    //   { name: "LTIM JUL FUT", security_id: 53377, sector: "IT" },
    //   { name: "LUPIN JUL FUT", security_id: 53378, sector: "Pharmaceuticals" },
    //   { name: "M&M JUL FUT", security_id: 53379, sector: "Automotive" },
    //   {
    //     name: "M&MFIN JUL FUT",
    //     security_id: 53380,
    //     sector: "Financial Services",
    //   },
    //   {
    //     name: "MANAPPURAM JUL FUT",
    //     security_id: 53381,
    //     sector: "Financial Services",
    //   },
    //   { name: "MARICO JUL FUT", security_id: 53382, sector: "FMCG" },
    //   { name: "MARUTI JUL FUT", security_id: 53383, sector: "Automotive" },
    //   { name: "MAXHEALTH JUL FUT", security_id: 53384, sector: "Healthcare" },
    //   { name: "MCX JUL FUT", security_id: 53385, sector: "Financial Services" },
    //   { name: "MFSL JUL FUT", security_id: 53386, sector: "Insurance" },
    //   { name: "MGL JUL FUT", security_id: 53387, sector: "Oil & Gas" },
    //   { name: "MOTHERSON JUL FUT", security_id: 53388, sector: "Automotive" },
    //   { name: "MPHASIS JUL FUT", security_id: 53389, sector: "IT" },
    //   {
    //     name: "MUTHOOTFIN JUL FUT",
    //     security_id: 53390,
    //     sector: "Financial Services",
    //   },
    //   { name: "NATIONALUM JUL FUT", security_id: 53391, sector: "Metals" },
    //   { name: "NAUKRI JUL FUT", security_id: 53392, sector: "IT" },
    //   { name: "NBCC JUL FUT", security_id: 53393, sector: "Construction" },
    //   { name: "NCC JUL FUT", security_id: 53394, sector: "Construction" },
    //   { name: "NESTLEIND JUL FUT", security_id: 53395, sector: "FMCG" },
    //   { name: "NHPC JUL FUT", security_id: 53396, sector: "Utilities" },
    //   { name: "NMDC JUL FUT", security_id: 53397, sector: "Metals" },
    //   { name: "NTPC JUL FUT", security_id: 53398, sector: "Utilities" },
    //   { name: "NYKAA JUL FUT", security_id: 53399, sector: "Retail" },
    //   { name: "OBEROIRLTY JUL FUT", security_id: 53402, sector: "Real Estate" },
    //   { name: "OFSS JUL FUT", security_id: 53403, sector: "IT" },
    //   { name: "OIL JUL FUT", security_id: 53404, sector: "Oil & Gas" },
    //   { name: "ONGC JUL FUT", security_id: 53405, sector: "Oil & Gas" },
    //   { name: "PAGEIND JUL FUT", security_id: 53406, sector: "Textiles" },
    //   { name: "PATANJALI JUL FUT", security_id: 53407, sector: "FMCG" },
    //   { name: "PAYTM JUL FUT", security_id: 53408, sector: "IT" },
    //   { name: "PEL JUL FUT", security_id: 53409, sector: "Financial Services" },
    //   { name: "PERSISTENT JUL FUT", security_id: 53413, sector: "IT" },
    //   { name: "PETRONET JUL FUT", security_id: 53414, sector: "Oil & Gas" },
    //   { name: "PFC JUL FUT", security_id: 53415, sector: "Financial Services" },
    //   { name: "PHOENIXLTD JUL FUT", security_id: 53416, sector: "Real Estate" },
    //   { name: "PIDILITIND JUL FUT", security_id: 53418, sector: "Chemicals" },
    //   { name: "PIIND JUL FUT", security_id: 53419, sector: "Chemicals" },
    //   { name: "PNB JUL FUT", security_id: 53420, sector: "Banking" },
    //   {
    //     name: "PNBHOUSING JUL FUT",
    //     security_id: 53421,
    //     sector: "Financial Services",
    //   },
    //   { name: "POLICYBZR JUL FUT", security_id: 53422, sector: "IT" },
    //   { name: "POLYCAB JUL FUT", security_id: 53423, sector: "Capital Goods" },
    //   {
    //     name: "POONAWALLA JUL FUT",
    //     security_id: 53424,
    //     sector: "Financial Services",
    //   },
    //   { name: "POWERGRID JUL FUT", security_id: 53425, sector: "Utilities" },
    //   { name: "PRESTIGE JUL FUT", security_id: 53426, sector: "Real Estate" },
    //   { name: "RBLBANK JUL FUT", security_id: 53427, sector: "Banking" },
    //   {
    //     name: "RECLTD JUL FUT",
    //     security_id: 53428,
    //     sector: "Financial Services",
    //   },
    //   { name: "RELIANCE JUL FUT", security_id: 53429, sector: "Conglomerate" },
    //   { name: "SAIL JUL FUT", security_id: 53430, sector: "Metals" },
    //   {
    //     name: "SBICARD JUL FUT",
    //     security_id: 53431,
    //     sector: "Financial Services",
    //   },
    //   { name: "SBILIFE JUL FUT", security_id: 53432, sector: "Insurance" },
    //   { name: "SBIN JUL FUT", security_id: 53433, sector: "Banking" },
    //   { name: "SHREECEM JUL FUT", security_id: 53434, sector: "Cement" },
    //   {
    //     name: "SHRIRAMFIN JUL FUT",
    //     security_id: 53435,
    //     sector: "Financial Services",
    //   },
    //   { name: "SIEMENS JUL FUT", security_id: 53436, sector: "Capital Goods" },
    //   { name: "SJVN JUL FUT", security_id: 53437, sector: "Utilities" },
    //   { name: "SOLARINDS JUL FUT", security_id: 53438, sector: "Chemicals" },
    //   { name: "SONACOMS JUL FUT", security_id: 53439, sector: "Automotive" },
    //   { name: "SRF JUL FUT", security_id: 53440, sector: "Chemicals" },
    //   {
    //     name: "SUNPHARMA JUL FUT",
    //     security_id: 53441,
    //     sector: "Pharmaceuticals",
    //   },
    //   {
    //     name: "SUPREMEIND JUL FUT",
    //     security_id: 53442,
    //     sector: "Consumer Durables",
    //   },
    //   {
    //     name: "SYNGENE JUL FUT",
    //     security_id: 53443,
    //     sector: "Pharmaceuticals",
    //   },
    //   { name: "TATACHEM JUL FUT", security_id: 53448, sector: "Chemicals" },
    //   { name: "TATACOMM JUL FUT", security_id: 53449, sector: "Telecom" },
    //   { name: "TATACONSUM JUL FUT", security_id: 53450, sector: "FMCG" },
    //   { name: "TATAELXSI JUL FUT", security_id: 53451, sector: "IT" },
    //   { name: "TATAMOTORS JUL FUT", security_id: 53452, sector: "Automotive" },
    //   { name: "TATAPOWER JUL FUT", security_id: 53453, sector: "Utilities" },
    //   { name: "TATASTEEL JUL FUT", security_id: 53454, sector: "Metals" },
    //   { name: "TATATECH JUL FUT", security_id: 53455, sector: "IT" },
    //   { name: "TCS JUL FUT", security_id: 53460, sector: "IT" },
    //   { name: "TECHM JUL FUT", security_id: 53461, sector: "IT" },
    //   { name: "TIINDIA JUL FUT", security_id: 53464, sector: "Automotive" },
    //   { name: "TITAGARH JUL FUT", security_id: 53465, sector: "Capital Goods" },
    //   {
    //     name: "TITAN JUL FUT",
    //     security_id: 53466,
    //     sector: "Consumer Discretionary",
    //   },
    //   {
    //     name: "TORNTPHARM JUL FUT",
    //     security_id: 53467,
    //     sector: "Pharmaceuticals",
    //   },
    //   { name: "TORNTPOWER JUL FUT", security_id: 53468, sector: "Utilities" },
    //   { name: "TRENT JUL FUT", security_id: 53469, sector: "Retail" },
    //   { name: "TVSMOTOR JUL FUT", security_id: 53470, sector: "Automotive" },
    //   { name: "ULTRACEMCO JUL FUT", security_id: 53471, sector: "Cement" },
    //   { name: "UNIONBANK JUL FUT", security_id: 53472, sector: "Banking" },
    //   { name: "UNITDSPR JUL FUT", security_id: 53473, sector: "FMCG" },
    //   { name: "UPL JUL FUT", security_id: 53474, sector: "Chemicals" },
    //   { name: "VBL JUL FUT", security_id: 53475, sector: "FMCG" },
    //   { name: "VEDL JUL FUT", security_id: 53478, sector: "Metals" },
    //   {
    //     name: "VOLTAS JUL FUT",
    //     security_id: 53479,
    //     sector: "Consumer Durables",
    //   },
    //   { name: "WIPRO JUL FUT", security_id: 53480, sector: "IT" },
    //   { name: "YESBANK JUL FUT", security_id: 53481, sector: "Banking" },
    //   {
    //     name: "ZYDUSLIFE JUL FUT",
    //     security_id: 53484,
    //     sector: "Pharmaceuticals",
    //   },
    //   { name: "PGEL JUL FUT", security_id: 53763, sector: "Utilities" },
    //   { name: "BDL JUL FUT", security_id: 64225, sector: "Defence" },
    //   {
    //     name: "BLUESTARCO JUL FUT",
    //     security_id: 64233,
    //     sector: "Consumer Durables",
    //   },
    //   { name: "FORTIS JUL FUT", security_id: 64412, sector: "Healthcare" },
    //   { name: "KAYNES JUL FUT", security_id: 64624, sector: "IT" },
    //   {
    //     name: "MANKIND JUL FUT",
    //     security_id: 64901,
    //     sector: "Pharmaceuticals",
    //   },
    //   { name: "MAZDOCK JUL FUT", security_id: 64907, sector: "Defence" },
    //   {
    //     name: "PPLPHARMA JUL FUT",
    //     security_id: 64988,
    //     sector: "Pharmaceuticals",
    //   },
    //   { name: "RVNL JUL FUT", security_id: 64997, sector: "Infrastructure" },
    //   { name: "UNOMINDA JUL FUT", security_id: 65239, sector: "Automotive" },
    // ];

    // interface StockData {
    //   _id: string;
    //   trading_symbol: string;
    //   LTP: string;
    //   close: string;
    //   sector: string;
    //   security_id: number;
    //   change?: number;
    //   [key: string]: any;
    // }

    // app.get("/api/heatmap", async (req, res) => {
    //   try {
    //     const collection = db.collection("nse_fno_stock");

    //     // Build a map for fast lookup
    //     const securityIdMap = new Map<
    //       number,
    //       { name: string; sector: string }
    //     >();
    //     const securityIds: number[] = [];

    //     securities.forEach((sec) => {
    //       securityIdMap.set(sec.security_id, {
    //         name: sec.name,
    //         sector: sec.sector,
    //       });
    //       securityIds.push(sec.security_id);
    //     });

    //     // Aggregation pipeline
    //     const pipeline = [
    //       {
    //         $match: {
    //           security_id: { $in: securityIds },
    //         },
    //       },
    //       {
    //         $sort: { received_at: -1 },
    //       },
    //       {
    //         $group: {
    //           _id: "$security_id",
    //           latestDoc: { $first: "$$ROOT" },
    //         },
    //       },
    //       {
    //         $replaceRoot: { newRoot: "$latestDoc" },
    //       },
    //       {
    //         $project: {
    //           _id: 1,
    //           security_id: 1,
    //           LTP: 1,
    //           close: 1,
    //           received_at: 1,
    //         },
    //       },
    //     ];

    //     // Run aggregation
    //     const cursor = collection.aggregate(pipeline);
    //     const items = await cursor.toArray();

    //     // Process data
    //     const processedItems: StockData[] = items.map((item) => {
    //       const securityId = Number(item.security_id);
    //       const securityInfo = securityIdMap.get(securityId);

    //       const ltp = parseFloat(item.LTP ?? "0");
    //       const close = parseFloat(item.close ?? "0");

    //       const change =
    //         ltp && close && !isNaN(ltp) && !isNaN(close) && close !== 0
    //           ? ((ltp - close) / close) * 100
    //           : undefined;

    //       return {
    //         _id: item._id?.toString() ?? "",
    //         trading_symbol: securityInfo?.name ?? "",
    //         LTP: item.LTP ?? "",
    //         close: item.close ?? "",
    //         sector: securityInfo?.sector ?? "Unknown",
    //         security_id: securityId,
    //         change,
    //       };
    //     });

    //     // Set headers and send response
    //     res.setHeader(
    //       "Cache-Control",
    //       "public, max-age=60, stale-while-revalidate=30"
    //     );
    //     res.setHeader("Vary", "Accept-Encoding");
    //     res.json(processedItems);
    //   } catch (error) {
    //     console.error("Error fetching heatmap data:", error);
    //     res.status(500).json({
    //       error: "Internal server error",
    //       details: error instanceof Error ? error.message : "Unknown error",
    //     });
    //   }
    // });