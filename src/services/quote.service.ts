import axios from "axios";
import { Db } from "mongodb";
import csvParser from "csv-parser";
import { Readable } from "stream";

let db: Db;

export const setDatabase = (database: Db) => {
  db = database;
};

// Convert UTC to IST
function getISTDate(): Date {
  return new Date();
}

// Check if market is open (9:15 AM - 3:30 PM IST)
function isMarketOpen(): boolean {
  const now = getISTDate();
  const hours = now.getHours();
  const minutes = now.getMinutes();
  const totalMinutes = hours * 60 + minutes;
  return totalMinutes >= 9 * 60 + 15 && totalMinutes <= 15 * 60 + 30;
}

/**
 * Fetch Market Quote data for NSE_FNO instruments.
 */
export const fetchMarketQuote = async (ids: number[]): Promise<Record<string, any>> => {
  try {
    const payload = { NSE_FNO: ids };
    const response: any = await axios.post(
      "https://api.dhan.co/v2/marketfeed/quote",
      payload,
      {
        headers: {
          "Content-Type": "application/json",
          "access-token": process.env.DHAN_API_KEY || "",
          "client-id": process.env.DHAN_CLIENT_ID || "",
        },
        timeout: 8000, // increased timeout
      }
    );

    const data = response?.data?.data?.NSE_FNO || {};
    console.log(`📡 Quote Response: ${Object.keys(data).length} items fetched.`);
    return data;
  } catch (err: any) {
    if (err.response?.status === 429) {
      console.error("⏳ Rate limit (429): Too many requests. Retrying...");
    } else if (err.code === "ECONNABORTED") {
      console.error("❌ Quote API Timeout: request took too long.");
    } else {
      console.error("❌ Quote API Error (NSE_FNO):", err.message || err);
    }
    return {};
  }
};

/**
 * Save Market Quote data into MongoDB.
 */
export const saveMarketQuote = async (data: Record<string, any>) => {
  try {
    if (!db) throw new Error("Database not initialized");

    const timestamp = new Date();
    const securityIds = Object.keys(data).map((id) => parseInt(id));

    // Fetch all instrument metadata for the batch
    const instruments = await db
      .collection("instruments")
      .find({ security_id: { $in: securityIds } })
      .toArray();

    const instrumentMap = Object.fromEntries(
      instruments.map((inst) => [inst.security_id, inst])
    );

    // Merge quote data with instrument metadata
    const documents = Object.entries(data).map(([security_id, details]: [string, any]) => {
      const instrument = instrumentMap[parseInt(security_id)] || {};

      return {
        security_id,
        trading_symbol: instrument.trading_symbol || "",
        instrument_type: instrument.instrument_type || "",
        expiry_date: instrument.expiry_date || null,
        strike_price: instrument.strike_price || 0,
        option_type: instrument.option_type || "",
        expiry_flag: instrument.expiry_flag || "",
        average_price: details.average_price || 0,
        buy_quantity: details.buy_quantity || 0,
        depth: details.depth || { buy: [], sell: [] },
        exchange: "NSE_FNO",
        last_price: details.last_price || 0,
        last_quantity: details.last_quantity || 0,
        last_trade_time: details.last_trade_time || "",
        lower_circuit_limit: details.lower_circuit_limit || 0,
        net_change: details.net_change || 0,
        ohlc: details.ohlc || { open: 0, close: 0, high: 0, low: 0 },
        oi: details.oi || 0,
        oi_day_high: details.oi_day_high || 0,
        oi_day_low: details.oi_day_low || 0,
        sell_quantity: details.sell_quantity || 0,
        upper_circuit_limit: details.upper_circuit_limit || 0,
        volume: details.volume || 0,
        timestamp,
      };
    });

    if (documents.length > 0) {
      const result = await db.collection("market_quotes").insertMany(documents);
      console.log(`💾 Saved ${result.insertedCount} Market Quote docs at ${timestamp.toLocaleString("en-IN")}.`);
    }
  } catch (err) {
    console.error("❌ Error saving Market Quote:", err);
  }
};

/**
 * Fetch and store instrument metadata from Dhan Scrip Master CSV
 */
export const fetchAndStoreInstruments = async () => {
  try {
    console.log("📡 Fetching instrument master from Dhan...");
    const response = await axios.get(
      "https://images.dhan.co/api-data/api-scrip-master-detailed.csv",
      { responseType: "stream" }
    );

    const dataStream = response.data as Readable;
    const instruments: any[] = [];

    await new Promise<void>((resolve, reject) => {
      dataStream
        .pipe(csvParser())
        .on("data", (row: any) => {
          instruments.push({
            security_id: parseInt(row["SECURITY_ID"] || "0"),
            trading_symbol: row["SYMBOL_NAME"] || "",
            instrument_type: row["INSTRUMENT"] || "",
            expiry_date: row["SM_EXPIRY_DATE"] || null,
            strike_price: parseFloat(row["STRIKE_PRICE"] || "0"),
            option_type: row["OPTION_TYPE"] || "",
            expiry_flag: row["EXPIRY_FLAG"] || "",
          });
        })
        .on("end", resolve)
        .on("error", reject);
    });

    if (!db) throw new Error("Database not initialized");
    await db.collection("instruments").deleteMany({});
    await db.collection("instruments").insertMany(instruments);

    console.log(`💾 Saved ${instruments.length} instruments to DB.`);
  } catch (err) {
    console.error("❌ Error fetching/storing instruments:", err);
  }
};
