import { Db } from "mongodb";
import { getInstrumentMetadata } from "./instrument.service";

let db: Db;

export const setDatabase = (database: Db) => {
  db = database;
};

function getISTDate(): Date {
  return new Date();
}

export const saveLTP = async (data: any) => {
  try {
    if (!db) throw new Error("Database not initialized");

    const meta = await getInstrumentMetadata(Number(data.securityId || data.security_id));

    const ltpDoc = {
      security_id: Number(data.securityId || data.security_id || 0),
      LTP: Number(data.LTP || data.ltp || 0),
      trading_symbol: meta?.trading_symbol || "",
      instrument_type: meta?.instrument_type || "",
      expiry_date: meta?.expiry_date || null,
      strike_price: meta?.strike_price || 0,
      option_type: meta?.option_type || "",
      expiry_flag: meta?.expiry_flag || "",
      timestamp: getISTDate(),
    };

    await db.collection("ltp_history").insertOne(ltpDoc);
    console.log("✅ Saved LTP to DB:", ltpDoc);
  } catch (err) {
    console.error("❌ Error saving LTP data:", err);
  }
};

export const getRecentLTPs = async (limit = 50) => {
  if (!db) throw new Error("Database not initialized");
  return await db
    .collection("ltp_history")
    .find({})
    .sort({ timestamp: -1 })
    .limit(limit)
    .toArray();
};
