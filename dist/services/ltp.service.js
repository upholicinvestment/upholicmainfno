"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getRecentLTPs = exports.saveLTP = exports.setDatabase = void 0;
const instrument_service_1 = require("./instrument.service");
let db;
const setDatabase = (database) => {
    db = database;
};
exports.setDatabase = setDatabase;
function getISTDate() {
    return new Date();
}
const saveLTP = async (data) => {
    try {
        if (!db)
            throw new Error("Database not initialized");
        const meta = await (0, instrument_service_1.getInstrumentMetadata)(Number(data.securityId || data.security_id));
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
        // console.log("✅ Saved LTP to DB:", ltpDoc);
    }
    catch (err) {
        console.error("❌ Error saving LTP data:", err);
    }
};
exports.saveLTP = saveLTP;
const getRecentLTPs = async (limit = 50) => {
    if (!db)
        throw new Error("Database not initialized");
    return await db
        .collection("ltp_history")
        .find({})
        .sort({ timestamp: -1 })
        .limit(limit)
        .toArray();
};
exports.getRecentLTPs = getRecentLTPs;
