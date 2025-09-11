"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getInstrumentMetadata = exports.fetchAndStoreInstruments = exports.setInstrumentDatabase = void 0;
const axios_1 = __importDefault(require("axios"));
const csv_parser_1 = __importDefault(require("csv-parser"));
let db;
const setInstrumentDatabase = (database) => {
    db = database;
};
exports.setInstrumentDatabase = setInstrumentDatabase;
/**
 * Fetch and store instrument metadata from Dhan Scrip Master CSV
 */
const fetchAndStoreInstruments = async () => {
    try {
        // console.log("📡 Fetching instrument master from Dhan...");
        const response = await axios_1.default.get("https://images.dhan.co/api-data/api-scrip-master-detailed.csv", { responseType: "stream" });
        const dataStream = response.data;
        const instruments = [];
        await new Promise((resolve, reject) => {
            dataStream
                .pipe((0, csv_parser_1.default)())
                .on("data", (row) => {
                instruments.push({
                    security_id: parseInt(row["SECURITY_ID"] || "0"),
                    trading_symbol: row["SYMBOL_NAME"] || "",
                    instrument_type: row["INSTRUMENT"] || "", // Updated
                    expiry_date: row["SM_EXPIRY_DATE"] || null, // Updated
                    strike_price: parseFloat(row["STRIKE_PRICE"] || "0"),
                    option_type: row["OPTION_TYPE"] || "",
                    expiry_flag: row["EXPIRY_FLAG"] || "",
                });
            })
                .on("end", resolve)
                .on("error", reject);
        });
        if (!db)
            throw new Error("Database not initialized");
        await db.collection("instruments").deleteMany({});
        await db.collection("instruments").insertMany(instruments);
        // console.log(`💾 Saved ${instruments.length} instruments to DB.`);
    }
    catch (err) {
        console.error("❌ Error fetching/storing instruments:", err);
    }
};
exports.fetchAndStoreInstruments = fetchAndStoreInstruments;
/**
 * Get instrument metadata by security_id
 */
const getInstrumentMetadata = async (security_id) => {
    if (!db)
        throw new Error("Database not initialized");
    return db.collection("instruments").findOne({ security_id });
};
exports.getInstrumentMetadata = getInstrumentMetadata;
