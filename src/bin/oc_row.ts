// src/bin/oc_row.ts
import { computeRowFromDB } from "../services/oc_signal";

(async () => {
  const MONGO = process.env.MONGO_URI || "mongodb://localhost:27017";
  const DB    = process.env.MONGO_DB_NAME || process.env.DB_NAME || "Upholic";
  const UNDERLYING = Number(process.env.OC_UNDERLYING_ID || 13);
  const EXPIRY     = process.env.OC_EXPIRY || "2025-10-14";

  const row = await computeRowFromDB(MONGO, DB, UNDERLYING, EXPIRY);
  console.log(row);
})();
