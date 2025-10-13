// src/api/oc_row.api.ts
import type { Express, RequestHandler } from "express";
import { computeRowFromDB, computeRowsFromDBWindow } from "../services/oc_signal";

export default function registerOcRow(app: Express) {
  // Single latest row (PUBLIC)
  const singleHandler: RequestHandler = async (req, res) => {
    try {
      const mongoUri = process.env.MONGO_URI || "mongodb://localhost:27017";
      const dbName   = process.env.MONGO_DB_NAME || process.env.DB_NAME || "Upholic";

      const underlying = Number(req.query.underlying ?? 13);
      const expiry     = String(req.query.expiry || "");
      const unitParam  = String(req.query.unit ?? "bps").toLowerCase();
      const unit: "bps" | "pct" | "points" =
        unitParam === "points" ? "points" : unitParam === "pct" ? "pct" : "bps";
      const signalMode = String(req.query.signalMode ?? "price") as "price" | "delta" | "hybrid";

      if (!expiry) {
        res.status(400).json({ error: "expiry is required (YYYY-MM-DD)" });
        return;
      }

      const row = await computeRowFromDB(mongoUri, dbName, underlying, expiry, unit, signalMode);
      if (!row) {
        res.status(404).json({ error: "no option_chain docs found" });
        return;
      }

      res.json(row);
    } catch (e: any) {
      res.status(500).json({ error: "server_error", detail: String(e?.message || e) });
    }
  };

  // Multi rows (PUBLIC) — bucketed by intervalMin, NO liquidity fields returned
  const multiHandler: RequestHandler = async (req, res) => {
    try {
      const mongoUri = process.env.MONGO_URI || "mongodb://localhost:27017";
      const dbName   = process.env.MONGO_DB_NAME || process.env.DB_NAME || "Upholic";

      const underlying = Number(req.query.underlying ?? 13);
      const expiry     = String(req.query.expiry || "");
      const limit      = Math.min(200, Math.max(2, Number(req.query.limit ?? 12)));

      const mode       = (String(req.query.mode ?? "level") === "delta") ? "delta" : "level";
      const unitParam  = String(req.query.unit ?? "bps").toLowerCase();
      const unit: "bps" | "pct" | "points" =
        unitParam === "points" ? "points" : unitParam === "pct" ? "pct" : "bps";
      const signalMode = String(req.query.signalMode ?? "price") as "price" | "delta" | "hybrid";

      const windowSteps = Number.isFinite(Number(req.query.windowSteps))
        ? Math.max(1, Number(req.query.windowSteps))
        : 5;
      const width       = Number.isFinite(Number(req.query.width))
        ? Math.max(50, Number(req.query.width))
        : 300;
      const classify    = String(req.query.classify ?? "1") !== "0";
      const intervalMin = Math.max(1, Math.min(30, Number(req.query.intervalMin || 3)));

      if (!expiry) {
        res.status(400).json({ error: "expiry is required (YYYY-MM-DD)" });
        return;
      }

      const rows = await computeRowsFromDBWindow(
        mongoUri,
        dbName,
        underlying,
        expiry,
        limit,
        { mode, unit, signalMode, windowSteps, width, classify, intervalMin }
      );

      if (!rows.length) {
        res.status(404).json({ error: "not enough ticks for this filter" });
        return;
      }

      // Already free of liquidity fields; send as-is
      res.json(rows);
    } catch (e: any) {
      res.status(500).json({ error: "server_error", detail: String(e?.message || e) });
    }
  };

  app.get("/api/oc/row", singleHandler);
  app.get("/api/oc/rows", multiHandler);
}
