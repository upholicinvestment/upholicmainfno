// src/api/oc_row.api.ts
import type { Express, RequestHandler } from "express";
import { MongoClient } from "mongodb";
import { computeRowFromDB, computeRowsFromDBWindow } from "../services/oc_signal";

/** Resolve “active” expiry from DB if none/auto is provided */
async function resolveActiveExpiry(
  mongoUri: string,
  dbName: string,
  underlying: number,
  segment = "IDX_I"
): Promise<string | null> {
  const client = new MongoClient(mongoUri);
  try {
    await client.connect();
    const db = client.db(dbName);

    // Prefer latest snapshot first
    const snap = await db
      .collection("option_chain")
      .find({ underlying_security_id: underlying, underlying_segment: segment })
      .project({ expiry: 1, updated_at: 1 })
      .sort({ updated_at: -1 })
      .limit(1)
      .toArray();

    if (snap.length && snap[0]?.expiry) return String(snap[0].expiry);

    // Fallback: latest tick
    const tick = await db
      .collection("option_chain_ticks")
      .find({ underlying_security_id: underlying, underlying_segment: segment })
      .project({ expiry: 1, ts: 1 })
      .sort({ ts: -1 })
      .limit(1)
      .toArray();

    if (tick.length && tick[0]?.expiry) return String(tick[0].expiry);

    return null;
  } finally {
    try { await client.close(); } catch {}
  }
}

export default function registerOcRow(app: Express) {
  // Single latest row (PUBLIC)
  const singleHandler: RequestHandler = async (req, res) => {
    try {
      const mongoUri = process.env.MONGO_URI || "mongodb://localhost:27017";
      const dbName   = process.env.MONGO_DB_NAME || process.env.DB_NAME || "Upholic";

      const underlying = Number(req.query.underlying ?? 13);
      const seg        = String(req.query.segment || "IDX_I");
      let   expiry     = String(req.query.expiry || "");
      const unitParam  = String(req.query.unit ?? "bps").toLowerCase();
      const unit: "bps" | "pct" | "points" =
        unitParam === "points" ? "points" : unitParam === "pct" ? "pct" : "bps";
      const signalMode = String(req.query.signalMode ?? "price") as "price" | "delta" | "hybrid";

      // Auto/empty expiry resolution
      if (!expiry || expiry.toLowerCase() === "auto" || expiry.toLowerCase() === "latest") {
        const resolved = await resolveActiveExpiry(mongoUri, dbName, underlying, seg);
        if (!resolved) {
          res.status(404).json({ error: "no_active_expiry", detail: "Could not resolve active expiry from DB" });
          return;
        }
        expiry = resolved;
      }

      const row = await computeRowFromDB(mongoUri, dbName, underlying, expiry, unit, signalMode);
      if (!row) {
        res.status(404).json({ error: "no_rows", detail: "No option_chain ticks for this filter" });
        return;
      }

      res.setHeader("Cache-Control", "no-store");
      res.setHeader("X-Resolved-Expiry", expiry);
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
      const seg        = String(req.query.segment || "IDX_I");
      let   expiry     = String(req.query.expiry || "");

      // ⬇️ REMOVE default `?? 12` and show "all" by default.
      // If client passes a limit, we’ll respect it; otherwise use a very large number.
      const limitParam = Number(req.query.limit);
      const limit = Number.isFinite(limitParam) ? Math.max(1, limitParam) : 1_000_000;

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

      // Auto/empty expiry resolution
      if (!expiry || expiry.toLowerCase() === "auto" || expiry.toLowerCase() === "latest") {
        const resolved = await resolveActiveExpiry(mongoUri, dbName, underlying, seg);
        if (!resolved) {
          res.status(404).json({ error: "no_active_expiry", detail: "Could not resolve active expiry from DB" });
          return;
        }
        expiry = resolved;
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
        res.status(404).json({
          error: "no_rows",
          detail: "No ticks for this expiry/interval filter",
          meta: { expiry, intervalMin, windowSteps, classify }
        });
        return;
      }

      res.setHeader("Cache-Control", "no-store");
      res.setHeader("X-Resolved-Expiry", expiry);
      res.json(rows);
    } catch (e: any) {
      res.status(500).json({ error: "server_error", detail: String(e?.message || e) });
    }
  };

  app.get("/api/oc/row", singleHandler);
  app.get("/api/oc/rows", multiHandler);
}
