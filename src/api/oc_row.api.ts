// src/api/oc_rows.api.ts
import type { Express, RequestHandler } from "express";
import type { Db } from "mongodb";
import { computeRowFromDB, computeRowsFromDBWindow } from "../services/oc_signal";

/**
 * NOTE: change signature to accept an already-connected `Db` (fnoDb).
 * In appnew.ts you should call: registerOcRow(app, fnoDb)
 */

/** Resolve “active” expiry using the passed DB (no new MongoClient) */
async function resolveActiveExpiry(
  db: Db,
  underlying: number,
  segment = "IDX_I"
): Promise<string | null> {
  try {
    // Prefer latest snapshot first
    const snap = await db
      .collection("option_chain")
      .find({ underlying_security_id: underlying, underlying_segment: segment } as any)
      .project({ expiry: 1, updated_at: 1 })
      .sort({ updated_at: -1 })
      .limit(1)
      .toArray();

    if (snap.length && (snap[0] as any)?.expiry) return String((snap[0] as any).expiry);

    // Fallback: latest tick from option_chain_ticks
    const tick = await db
      .collection(process.env.OC_SOURCE_COLL || "option_chain_ticks")
      .find({ underlying_security_id: underlying, underlying_segment: segment } as any)
      .project({ expiry: 1, ts: 1 })
      .sort({ ts: -1 })
      .limit(1)
      .toArray();

    if (tick.length && (tick[0] as any)?.expiry) return String((tick[0] as any).expiry);

    return null;
  } catch (e) {
    // swallow and return null - caller handles the 404
    return null;
  }
}

/**
 * registerOcRow now requires the DB the caller wants to use (prefer fnoDb).
 */
export default function registerOcRow(app: Express, db: Db) {
  // Helper to pick correct URI/dbName to pass into computeRowFromDB(...) helpers.
  // If the passed db matches your FNO DB name env, prefer MONGO_URII/MONGO_DB_NAMEE.
  function pickMongoUriAndDbName(): { mongoUri: string; dbName: string } {
    const passedDbName = db?.databaseName || "";
    const fnoName = process.env.MONGO_DB_NAMEE || "";
    if (passedDbName && fnoName && passedDbName === fnoName) {
      const uri = process.env.MONGO_URII || process.env.MONGO_URI || "mongodb://localhost:27017";
      return { mongoUri: uri, dbName: passedDbName };
    }
    // default to primary env
    return {
      mongoUri: process.env.MONGO_URI || "mongodb://localhost:27017",
      dbName: process.env.MONGO_DB_NAME || passedDbName || "Upholic",
    };
  }

  // Single latest row (PUBLIC)
  const singleHandler: RequestHandler = async (req, res) => {
    try {
      const { mongoUri, dbName } = pickMongoUriAndDbName();

      const underlying = Number(req.query.underlying ?? 13);
      const seg        = String(req.query.segment || "IDX_I");
      let   expiry     = String(req.query.expiry || "");
      const unitParam  = String(req.query.unit ?? "bps").toLowerCase();
      const unit: "bps" | "pct" | "points" =
        unitParam === "points" ? "points" : unitParam === "pct" ? "pct" : "bps";
      const signalMode = String(req.query.signalMode ?? "price") as "price" | "delta" | "hybrid";

      // Auto/empty expiry resolution using the passed `db`
      if (!expiry || expiry.toLowerCase() === "auto" || expiry.toLowerCase() === "latest") {
        const resolved = await resolveActiveExpiry(db, underlying, seg);
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

  // Multi rows (PUBLIC) — bucketed by intervalMin
  const multiHandler: RequestHandler = async (req, res) => {
    try {
      const { mongoUri, dbName } = pickMongoUriAndDbName();

      const underlying = Number(req.query.underlying ?? 13);
      const seg        = String(req.query.segment || "IDX_I");
      let   expiry     = String(req.query.expiry || "");

      // Show "all" by default unless a limit is provided
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

      // Auto/empty expiry resolution using the passed `db`
      if (!expiry || expiry.toLowerCase() === "auto" || expiry.toLowerCase() === "latest") {
        const resolved = await resolveActiveExpiry(db, underlying, seg);
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
