// src/api/oc_rows_bulk.api.ts
import type { Express, Request, Response, NextFunction } from "express";
import type { Db } from "mongodb";
import crypto from "crypto";

/* =================== Constants (IST Session) =================== */
const IST_OFFSET_MIN = 330; // +05:30
const IST_OFFSET_MS = IST_OFFSET_MIN * 60_000;

const SESSION_START_MIN = 9 * 60 + 15; // 09:15 IST
const SESSION_END_MIN   = 15 * 60 + 30; // 15:30 IST

/** Valid intervals we support */
const ALLOWED_INTERVALS = new Set<number>([3, 5, 15, 30]);

/* =================== Types =================== */
type CacheDoc = {
  underlying_security_id: number;
  underlying_segment: string;
  expiry: string;
  intervalMin: number;
  mode: "level" | "delta";
  unit: "bps" | "pct" | "points";
  tsBucket: Date;           // bucket START in UTC (as stored)
  time: string;             // stored human label (unused for output)
  volatility: number;
  signal: "Bullish" | "Bearish";
  spot: number;
  updated_at?: Date;
};

type RowOut = Pick<
  CacheDoc,
  "volatility" | "time" | "signal" | "spot" | "tsBucket" | "updated_at"
>;

/* =================== Collections =================== */
const CACHE_COLL = "oc_rows_cache";
const TICKS_COLL = process.env.OC_SOURCE_COLL || "option_chain_ticks";

/* =================== Time helpers (IST) =================== */
const dtfIST = new Intl.DateTimeFormat("en-IN", {
  timeZone: "Asia/Kolkata",
  hour12: false,
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
});
function timeIST(d: Date) {
  return dtfIST.format(d) + " IST";
}
function istDayStartMs(utcMs: number) {
  const dayMs = 86_400_000;
  const istMs = utcMs + IST_OFFSET_MS;
  return Math.floor(istMs / dayMs) * dayMs;
}

/**
 * Given any UTC timestamp (usually the stored tsBucket/start),
 * return the **canonical candle END** for that interval on the same trading day:
 *  - Grid starts at 09:15 IST
 *  - First candle end is fixed (e.g., 09:18 for 3m)
 *  - Uses CEIL to snap forward to the next boundary
 *  - Clamped to 15:30 IST
 */
function canonicalBucketEndUTCFromStartUTC(tsBucketUtc: Date, intervalMin: number): Date {
  const intervalMs = Math.max(1, intervalMin) * 60_000;
  const utcMs = tsBucketUtc.getTime();

  const dayStartIst     = istDayStartMs(utcMs);
  const sessionStartIst = dayStartIst + SESSION_START_MIN * 60_000; // 09:15 IST
  const sessionEndIst   = dayStartIst + SESSION_END_MIN   * 60_000; // 15:30 IST

  // Work in IST to align to human grid
  const istMs  = utcMs + IST_OFFSET_MS;
  const delta  = istMs - sessionStartIst;

  // Snap to the NEXT boundary (ceil). Ensure at least one step past 09:15
  // so the first close is 09:18/09:20/09:30/09:45 (never 09:15).
  const k = Math.max(1, Math.ceil(delta / intervalMs));
  const alignedEndIst = Math.min(sessionStartIst + k * intervalMs, sessionEndIst);

  // Convert back to UTC
  return new Date(alignedEndIst - IST_OFFSET_MS);
}

/* =================== Expiry resolver =================== */
async function resolveActiveExpiry(
  db: Db,
  underlying: number,
  segment: string
): Promise<string | null> {
  // Prefer latest snapshot first
  const snap = await db
    .collection("option_chain")
    .find({ underlying_security_id: underlying, underlying_segment: segment } as any)
    .project({ expiry: 1, updated_at: 1 })
    .sort({ updated_at: -1 })
    .limit(1)
    .toArray();

  if (snap.length && (snap[0] as any)?.expiry) {
    return String((snap[0] as any).expiry);
  }

  // Fallback: latest tick
  const tick = await db
    .collection(TICKS_COLL)
    .find({ underlying_security_id: underlying, underlying_segment: segment } as any)
    .project({ expiry: 1, ts: 1 })
    .sort({ ts: -1 })
    .limit(1)
    .toArray();

  if (tick.length && (tick[0] as any)?.expiry) {
    return String((tick[0] as any).expiry);
  }

  return null;
}

/* =================== ETag builder =================== */
function buildEtag(payload: {
  underlying: number;
  segment: string;
  expiry: string;
  mode: string;
  unit: string;
  rowsByInterval: Record<string, RowOut[]>;
}) {
  let maxTs = 0;
  let total = 0;
  for (const key of Object.keys(payload.rowsByInterval)) {
    const arr = payload.rowsByInterval[key] || [];
    total += arr.length;
    for (const d of arr) {
      const t = (d.updated_at ? new Date(d.updated_at) : new Date(d.tsBucket)).getTime();
      if (t > maxTs) maxTs = t;
    }
  }
  const basis = JSON.stringify({
    u: payload.underlying,
    s: payload.segment,
    e: payload.expiry,
    m: payload.mode,
    un: payload.unit,
    total,
    maxTs,
    keys: Object.fromEntries(
      Object.entries(payload.rowsByInterval).map(([k, v]) => [k, v.length])
    ),
  });
  return `"vi-${crypto.createHash("md5").update(basis).digest("hex")}"`;
}

/* =================== Route =================== */
export default function registerOcRowsBulk(app: Express, db: Db) {
  app.get(
    "/api/oc/rows/bulk",
    async (req: Request, res: Response, next: NextFunction) => {
      try {
        // Query params
        const underlying = Number(req.query.underlying ?? 13);
        const segment    = String(req.query.segment ?? "IDX_I");
        const mode       = String(req.query.mode ?? "level") as "level" | "delta";
        const unit       = String(req.query.unit ?? "bps")   as "bps"   | "pct" | "points";
        const sinceMin   = Number(req.query.sinceMin ?? 390); // ~full trading day
        const limit      = Math.max(50, Math.min(5000, Number(req.query.limit ?? 2000)));

        // Intervals
        const rawIntervals = String(req.query.intervals ?? "3,5,15,30")
          .split(",")
          .map((x) => Number(x.trim()))
          .filter((n) => ALLOWED_INTERVALS.has(n));
        const intervals = rawIntervals.length ? rawIntervals : [3];

        // Expiry
        const expiryParam = String(req.query.expiry ?? "auto").trim();
        const expiry =
          expiryParam.toLowerCase() === "auto"
            ? (await resolveActiveExpiry(db, underlying, segment)) || "NA"
            : expiryParam;

        if (!expiry || expiry === "NA") {
          res.setHeader("X-Resolved-Expiry", "NA");
          res.json({
            expiry: "NA",
            rows: Object.fromEntries(intervals.map((i) => [String(i), []])),
          });
          return;
        }

        // ---------------------------
        // Cutoff logic (modified)
        // - During market hours: preserve original sinceMin behavior
        // - Outside market hours (before 09:15 IST or after 15:30 IST): expand to 24 hours
        // ---------------------------
        const MAX_LOOKBACK_MS = 24 * 60 * 60 * 1000; // 24 hours

        const nowUtc = new Date();
        const nowIstMs = nowUtc.getTime() + IST_OFFSET_MS;
        const istDayStart = istDayStartMs(nowUtc.getTime());
        const sessionStartIst = istDayStart + SESSION_START_MIN * 60_000; // 09:15 IST
        const sessionEndIst   = istDayStart + SESSION_END_MIN   * 60_000; // 15:30 IST

        // Default (preserve existing behavior during market hours)
        let cutoffMs = Date.now() - Math.max(1, sinceMin) * 60_000;

        // If we're outside the trading session (after 15:30 IST or before 09:15 IST),
        // expand the window to 24 hours so the UI keeps showing the last available data
        // until new data arrives or the market re-opens.
        if (nowIstMs >= sessionEndIst || nowIstMs < sessionStartIst) {
          cutoffMs = Date.now() - MAX_LOOKBACK_MS;
        }

        const cutoff = new Date(cutoffMs);

        const coll = db.collection<CacheDoc>(CACHE_COLL);

        const byInterval: Record<string, RowOut[]> = {};

        await Promise.all(
          intervals.map(async (intervalMin) => {
            const docs = (await coll
              .find({
                underlying_security_id: underlying,
                underlying_segment: segment,
                expiry,
                intervalMin,
                mode,
                unit,
                tsBucket: { $gte: cutoff },
              } as any)
              .project({
                volatility: 1,
                time: 1,          // stored string; we re-label below
                signal: 1,
                spot: 1,
                tsBucket: 1,
                updated_at: 1,
                _id: 0,
              } as any)
              .sort({ tsBucket: -1 })
              .limit(limit)
              .toArray()) as unknown as RowOut[];

            byInterval[String(intervalMin)] = docs;
          })
        );

        // ETag handling
        const etag = buildEtag({
          underlying,
          segment,
          expiry,
          mode,
          unit,
          rowsByInterval: byInterval,
        });

        const ifNoneMatch = req.headers["if-none-match"];
        if (ifNoneMatch && ifNoneMatch === etag) {
          res.status(304).end();
          return;
        }

        res.setHeader("ETag", etag);
        res.setHeader("X-Resolved-Expiry", expiry);
        res.setHeader("Cache-Control", "no-store");

        // Shape rows for client: label by **canonical** IST-aligned BUCKET END
        const rows = Object.fromEntries(
          Object.entries(byInterval).map(([k, v]) => {
            const intervalMin = Number(k);
            return [
              k,
              v.map((d) => {
                const bucketStartUtc = new Date(d.tsBucket as unknown as Date);
                const bucketEndUtc   = canonicalBucketEndUTCFromStartUTC(bucketStartUtc, intervalMin);
                return {
                  volatility: d.volatility,
                  time: timeIST(bucketEndUtc), // e.g., 09:18 / ... / 15:30
                  signal: d.signal,
                  spot: d.spot,
                };
              }),
            ];
          })
        );

        res.json({ expiry, rows });
      } catch (err) {
        next(err);
      }
    }
  );
}