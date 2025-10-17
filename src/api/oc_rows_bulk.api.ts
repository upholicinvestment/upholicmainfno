import type { Express, Request, Response, NextFunction } from "express";
import type { Db } from "mongodb";
import crypto from "crypto";

/** Valid intervals we support */
const ALLOWED_INTERVALS = new Set<number>([3, 5, 15, 30]);

type CacheDoc = {
  underlying_security_id: number;
  underlying_segment: string;
  expiry: string;
  intervalMin: number;
  mode: "level" | "delta";
  unit: "bps" | "pct" | "points";
  tsBucket: Date;           // bucket START in UTC
  time: string;             // (stored) human readable, but we will recompute
  volatility: number;
  signal: "Bullish" | "Bearish";
  spot: number;
  updated_at?: Date;
};

type RowOut = Pick<
  CacheDoc,
  "volatility" | "time" | "signal" | "spot" | "tsBucket" | "updated_at"
>;

const CACHE_COLL = "oc_rows_cache";
const TICKS_COLL = process.env.OC_SOURCE_COLL || "option_chain_ticks";

// ---- IST formatting helpers ----
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

/** Resolve active expiry from latest option_chain or ticks */
async function resolveActiveExpiry(
  db: Db,
  underlying: number,
  segment: string
): Promise<string | null> {
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

/** Build a stable ETag using counts, max ts, and identity */
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

/** GET /api/oc/rows/bulk */
export default function registerOcRowsBulk(app: Express, db: Db) {
  app.get(
    "/api/oc/rows/bulk",
    async (req: Request, res: Response, next: NextFunction) => {
      try {
        // Query params
        const underlying = Number(req.query.underlying ?? 13);
        const segment = String(req.query.segment ?? "IDX_I");
        const mode = String(req.query.mode ?? "level") as "level" | "delta";
        const unit = String(req.query.unit ?? "bps") as "bps" | "pct" | "points";
        const sinceMin = Number(req.query.sinceMin ?? 390); // ~full trading day
        const limit = Math.max(50, Math.min(5000, Number(req.query.limit ?? 2000)));

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

        const cutoff = new Date(Date.now() - Math.max(1, sinceMin) * 60_000);
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
                time: 1,          // stored, but we will recompute a proper label
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

        // Shape rows for client: label by IST-aligned BUCKET END
        const rows = Object.fromEntries(
          Object.entries(byInterval).map(([k, v]) => {
            const intervalMin = Number(k);
            return [
              k,
              v.map((d) => {
                const bucketStart = new Date(d.tsBucket as unknown as Date);
                const bucketEnd = new Date(bucketStart.getTime() + intervalMin * 60_000);
                return {
                  volatility: d.volatility,
                  time: timeIST(bucketEnd),  // <<< correct label
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
