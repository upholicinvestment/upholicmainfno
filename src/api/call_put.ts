// src/api/call_put.ts
import type { Express, Request, Response, RequestHandler } from "express";
import type { Db } from "mongodb";
import crypto from "crypto";

/* ----------------------------- Types ------------------------------ */
type Leg = { oi?: number | null };

type StrikeRow = {
  strike: number;
  ce?: Leg | null;
  pe?: Leg | null;
};

type TickDoc = {
  underlying_security_id: number;
  underlying_segment: string;
  expiry: string;           // ISO yyyy-mm-dd
  ts: Date;                 // tick timestamp (UTC)
  last_price: number;       // underlying LTP
  strikes: StrikeRow[];     // normalized strikes
};

/* --------------------------- Utilities ---------------------------- */
const LOG = (process.env.OC_LOG_VERBOSE ?? "true").toLowerCase() === "true";
const log = (...a: unknown[]) => { if (LOG) console.log("[OI]", ...a); };

const toISO = (d: Date | string | number) => new Date(d).toISOString();

function detectStrikeStep(rows: StrikeRow[]): number {
  const uniques = Array.from(
    new Set(rows.map(r => Number(r?.strike)).filter(Number.isFinite))
  ).sort((a, b) => a - b);

  for (let i = 1; i < uniques.length; i++) {
    const diff = Math.abs(uniques[i] - uniques[i - 1]);
    if (diff > 0) return diff;
  }
  return 50; // sensible default for NIFTY
}

const roundToStep = (px: number, step: number) => Math.round(px / step) * step;
const minutesToMs = (min: number) => Math.max(1, Math.floor(min)) * 60 * 1000;

// ----- IST + session-anchored bucketing (anchor = 09:15 IST) -----
const IST_OFFSET_MIN = 330; // +05:30
const IST_OFFSET_MS = IST_OFFSET_MIN * 60_000;
const DAY_MS = 86_400_000;
const SESSION_START_MIN = 9 * 60 + 15; // 09:15 IST

function istDayStartMs(utcMs: number) {
  const istMs = utcMs + IST_OFFSET_MS;
  return Math.floor(istMs / DAY_MS) * DAY_MS;
}

function floorToSessionBucketStartUTC(dUTC: Date, intervalMin: number): Date {
  const intervalMs = Math.max(1, intervalMin) * 60_000;
  const utcMs = dUTC.getTime();
  const dayStartIst = istDayStartMs(utcMs);
  const sessionAnchorIst = dayStartIst + SESSION_START_MIN * 60_000;
  const istNow = utcMs + IST_OFFSET_MS;
  const k = Math.floor((istNow - sessionAnchorIst) / intervalMs);
  const startIst = sessionAnchorIst + k * intervalMs;
  return new Date(startIst - IST_OFFSET_MS);
}

function ceilToSessionBucketEndUTC(dUTC: Date, intervalMin: number): Date {
  const intervalMs = Math.max(1, intervalMin) * 60_000;
  const utcMs = dUTC.getTime();
  const dayStartIst = istDayStartMs(utcMs);
  const sessionAnchorIst = dayStartIst + SESSION_START_MIN * 60_000;
  const istNow = utcMs + IST_OFFSET_MS;
  const k = Math.ceil((istNow - sessionAnchorIst) / intervalMs);
  const endIst = sessionAnchorIst + k * intervalMs;
  return new Date(endIst - IST_OFFSET_MS);
}

// IST-aligned “trading day” window derived from a UTC timestamp
function tradingWindowBySinceMin(latestTs: Date, sinceMin: number): { start: Date; end: Date } {
  // We keep it simple: `sinceMin` minutes back from latest, up to latest.
  const end = new Date(latestTs);
  const start = new Date(end.getTime() - minutesToMs(sinceMin));
  return { start, end };
}

/* ---------------------- Micro-cache + coalescing ------------------ */
type CacheEntry<T> = { exp: number; etag: string; data: T };
const microCache = new Map<string, CacheEntry<any>>();
const inFlight = new Map<string, Promise<CacheEntry<any>>>();

const SERIES_TTL_MS = Number(process.env.OC_SERIES_TTL_MS ?? 5_000); // small TTL prevents DB stampede
const BULK_TTL_MS   = Number(process.env.OC_BULK_TTL_MS   ?? 7_000);

// Cheap, stable ETag from a small basis to avoid hashing full arrays
function makeEtag(basis: unknown): string {
  const raw = typeof basis === "string" ? basis : JSON.stringify(basis);
  const md5 = crypto.createHash("md5").update(raw).digest("hex");
  return `"oi-${md5}"`;
}

async function cached<T>(
  key: string,
  ttlMs: number,
  compute: () => Promise<{ data: T; etagBasis?: unknown }>
): Promise<CacheEntry<T>> {
  const now = Date.now();
  const hit = microCache.get(key);
  if (hit && hit.exp > now) return hit;

  // coalesce concurrent callers
  const inflight = inFlight.get(key);
  if (inflight) return inflight as Promise<CacheEntry<T>>;

  const p = (async () => {
    const { data, etagBasis } = await compute();
    const etag = makeEtag(etagBasis ?? data);
    const entry: CacheEntry<T> = { data, etag, exp: now + ttlMs };
    microCache.set(key, entry);
    inFlight.delete(key);
    return entry;
  })().catch((e) => {
    inFlight.delete(key);
    throw e;
  });

  inFlight.set(key, p);
  return p;
}

function serveCachedJSON<T>(req: Request, res: Response, entry: CacheEntry<T>): void {
  res.setHeader("ETag", entry.etag);
  res.setHeader("Cache-Control", "private, max-age=3, stale-while-revalidate=30");
  const inm = req.headers["if-none-match"];
  if (inm && inm === entry.etag) {
    res.status(304).end();
    return;
  }
  res.json(entry.data);
}

/* --------------------------- Index helpers ------------------------ */
async function ensureTickIndexes(db: Db): Promise<void> {
  const col = db.collection<TickDoc>("option_chain_ticks");
  // Covers: latest lookup (sort ts:-1) without expiry in filter
  // Query: { underlying_security_id, underlying_segment } sort { ts:-1 } limit 1
  // Index: prefix equality fields, then sort field desc
  const latestIdx = {
    name: "octicks_latest_lookup",
    key: { underlying_security_id: 1, underlying_segment: 1, ts: -1 },
  };

  // Covers: equality on underlying + segment + expiry, range on ts, sort ts:1
  // Query: { underlying_security_id, underlying_segment, expiry, ts: {$gte,$lt} } sort { ts:1 }
  const windowIdx = {
    name: "octicks_underlying_expiry_ts",
    key: { underlying_security_id: 1, underlying_segment: 1, expiry: 1, ts: 1 },
  };

  // Optional helper if you sometimes scan by expiry alone (kept small)
  const expiryIdx = {
    name: "octicks_expiry_ts",
    key: { expiry: 1, ts: 1 },
  };

  try {
    await col.createIndexes([latestIdx, windowIdx, expiryIdx]);
    log("✅ option_chain_ticks indexes ensured");
  } catch (e: any) {
    console.error("❌ Failed to create indexes on option_chain_ticks:", e?.message || e);
  }
}

/* --------------------------- Route impls -------------------------- */
export default function registerNiftyRoutes(app: Express, db: Db) {
  const ticksCol = db.collection<TickDoc>("option_chain_ticks");

  // ensure indexes once on boot
  // fire-and-forget is fine; queries still work even if this is building
  void ensureTickIndexes(db);

  /** ============= Fixed ATM endpoint ============= */
  const atmHandler: RequestHandler = async (req: Request, res: Response): Promise<void> => {
    try {
      const intervalMin = Math.max(1, parseInt(String(req.query.interval || "3"), 10));
      const id = Number(process.env.OC_UNDERLYING_ID ?? 13);
      const seg = String(process.env.OC_SEGMENT ?? "IDX_I");

      const cacheKey = `atm:${id}:${seg}:i${intervalMin}`;
      const entry = await cached(cacheKey, SERIES_TTL_MS, async () => {
        const latestArr = await ticksCol
          .find({ underlying_security_id: id, underlying_segment: seg })
          .sort({ ts: -1 }) // uses octicks_latest_lookup
          .limit(1)
          .toArray();

        if (!latestArr.length) {
          return { data: { error: "No option_chain_ticks for underlying/segment." } as any, etagBasis: "empty" };
        }
        const latest = latestArr[0];
        const { start, end } = tradingWindowBySinceMin(latest.ts, 24 * 60);

        const docs = await ticksCol
          .find(
            {
              underlying_security_id: id,
              underlying_segment: seg,
              expiry: latest.expiry,
              ts: { $gte: start, $lt: end },
            },
            { projection: { ts: 1, last_price: 1, strikes: 1, expiry: 1 } as const }
          )
          .sort({ ts: 1 }) // uses octicks_underlying_expiry_ts
          .toArray();

        if (!docs.length) {
          return { data: { error: "No ticks found in the date window." } as any, etagBasis: "empty-window" };
        }

        // step + fixed ATM at latest
        let step = 50;
        for (const d of docs) {
          if (Array.isArray(d.strikes) && d.strikes.length) { step = detectStrikeStep(d.strikes); break; }
        }
        const fixedAtm = roundToStep(latest.last_price, step);
        log("ATM window (fixed)", { expiry: latest.expiry, step, fixedAtm, start: toISO(start), end: toISO(end) });

        type AtmBin = { ts: Date; callOI: number; putOI: number }; // ts = BUCKET END (UTC)
        const bins = new Map<number, AtmBin>();

        let lastCE = 0;
        let lastPE = 0;

        for (const doc of docs) {
          const startUTC = floorToSessionBucketStartUTC(new Date(doc.ts), intervalMin);
          const endUTC = ceilToSessionBucketEndUTC(new Date(doc.ts), intervalMin);
          const key = startUTC.getTime();

          let ceOI = lastCE;
          let peOI = lastPE;

          if (Array.isArray(doc.strikes)) {
            const row = (doc.strikes as StrikeRow[]).find(r => r?.strike === fixedAtm);
            if (row) {
              const c = Number(row?.ce?.oi ?? NaN);
              const p = Number(row?.pe?.oi ?? NaN);
              if (Number.isFinite(c)) ceOI = c;
              if (Number.isFinite(p)) peOI = p;
            }
          }

          lastCE = ceOI;
          lastPE = peOI;
          bins.set(key, { ts: endUTC, callOI: ceOI, putOI: peOI });
        }

        const series = Array.from(bins.entries())
          .sort((a, b) => a[0] - b[0])
          .map(([, v]) => ({
            timestamp: v.ts.toISOString(), // bucket END
            atmStrike: fixedAtm,
            callOI: v.callOI,
            putOI: v.putOI,
          }));

        const data = { expiry: latest.expiry, step, atmStrike: fixedAtm, series };
        const etagBasis = { exp: latest.expiry, last: series.at(-1)?.timestamp ?? "", count: series.length, atm: fixedAtm, step };
        return { data, etagBasis };
      });

      if ("error" in (entry.data as any)) {
        res.status(404);
      }
      serveCachedJSON(req, res, entry);
    } catch (e: any) {
      console.error("ATM handler error:", e?.message || e);
      res.status(500).json({ error: "Internal server error" });
    }
  };

  /** ============= OVERALL endpoint ============= */
  const overallHandler: RequestHandler = async (req: Request, res: Response): Promise<void> => {
    try {
      const intervalMin = Math.max(1, parseInt(String(req.query.interval || "3"), 10));
      const id = Number(process.env.OC_UNDERLYING_ID ?? 13);
      const seg = String(process.env.OC_SEGMENT ?? "IDX_I");

      const cacheKey = `overall:${id}:${seg}:i${intervalMin}`;
      const entry = await cached(cacheKey, SERIES_TTL_MS, async () => {
        const latestArr = await ticksCol
          .find({ underlying_security_id: id, underlying_segment: seg })
          .sort({ ts: -1 }) // uses octicks_latest_lookup
          .limit(1)
          .toArray();

        if (!latestArr.length) {
          return { data: { error: "No option_chain_ticks for underlying/segment." } as any, etagBasis: "empty" };
        }
        const latest = latestArr[0];
        const { start, end } = tradingWindowBySinceMin(latest.ts, 24 * 60);

        const docs = await ticksCol
          .find(
            {
              underlying_security_id: id,
              underlying_segment: seg,
              expiry: latest.expiry,
              ts: { $gte: start, $lt: end },
            },
            { projection: { ts: 1, strikes: 1, expiry: 1 } as const }
          )
          .sort({ ts: 1 }) // uses octicks_underlying_expiry_ts
          .toArray();

        if (!docs.length) {
          return { data: { error: "No ticks found in the date window." } as any, etagBasis: "empty-window" };
        }

        type TotalBin = { ts: Date; callOI: number; putOI: number }; // ts = BUCKET END (UTC)
        const bins = new Map<number, TotalBin>();

        for (const doc of docs) {
          const startUTC = floorToSessionBucketStartUTC(new Date(doc.ts), intervalMin);
          const endUTC = ceilToSessionBucketEndUTC(new Date(doc.ts), intervalMin);
          const key = startUTC.getTime();

          const strikes = Array.isArray(doc.strikes) ? (doc.strikes as StrikeRow[]) : [];
          const sumCE = strikes.reduce<number>((acc, r) => acc + (Number(r?.ce?.oi ?? 0) || 0), 0);
          const sumPE = strikes.reduce<number>((acc, r) => acc + (Number(r?.pe?.oi ?? 0) || 0), 0);

          bins.set(key, { ts: endUTC, callOI: sumCE, putOI: sumPE });
        }

        const step = (() => {
          for (const d of docs) if (Array.isArray(d.strikes) && d.strikes.length) return detectStrikeStep(d.strikes);
          return 50;
        })();

        const series = Array.from(bins.entries())
          .sort((a, b) => a[0] - b[0])
          .map(([, v]) => ({
            timestamp: v.ts.toISOString(), // bucket END
            callOI: v.callOI,
            putOI: v.putOI,
          }));

        const data = { expiry: latest.expiry, step, series };
        const etagBasis = { exp: latest.expiry, last: series.at(-1)?.timestamp ?? "", count: series.length };
        return { data, etagBasis };
      });

      if ("error" in (entry.data as any)) {
        res.status(404);
      }
      serveCachedJSON(req, res, entry);
    } catch (e: any) {
      console.error("OVERALL handler error:", e?.message || e);
      res.status(500).json({ error: "Internal server error" });
    }
  };

  /** ============= BULK endpoint (ETag + 24h backfill) ============= */
  const bulkHandler: RequestHandler = async (req: Request, res: Response): Promise<void> => {
    try {
      const id = Number(process.env.OC_UNDERLYING_ID ?? 13);
      const seg = String(process.env.OC_SEGMENT ?? "IDX_I");

      // intervals
      const rawIntervals = String(req.query.intervals ?? "3,15,30,60")
        .split(",")
        .map((x) => Number(x.trim()))
        .filter((n) => [3, 5, 15, 30, 60].includes(n));
      const intervals = rawIntervals.length ? Array.from(new Set(rawIntervals)).sort((a, b) => a - b) : [3, 15, 30, 60];

      // window: default 24h
      const sinceMin = Math.max(1, Number(req.query.sinceMin ?? 1440));

      const cacheKey = `bulk:${id}:${seg}:i[${intervals.join(",")}]:since${sinceMin}`;
      const entry = await cached(cacheKey, BULK_TTL_MS, async () => {
        // latest tick → expiry and window
        const latestArr = await ticksCol
          .find({ underlying_security_id: id, underlying_segment: seg })
          .sort({ ts: -1 }) // uses octicks_latest_lookup
          .limit(1)
          .toArray();

        if (!latestArr.length) {
          const data = { error: "No option_chain_ticks for underlying/segment." } as any;
          return { data, etagBasis: "empty" };
        }
        const latest = latestArr[0];
        const { start, end } = tradingWindowBySinceMin(latest.ts, sinceMin);

        // load once
        const docs = await ticksCol
          .find(
            {
              underlying_security_id: id,
              underlying_segment: seg,
              expiry: latest.expiry,
              ts: { $gte: start, $lt: end },
            },
            { projection: { ts: 1, last_price: 1, strikes: 1 } as const }
          )
          .sort({ ts: 1 }) // uses octicks_underlying_expiry_ts
          .toArray();

        if (!docs.length) {
          const data = { error: "No ticks found in the window." } as any;
          return { data, etagBasis: "empty-window" };
        }

        // step + fixed ATM
        let step = 50;
        for (const d of docs) {
          if (Array.isArray(d.strikes) && d.strikes.length) { step = detectStrikeStep(d.strikes); break; }
        }
        const fixedAtm = roundToStep(latest.last_price, step);

        type Pt = { timestamp: string; callOI: number; putOI: number };

        const buildAtmSeries = (binMin: number): Pt[] => {
          const bins = new Map<number, { ts: Date; callOI: number; putOI: number }>();
          let lastCE = 0;
          let lastPE = 0;

          for (const doc of docs) {
            const startUTC = floorToSessionBucketStartUTC(new Date(doc.ts), binMin);
            const endUTC = ceilToSessionBucketEndUTC(new Date(doc.ts), binMin);
            const key = startUTC.getTime();

            let ce = lastCE;
            let pe = lastPE;

            if (Array.isArray(doc.strikes)) {
              const row = (doc.strikes as StrikeRow[]).find(r => r?.strike === fixedAtm);
              if (row) {
                const c = Number(row?.ce?.oi ?? NaN);
                const p = Number(row?.pe?.oi ?? NaN);
                if (Number.isFinite(c)) ce = c;
                if (Number.isFinite(p)) pe = p;
              }
            }
            lastCE = ce;
            lastPE = pe;
            bins.set(key, { ts: endUTC, callOI: ce, putOI: pe });
          }

          return Array.from(bins.entries())
            .sort((a, b) => a[0] - b[0])
            .map(([, v]) => ({ timestamp: v.ts.toISOString(), callOI: v.callOI, putOI: v.putOI })); // bucket END
        };

        const buildOverallSeries = (binMin: number): Pt[] => {
          const bins = new Map<number, { ts: Date; callOI: number; putOI: number }>();

          for (const doc of docs) {
            const startUTC = floorToSessionBucketStartUTC(new Date(doc.ts), binMin);
            const endUTC = ceilToSessionBucketEndUTC(new Date(doc.ts), binMin);
            const key = startUTC.getTime();

            const strikes = Array.isArray(doc.strikes) ? (doc.strikes as StrikeRow[]) : [];
            const sumCE = strikes.reduce<number>((acc, r) => acc + (Number(r?.ce?.oi ?? 0) || 0), 0);
            const sumPE = strikes.reduce<number>((acc, r) => acc + (Number(r?.pe?.oi ?? 0) || 0), 0);
            bins.set(key, { ts: endUTC, callOI: sumCE, putOI: sumPE });
          }

          return Array.from(bins.entries())
            .sort((a, b) => a[0] - b[0])
            .map(([, v]) => ({ timestamp: v.ts.toISOString(), callOI: v.callOI, putOI: v.putOI })); // bucket END
        };

        const atmRows: Record<string, Pt[]> = {};
        const ovRows:  Record<string, Pt[]> = {};
        for (const m of intervals) {
          atmRows[String(m)] = buildAtmSeries(m);
          ovRows[String(m)]  = buildOverallSeries(m);
        }

        // ETag: stable hash over expiry + last ts + counts
        const lastTs = docs[docs.length - 1]?.ts?.toISOString() || "";
        const etagBasis = JSON.stringify({
          exp: latest.expiry,
          last: lastTs,
          atm: Object.fromEntries(Object.entries(atmRows).map(([k, v]) => [k, v.length])),
          ov:  Object.fromEntries(Object.entries(ovRows).map(([k, v]) => [k, v.length])),
          atmStrike: fixedAtm,
          step,
        });

        const data = {
          expiry: latest.expiry,
          atm: { step, atmStrike: fixedAtm, rows: atmRows },
          overall: { rows: ovRows },
        };

        return { data, etagBasis };
      });

      // Use cached ETag/Cache-Control
      serveCachedJSON(req, res, entry);
    } catch (e: any) {
      console.error("OI BULK error:", e?.message || e);
      res.status(500).json({ error: "Internal server error" });
    }
  };

  app.get("/api/nifty/atm", atmHandler);          // kept for backward-compat
  app.get("/api/nifty/overall", overallHandler);  // kept for backward-compat
  app.get("/api/oi/bulk", bulkHandler);           // bulk + ETag + micro-cache
}