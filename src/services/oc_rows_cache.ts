// src/services/oc_rows_cache.ts
import { Db, MongoClient } from "mongodb";

/* ========= Types ========= */
type Signal = "Bullish" | "Bearish";
type Mode = "level" | "delta";
type Unit = "bps" | "pct" | "points";

type TickDoc = {
  underlying_security_id: number;
  underlying_segment?: string;
  underlying_symbol?: string;
  expiry: string;
  last_price?: number;
  spot?: number;
  strikes?: Array<{
    strike: number;
    ce?: { greeks?: { delta?: number }; oi?: number };
    pe?: { greeks?: { delta?: number }; oi?: number };
  }>;
  ts: Date | string;
};

type CacheDoc = {
  underlying_security_id: number;
  underlying_segment: string;
  expiry: string;
  intervalMin: number;
  mode: Mode;
  unit: Unit;
  tsBucket: Date;         // canonical bucket key
  time: string;           // "HH:MM:SS IST"
  volatility: number;
  signal: Signal;
  spot: number;
  created_at: Date;
  updated_at: Date;
};

const TICKS_COLL = process.env.OC_SOURCE_COLL || "option_chain_ticks";
const CACHE_COLL = "oc_rows_cache";
const VERBOSE = (process.env.OC_ROWS_LOG_VERBOSE || "true").toLowerCase() !== "false";

/* ========= Helpers ========= */
function toDateSafe(d: Date | string): Date {
  return d instanceof Date ? d : new Date(d);
}
function timeIST(d: Date): string {
  return (
    new Intl.DateTimeFormat("en-IN", {
      timeZone: "Asia/Kolkata",
      hour12: false,
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    }).format(d) + " IST"
  );
}
function istMidnight(date: Date): Date {
  const istString = date.toLocaleString("en-US", { timeZone: "Asia/Kolkata" });
  const istDate = new Date(istString);
  istDate.setHours(0, 0, 0, 0);
  return istDate;
}
function floorToBucket(d: Date, minutes: number): number {
  const ms = minutes * 60 * 1000;
  return Math.floor(d.getTime() / ms) * ms;
}
function gaussianWeight(k: number, spot: number, width = 300): number {
  const d = k - spot;
  return Math.exp(-(d * d) / (2 * width * width));
}
function signalFromPrice(prevSpot: number, spot: number): Signal {
  return spot >= prevSpot ? "Bullish" : "Bearish";
}
function signalFromWeightedDelta(strikes: TickDoc["strikes"] = [], spot: number): Signal {
  let net = 0;
  for (const s of strikes) {
    const w = gaussianWeight(Number(s.strike), spot, 300);
    const ceDelta = s.ce?.greeks?.delta ?? 0;
    const peDelta = s.pe?.greeks?.delta ?? 0;
    const ceOI = s.ce?.oi ?? 0;
    const peOI = s.pe?.oi ?? 0;
    net += w * (ceDelta * ceOI + peDelta * peOI);
  }
  return net >= 0 ? "Bullish" : "Bearish";
}
function volatilityValue(prevSpot: number | undefined, spot: number, unit: Unit = "bps"): number {
  if (!prevSpot || prevSpot <= 0) return 0;
  const diff = spot - prevSpot;
  if (unit === "points") return Number(diff.toFixed(2));
  const pct = (diff / prevSpot) * 100;
  if (unit === "pct") return Number(pct.toFixed(2));
  const bps = pct * 100;
  return Number(bps.toFixed(2));
}

/* ========= Resolve active expiry ========= */
async function resolveActiveExpiry(
  client: MongoClient,
  dbName: string,
  underlying: number,
  segment = "IDX_I"
): Promise<string | null> {
  const db = client.db(dbName);

  const snap = await db
    .collection("option_chain")
    .find({ underlying_security_id: underlying, underlying_segment: segment } as any)
    .project({ expiry: 1, updated_at: 1 })
    .sort({ updated_at: -1 })
    .limit(1)
    .toArray();
  if (snap.length && (snap[0] as any)?.expiry) return String((snap[0] as any).expiry);

  const tick = await db
    .collection(TICKS_COLL)
    .find({ underlying_security_id: underlying, underlying_segment: segment } as any)
    .project({ expiry: 1, ts: 1 })
    .sort({ ts: -1 })
    .limit(1)
    .toArray();
  if (tick.length && (tick[0] as any)?.expiry) return String((tick[0] as any).expiry);

  return null;
}

/* ========= Compute rows from ticks ========= */
async function computeRowsFromTicksWindow(params: {
  db: Db;
  underlying: number;
  segment: string;
  expiry: string;
  intervalMin: number;
  since: Date;
  mode: Mode;
  unitStore?: Unit;
}): Promise<{
  rows: Array<{ tsBucket: Date; time: string; volatility: number; signal: Signal; spot: number }>;
  tickCount: number;
}> {
  const { db, underlying, segment, expiry, intervalMin, since, mode, unitStore = "bps" } = params;
  const coll = db.collection<TickDoc>(TICKS_COLL);

  const ticksAscRaw = await coll
    .find({
      underlying_security_id: underlying,
      underlying_segment: segment,
      expiry,
      ts: { $gte: since },
    } as any)
    .sort({ ts: 1 })
    .toArray();

  const ticksAsc = ticksAscRaw.map((t) => ({ ...t, ts: toDateSafe(t.ts) as Date }));
  const tickCount = ticksAsc.length;
  if (tickCount < 2) return { rows: [], tickCount };

  // LAST tick per bucket
  const byBucket = new Map<number, TickDoc & { ts: Date }>();
  for (const t of ticksAsc) {
    const key = floorToBucket(t.ts as Date, intervalMin);
    const prev = byBucket.get(key);
    if (!prev || (prev.ts as Date) < (t.ts as Date)) byBucket.set(key, t as any);
  }

  const keysAsc = [...byBucket.keys()].sort((a, b) => a - b);
  if (keysAsc.length < 2) return { rows: [], tickCount };

  const rows: Array<{ tsBucket: Date; time: string; volatility: number; signal: Signal; spot: number }> = [];

  for (let i = 1; i < keysAsc.length; i++) {
    const curr = byBucket.get(keysAsc[i])!;
    const prev = byBucket.get(keysAsc[i - 1])!;

    const spot = Number((curr.spot ?? curr.last_price ?? 0) as number);
    const prevSpot = Number((prev.spot ?? prev.last_price ?? 0) as number);

    const volBps = volatilityValue(prevSpot, spot, "bps");
    const volatility =
      unitStore === "bps"
        ? volBps
        : unitStore === "pct"
        ? volatilityValue(prevSpot, spot, "pct")
        : volatilityValue(prevSpot, spot, "points");

    let sig: Signal = signalFromPrice(prevSpot, spot);
    if (mode === "delta") sig = signalFromWeightedDelta(curr.strikes, spot);

    rows.push({
      tsBucket: new Date(keysAsc[i]),
      time: timeIST(curr.ts as Date),
      volatility,
      signal: sig,
      spot,
    });
  }

  return { rows, tickCount };
}

/* ========= Index management ========= */
async function dropLegacyOcRowsIndexes(db: Db) {
  const coll = db.collection(CACHE_COLL);
  try {
    const idxes = await coll.listIndexes().toArray();
    for (const idx of idxes) {
      const name = idx.name || "";
      const key = idx.key || {};
      const hasBucketKey = Object.prototype.hasOwnProperty.call(key, "bucket_key");
      const legacyName =
        name === "rows_usid_expiry_int_bucket_unique" ||
        name === "rows_usid_expiry_int_bucket" ||
        name.includes("bucket_key") ||
        name.includes("bucket");
      if (hasBucketKey || legacyName) {
        try {
          await coll.dropIndex(name);
          if (VERBOSE) 
            console.log(`[oc_rows_cache] Dropped legacy index: ${name}`);
        } catch (e: any) {
          console.warn(`[oc_rows_cache] Failed dropping index ${name}:`, e?.message || e);
        }
      }
    }
  } catch (e: any) {
    console.warn("[oc_rows_cache] listIndexes failed (maybe no collection yet):", e?.message || e);
  }
}

export async function ensureOcRowsIndexes(db: Db) {
  const coll = db.collection<CacheDoc>(CACHE_COLL);

  // 🔧 Clean up legacy unique index that used `bucket_key`
  await dropLegacyOcRowsIndexes(db);

  try {
    await coll.createIndex(
      {
        underlying_security_id: 1,
        underlying_segment: 1,
        expiry: 1,
        intervalMin: 1,
        mode: 1,
        unit: 1,
        tsBucket: -1,
      },
      {
        name: "oc_rows_cache_core",
        unique: true,
        partialFilterExpression: { tsBucket: { $type: "date" } },
      }
    );
  } catch (e: any) {
    // ignore "existing index" shape mismatch noise
    if (String(e?.message || "").toLowerCase().includes("existing index")) {
      if (VERBOSE) console.warn("oc_rows_cache_core already exists (ok)");
    } else {
      throw e;
    }
  }

  try {
    await coll.createIndex(
      {
        underlying_security_id: 1,
        expiry: 1,
        intervalMin: 1,
        tsBucket: -1,
      },
      { name: "oc_rows_cache_query" }
    );
  } catch (e: any) {
    if (String(e?.message || "").toLowerCase().includes("existing index")) {
      if (VERBOSE) console.warn("oc_rows_cache_query already exists (ok)");
    } else {
      throw e;
    }
  }
}

/* ========= Materializer ========= */
export async function materializeOcRowsOnce(args: {
  mongoUri: string;
  dbName: string;
  underlying: number;
  segment?: string;
  intervals: number[];
  sinceMs?: number;
  mode?: Mode;
  unit?: Unit;
}): Promise<Record<number, number>> {
  const {
    mongoUri,
    dbName,
    underlying,
    segment = "IDX_I",
    intervals,
    sinceMs,
    mode = "level",
    unit = "bps",
  } = args;

  const client = new MongoClient(mongoUri);
  await client.connect();
  try {
    const db = client.db(dbName);

    await ensureOcRowsIndexes(db);

    const expiry = await resolveActiveExpiry(client, dbName, underlying, segment);
    if (!expiry) {
      if (VERBOSE) console.warn(`[oc_rows_cache] No active expiry for ${underlying}/${segment}`);
      return Object.fromEntries(intervals.map((m) => [m, 0]));
    }

    const now = new Date();
    const baseSince = sinceMs ? new Date(now.getTime() - sinceMs) : istMidnight(now);

    const fallbackWindowsMs = Array.from(
      new Set<number>([
        sinceMs ?? 12 * 60 * 60 * 1000,
        24 * 60 * 60 * 1000,
        48 * 60 * 60 * 1000,
        72 * 60 * 60 * 1000,
      ])
    ).sort((a, b) => a - b);

    const results: Record<number, number> = {};

    for (const intervalMin of intervals) {
      let usedSince = baseSince;
      let rowsOut: Array<{ tsBucket: Date; time: string; volatility: number; signal: Signal; spot: number }> = [];
      let tickCount = 0;

      // Try base window
      {
        const { rows, tickCount: tc } = await computeRowsFromTicksWindow({
          db,
          underlying,
          segment,
          expiry,
          intervalMin,
          since: usedSince,
          mode,
          unitStore: unit,
        });
        rowsOut = rows;
        tickCount = tc;
        if (VERBOSE) {
          // console.log(
          //   `[oc_rows_cache] try base window: expiry=${expiry} interval=${intervalMin}m since=${usedSince.toISOString()} ticks=${tickCount} rows=${rowsOut.length}`
          // );
        }
      }

      // Fallbacks
      if (rowsOut.length === 0) {
        for (const ms of fallbackWindowsMs) {
          const trySince = new Date(now.getTime() - ms);
          const { rows, tickCount: tc } = await computeRowsFromTicksWindow({
            db,
            underlying,
            segment,
            expiry,
            intervalMin,
            since: trySince,
            mode,
            unitStore: unit,
          });
          if (VERBOSE) {
            // console.log(
            //   `[oc_rows_cache] fallback window: expiry=${expiry} interval=${intervalMin}m since=${trySince.toISOString()} ticks=${tc} rows=${rows.length}`
            // );
          }
          if (rows.length > 0) {
            rowsOut = rows;
            usedSince = trySince;
            tickCount = tc;
            break;
          }
        }
      }

      if (!rowsOut.length) {
        if (VERBOSE) {
          console.warn(
            `[oc_rows_cache] no rows after fallbacks: u=${underlying}/${segment} exp=${expiry} interval=${intervalMin}m`
          );
        }
        results[intervalMin] = 0;
        continue;
      }

      const bulk = db.collection<CacheDoc>(CACHE_COLL).initializeUnorderedBulkOp();
      for (const r of rowsOut) {
        bulk
          .find({
            underlying_security_id: underlying,
            underlying_segment: segment,
            expiry,
            intervalMin,
            mode,
            unit,
            tsBucket: r.tsBucket,
          } as any)
          .upsert()
          .update({
            $set: {
              time: r.time,
              volatility: r.volatility,
              signal: r.signal,
              spot: r.spot,
              updated_at: new Date(),
            },
            $setOnInsert: {
              created_at: new Date(),
            },
          });
      }

      const res = await bulk.execute();
      const upserts = (res.modifiedCount ?? 0) + (res.upsertedCount ?? 0);

      if (VERBOSE) {
        // console.log(
          // `[oc_rows_cache] upserted=${upserts} (interval=${intervalMin}m, ticks=${tickCount}, since=${usedSince.toISOString()})`
        // );
      }

      results[intervalMin] = upserts;
    }

    return results;
  } finally {
    try { await client.close(); } catch {}
  }
}

/* ========= Background scheduler ========= */
export function startOcRowsMaterializer(opts: {
  mongoUri: string;
  dbName: string;
  underlyings: Array<{ id: number; segment?: string }>;
  intervals: number[];
  sinceMs?: number;
  scheduleMs?: number;
  mode?: Mode;
  unit?: Unit;
}): NodeJS.Timeout {
  const {
    mongoUri,
    dbName,
    underlyings,
    intervals,
    sinceMs = 12 * 60 * 60 * 1000,
    scheduleMs = 60_000,
    mode = "level",
    unit = "bps",
  } = opts;

  (async () => {
    for (const u of underlyings) {
      try {
        const res = await materializeOcRowsOnce({
          mongoUri,
          dbName,
          underlying: u.id,
          segment: u.segment || "IDX_I",
          intervals,
          sinceMs,
          mode,
          unit,
        });
        // console.log("⛏️ oc_rows_cache initial fill:", { underlying: u.id, res });
      } catch (e: any) {
        console.warn("oc_rows_cache initial fill error:", e?.message || e);
      }
    }
  })();

  const timer = setInterval(async () => {
    for (const u of underlyings) {
      try {
        const res = await materializeOcRowsOnce({
          mongoUri,
          dbName,
          underlying: u.id,
          segment: u.segment || "IDX_I",
          intervals,
          sinceMs,
          mode,
          unit,
        });
        // console.log("⛏️ oc_rows_cache sweep:", { underlying: u.id, res });
      } catch (e: any) {
        console.warn("oc_rows_cache sweep error:", e?.message || e);
      }
    }
  }, Math.max(15_000, scheduleMs));

  return timer;
}
