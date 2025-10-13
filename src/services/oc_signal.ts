// src/services/oc_signal.ts
import { MongoClient, ObjectId } from "mongodb";

/* ========= Types ========= */
type Greeks = { delta?: number; theta?: number; gamma?: number; vega?: number };
type Leg = { greeks?: Greeks; last_price?: number; oi?: number; previous_oi?: number; volume?: number };
type Strike = { strike: number; ce?: Leg; pe?: Leg };

export type OptionChainDoc = {
  _id: ObjectId | string;
  underlying_symbol?: string;
  underlying_segment?: string;
  underlying_security_id?: number;
  expiry?: string;                 // "YYYY-MM-DD"
  last_price?: number;             // spot fallback
  spot?: number;                   // preferred
  strikes?: Strike[];
  updated_at?: Date | string;      // in snapshot coll
  ts?: Date;                       // in ticks coll
};

export type DecisionRow = {
  volatility: number;           // in chosen unit
  time: string;                 // HH:MM:SS IST
  signal: "Bullish" | "Bearish";
  spot: number;
};

export type DecisionRowOut = DecisionRow & {
  // no liquidity fields exposed
  moveClass?: "Big Upside" | "Big Downside" | "Small Upside" | "Small Downside";
  pointsHint?: "300-400" | "100-200";
};

/* ========= Helpers ========= */
function timeIST(d: Date): string {
  return new Intl.DateTimeFormat("en-IN", {
    timeZone: "Asia/Kolkata",
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(d);
}
function gaussianWeight(k: number, spot: number, width = 300): number {
  const d = k - spot;
  return Math.exp(-(d * d) / (2 * width * width));
}
function detectStrikeStep(strikes: Strike[]): number {
  const vals = Array.from(new Set(strikes.map(s => Number(s.strike))))
    .filter(Number.isFinite)
    .sort((a, b) => a - b);
  let step = 50;
  for (let i = 1; i < Math.min(vals.length, 50); i++) {
    const d = Math.abs(vals[i] - vals[i - 1]);
    if (d > 0) { step = d; break; }
  }
  return step;
}
function roundToStep(price: number, step: number): number {
  return Math.round(price / step) * step;
}

/* ========= Signals ========= */
function signalFromPrice(prevSpot: number, spot: number): "Bullish" | "Bearish" {
  return spot >= prevSpot ? "Bullish" : "Bearish";
}
function signalFromWeightedDelta(strikes: Strike[] = [], spot: number): "Bullish" | "Bearish" {
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

/** Convert spot move to desired unit:
 *  - "pct"    => percent (e.g., -0.08)
 *  - "bps"    => basis points (percent * 100) (e.g., -8.00)
 *  - "points" => raw points (e.g., -20.5)
 */
function volatilityValue(prevSpot: number | undefined, spot: number, unit: "bps"|"pct"|"points" = "bps"): number {
  if (!prevSpot || prevSpot <= 0) return 0;
  const diff = spot - prevSpot;

  if (unit === "points") return Number(diff.toFixed(2));

  const pct = (diff / prevSpot) * 100;
  if (unit === "pct") return Number(pct.toFixed(2));

  const bps = pct * 100;
  return Number(bps.toFixed(2));
}

/* ========= Liquidity (internal only) ========= */
function liquidityFromOIDelta(curr: Strike[] = [], prev: Strike[] = [], spot: number, width = 300) {
  const prevMap = new Map<number, Strike>();
  for (const p of prev) prevMap.set(Number(p.strike), p);

  let ceSum = 0, peSum = 0;
  for (const s of curr) {
    const k = Number(s.strike);
    const w = gaussianWeight(k, spot, width);
    const p = prevMap.get(k);

    const ceNow = s.ce?.oi ?? 0;
    const peNow = s.pe?.oi ?? 0;
    const cePrev = p?.ce?.oi ?? 0;
    const pePrev = p?.pe?.oi ?? 0;

    const dce = Math.max(0, ceNow - cePrev);
    const dpe = Math.max(0, peNow - pePrev);

    ceSum += w * dce;
    peSum += w * dpe;
  }
  const total = ceSum + peSum;
  if (total <= 0) return { liqCall: 0.5, liqPut: 0.5 };
  return { liqCall: ceSum / total, liqPut: peSum / total };
}
function liquidityFromOILevel(curr: Strike[] = [], spot: number, windowSteps = 5) {
  if (!curr.length) return { liqCall: 0.5, liqPut: 0.5 };
  const step = detectStrikeStep(curr);
  const atm = roundToStep(spot, step);
  const inside = curr.filter(s => Math.abs(Number(s.strike) - atm) <= windowSteps * step);

  let ceSum = 0, peSum = 0;
  for (const s of inside) {
    ceSum += s.ce?.oi ?? 0;
    peSum += s.pe?.oi ?? 0;
  }
  const total = ceSum + peSum;
  if (total <= 0) return { liqCall: 0.5, liqPut: 0.5 };
  return { liqCall: ceSum / total, liqPut: peSum / total };
}

/* ========= Classifier (internal) ========= */
function classifyMove(
  signal: "Bullish" | "Bearish",
  liqCall: number,
  liqPut: number,
  volatilityBps: number,
  bigThresholdBps = 10
) {
  const liqSide = liqCall >= liqPut ? "Call" : "Put";
  const big = Math.abs(volatilityBps) >= bigThresholdBps;
  const up = volatilityBps > 0;

  if (big && up && signal === "Bullish" && liqSide === "Call") {
    return { moveClass: "Big Upside" as const, pointsHint: "300-400" as const };
  }
  if (big && !up && signal === "Bearish" && liqSide === "Put") {
    return { moveClass: "Big Downside" as const, pointsHint: "300-400" as const };
  }
  if (!big && up && signal === "Bullish" && liqSide === "Put") {
    return { moveClass: "Small Upside" as const, pointsHint: "100-200" as const };
  }
  if (!big && !up && signal === "Bearish" && liqSide === "Call") {
    return { moveClass: "Small Downside" as const, pointsHint: "100-200" as const };
  }
  return {};
}

/* ========= Mongo ========= */
const SORT_SPEC: Record<string, 1 | -1> = { ts: -1, updated_at: -1, _id: -1 };

async function getRecentTicks(
  client: MongoClient,
  dbName: string,
  filter: { underlying_security_id: number; expiry: string },
  limit: number
): Promise<OptionChainDoc[]> {
  const collName = process.env.OC_SOURCE_COLL || "option_chain_ticks";
  const coll = client.db(dbName).collection<OptionChainDoc & { ts?: Date }>(collName);
  const docs = await coll.find(filter as any).sort(SORT_SPEC).limit(limit).toArray();
  return docs as OptionChainDoc[];
}

/* ========= Bucketing ========= */
function floorToBucket(d: Date, minutes: number): number {
  const ms = minutes * 60 * 1000;
  return Math.floor(d.getTime() / ms) * ms;
}

/* ========= Public APIs ========= */
export async function computeRowsFromDBWindow(
  mongoUri: string,
  dbName: string,
  underlying_security_id: number,
  expiry: string,
  limit = 12,
  options?: {
    mode?: "level" | "delta";
    unit?: "bps" | "pct" | "points";
    signalMode?: "price" | "delta" | "hybrid";
    windowSteps?: number;
    width?: number;
    classify?: boolean;
    intervalMin?: number; // default 3
  }
): Promise<DecisionRowOut[]> {
  const {
    mode = "level",
    unit = "bps",
    signalMode = "price",
    windowSteps = 5,
    width = 300,
    classify = true,
    intervalMin = 3,
  } = options || {};

  const client = new MongoClient(mongoUri);
  await client.connect();
  try {
    const ticksDesc = await getRecentTicks(client, dbName, { underlying_security_id, expiry }, Math.max(2, limit + 1000));
    if (ticksDesc.length < 2) return [];

    // Oldest -> newest for bucketing
    const ticksAsc = [...ticksDesc].reverse();

    // 1) Bucket by intervalMin, keep LAST tick in each bucket
    const byBucket = new Map<number, OptionChainDoc>();
    for (const t of ticksAsc) {
      const ts = (t.ts instanceof Date) ? t.ts : new Date();
      const key = floorToBucket(ts, intervalMin);
      const prev = byBucket.get(key);
      if (!prev || (prev.ts instanceof Date && ts > prev.ts)) {
        byBucket.set(key, t);
      }
    }

    // 2) Ordered series (ASC)
    const bucketKeys = [...byBucket.keys()].sort((a, b) => a - b);
    const seriesAsc = bucketKeys.map(k => byBucket.get(k)!).filter(Boolean);
    if (seriesAsc.length < 2) return [];

    // 3) Compute rows from consecutive buckets
    const outAsc: DecisionRowOut[] = [];
    for (let i = 1; i < seriesAsc.length; i++) {
      const curr = seriesAsc[i];
      const prev = seriesAsc[i - 1];

      const spot = Number(curr.spot ?? curr.last_price ?? 0);
      const prevSpot = Number(prev?.spot ?? prev?.last_price ?? 0);
      const time = timeIST(curr.ts instanceof Date ? curr.ts : new Date());

      const volDisplay = volatilityValue(prevSpot, spot, unit);
      const volBps = volatilityValue(prevSpot, spot, "bps");

      let sig: "Bullish" | "Bearish" = signalFromPrice(prevSpot, spot);
      if (signalMode === "delta") sig = signalFromWeightedDelta(curr.strikes ?? [], spot);
      else if (signalMode === "hybrid") {
        const dSig = signalFromWeightedDelta(curr.strikes ?? [], spot);
        sig = dSig === sig ? sig : sig; // keep price if disagreement
      }

      // internal liquidity for classifier only
      let liqCall = 0.5, liqPut = 0.5;
      if (mode === "delta") {
        const res = liquidityFromOIDelta(curr.strikes ?? [], prev.strikes ?? [], spot, width);
        liqCall = res.liqCall; liqPut = res.liqPut;
      } else {
        const res = liquidityFromOILevel(curr.strikes ?? [], spot, windowSteps);
        liqCall = res.liqCall; liqPut = res.liqPut;
      }

      const row: DecisionRowOut = { volatility: volDisplay, time, signal: sig, spot };

      if (classify) {
        const c = classifyMove(sig, liqCall, liqPut, volBps);
        row.moveClass = c.moveClass;
        row.pointsHint = c.pointsHint;
      }

      outAsc.push(row);
    }

    // 4) return last N in DESC
    return outAsc.slice(Math.max(0, outAsc.length - limit)).reverse();
  } finally {
    await client.close();
  }
}

export async function computeRowFromDB(
  mongoUri: string,
  dbName: string,
  underlying_security_id: number,
  expiry: string,
  unit: "bps" | "pct" | "points" = "bps",
  signalMode: "price" | "delta" | "hybrid" = "price"
): Promise<DecisionRow | undefined> {
  const rows = await computeRowsFromDBWindow(mongoUri, dbName, underlying_security_id, expiry, 2, {
    unit,
    signalMode,
    classify: false,
    intervalMin: 3,
  });
  return rows.slice(-1)[0];
}
