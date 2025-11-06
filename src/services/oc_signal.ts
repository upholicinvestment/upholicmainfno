//src/services/oc_signal.ts
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
  expiry?: string; // "YYYY-MM-DD"
  last_price?: number;
  spot?: number;
  strikes?: Strike[];
  updated_at?: Date | string;
  ts?: Date;
};

export type DecisionRow = {
  volatility: number;
  time: string;
  signal: "Bullish" | "Bearish";
  spot: number;
};

export type DecisionRowOut = DecisionRow & {
  moveClass?: "Big Upside" | "Big Downside" | "Small Upside" | "Small Downside";
  pointsHint?: "300-400" | "100-200";
};

/* ========= Constants ========= */
const IST_OFFSET_MIN = 330; // +05:30
const IST_OFFSET_MS = IST_OFFSET_MIN * 60_000;
const SESSION_START_MIN = 9 * 60 + 15; // 09:15 IST
const SESSION_END_MIN = 15 * 60 + 30;  // 15:30 IST
const DAY_MS = 86_400_000;

/* ========= Time Helpers ========= */
function timeIST(d: Date): string {
  return new Intl.DateTimeFormat("en-IN", {
    timeZone: "Asia/Kolkata",
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(d);
}

function istDayStartMs(utcMs: number) {
  const istMs = utcMs + IST_OFFSET_MS;
  return Math.floor(istMs / DAY_MS) * DAY_MS;
}

/** Build fixed 09:15–15:30 IST grid in UTC for given interval */
function buildSessionBucketsUTC(intervalMin: number): { start: Date; end: Date }[] {
  const intervalMs = intervalMin * 60_000;
  const now = new Date();
  const utcMs = now.getTime();
  const istDayStart = istDayStartMs(utcMs);

  const startIst = istDayStart + SESSION_START_MIN * 60_000;
  const endIst   = istDayStart + SESSION_END_MIN   * 60_000;

  const buckets: { start: Date; end: Date }[] = [];
  for (let t = startIst; t < endIst; t += intervalMs) {
    const s = t;
    const e = Math.min(t + intervalMs, endIst); // clamp to 15:30
    buckets.push({ start: new Date(s - IST_OFFSET_MS), end: new Date(e - IST_OFFSET_MS) });
  }
  return buckets;
}

/* ========= Signals ========= */
function gaussianWeight(k: number, spot: number, width = 300): number {
  const d = k - spot;
  return Math.exp(-(d * d) / (2 * width * width));
}
function detectStrikeStep(strikes: Strike[]): number {
  const vals = Array.from(new Set(strikes.map((s) => Number(s.strike))))
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
function signalFromPrice(prevSpot: number, spot: number): "Bullish" | "Bearish" {
  return spot >= prevSpot ? "Bullish" : "Bearish";
}
function signalFromWeightedDelta(strikes: Strike[] = [], spot: number): "Bullish" | "Bearish" {
  let net = 0;
  for (const s of strikes) {
    const w = gaussianWeight(Number(s.strike), spot, 300);
    const ceDelta = s.ce?.greeks?.delta ?? 0;
    const peDelta = s.pe?.greeks?.delta ?? 0;
    const ceOI    = s.ce?.oi ?? 0;
    const peOI    = s.pe?.oi ?? 0;
    net += w * (ceDelta * ceOI + peDelta * peOI);
  }
  return net >= 0 ? "Bullish" : "Bearish";
}

/* ========= Volatility ========= */
function volatilityValue(prevSpot: number | undefined, spot: number, unit: "bps" | "pct" | "points" = "bps"): number {
  if (!prevSpot || prevSpot <= 0) return 0;
  const diff = spot - prevSpot;
  if (unit === "points") return Number(diff.toFixed(2));
  const pct = (diff / prevSpot) * 100;
  if (unit === "pct") return Number(pct.toFixed(2));
  const bps = pct * 100;
  return Number(bps.toFixed(2));
}

/* ========= Liquidity ========= */
function liquidityFromOIDelta(curr: Strike[] = [], prev: Strike[] = [], spot: number, width = 300) {
  const prevMap = new Map<number, Strike>();
  for (const p of prev) prevMap.set(Number(p.strike), p);

  let ceSum = 0, peSum = 0;
  for (const s of curr) {
    const k = Number(s.strike);
    const w = gaussianWeight(k, spot, width);
    const p = prevMap.get(k);

    const ceNow  = s.ce?.oi ?? 0;
    const peNow  = s.pe?.oi ?? 0;
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
  const atm  = roundToStep(spot, step);
  const inside = curr.filter((s) => Math.abs(Number(s.strike) - atm) <= windowSteps * step);

  let ceSum = 0, peSum = 0;
  for (const s of inside) {
    ceSum += s.ce?.oi ?? 0;
    peSum += s.pe?.oi ?? 0;
  }
  const total = ceSum + peSum;
  if (total <= 0) return { liqCall: 0.5, liqPut: 0.5 };
  return { liqCall: ceSum / total, liqPut: peSum / total };
}

/* ========= Classifier ========= */
type MoveClass = DecisionRowOut["moveClass"];
type PointsHint = DecisionRowOut["pointsHint"];

function classifyMove(
  signal: "Bullish" | "Bearish",
  liqCall: number,
  liqPut: number,
  volatilityBps: number,
  bigThresholdBps = 10
): { moveClass?: MoveClass; pointsHint?: PointsHint } {
  const liqSide = liqCall >= liqPut ? "Call" : "Put";
  const big = Math.abs(volatilityBps) >= bigThresholdBps;
  const up  = volatilityBps > 0;

  if (big && up   && signal === "Bullish" && liqSide === "Call") return { moveClass: "Big Upside",   pointsHint: "300-400" };
  if (big && !up  && signal === "Bearish" && liqSide === "Put")  return { moveClass: "Big Downside", pointsHint: "300-400" };
  if (!big && up  && signal === "Bullish" && liqSide === "Put")  return { moveClass: "Small Upside", pointsHint: "100-200" };
  if (!big && !up && signal === "Bearish" && liqSide === "Call") return { moveClass: "Small Downside", pointsHint: "100-200" };
  return {};
}

/* ========= Mongo ========= */
const SORT_SPEC: Record<string, 1 | -1> = { ts: -1, updated_at: -1, _id: -1 };

/* ========= Main Function ========= */
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
    intervalMin?: number;
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
    const coll = client.db(dbName).collection<OptionChainDoc>(process.env.OC_SOURCE_COLL || "option_chain_ticks");

    // Canonical fixed session (09:15 → 15:30 IST)
    const buckets = buildSessionBucketsUTC(intervalMin);
    const sessionStart = buckets[0].start;
    const sessionEnd   = buckets[buckets.length - 1].end;

    const ticks = await coll
      .find({ underlying_security_id, expiry, ts: { $gte: sessionStart, $lt: sessionEnd } })
      .sort({ ts: 1 })
      .toArray();

    if (!ticks.length) return [];

    const out: DecisionRowOut[] = [];
    let prevSpot: number | undefined;

    for (const { start, end } of buckets) {
      const slice = ticks.filter((t) => {
        const ts = t.ts instanceof Date ? t.ts : new Date(t.ts!);
        return ts >= start && ts < end;
      });

      // Skip empty early candles until first tick exists
      if (!slice.length && !prevSpot) continue;

      const endMs = end.getTime();
      const first = slice[0] || ticks.find((t) => new Date(t.ts!).getTime() < endMs)!;
      const last  = slice[slice.length - 1] || first;

      const spot = Number(last.spot ?? last.last_price ?? 0);
      if (!prevSpot) prevSpot = Number(first.spot ?? first.last_price ?? spot);

      const volDisplay = volatilityValue(prevSpot, spot, unit);
      const volBps     = volatilityValue(prevSpot, spot, "bps");

      let sig: "Bullish" | "Bearish" = signalFromPrice(prevSpot, spot);
      if (signalMode === "delta") {
        sig = signalFromWeightedDelta(last.strikes ?? [], spot);
      } else if (signalMode === "hybrid") {
        const dSig = signalFromWeightedDelta(last.strikes ?? [], spot);
        if (dSig === sig) sig = dSig;
      }

      const { liqCall, liqPut } =
        mode === "delta"
          ? liquidityFromOIDelta(last.strikes ?? [], first.strikes ?? [], spot, width)
          : liquidityFromOILevel(last.strikes ?? [], spot, windowSteps);

      const labelTime = timeIST(end); // bucket END
      const row: DecisionRowOut = { volatility: volDisplay, time: labelTime, signal: sig, spot };

      if (classify) {
        const c = classifyMove(sig, liqCall, liqPut, volBps);
        row.moveClass = c.moveClass;
        row.pointsHint = c.pointsHint;
      }

      out.push(row);
      prevSpot = spot;
    }

    // Return last N rows in DESC order
    return out.slice(-limit).reverse();
  } finally {
    await client.close();
  }
}

/* ========= Single-row ========= */
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
