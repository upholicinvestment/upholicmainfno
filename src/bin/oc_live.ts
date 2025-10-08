// src/bin/oc_live.ts
import "dotenv/config";
import { getLiveOptionChain, toNormalizedArray, DhanOptionLeg } from "../services/option_chain";
import { istNowString } from "../utils/time";

function strikeStepFromKeys(keys: string[]): number {
  const n = Math.min(keys.length, 20);
  const nums = keys.slice(0, n).map(k => Number(k)).filter(Number.isFinite);
  nums.sort((a, b) => a - b);
  let step = 50;
  for (let i = 1; i < nums.length; i++) {
    const diff = Math.abs(nums[i] - nums[i - 1]);
    if (diff > 0) { step = diff; break; }
  }
  return step;
}
function roundToStep(price: number, step: number): number {
  return Math.round(price / step) * step;
}
function isActive(leg?: DhanOptionLeg): boolean {
  if (!leg) return false;
  return (
    (leg.last_price ?? 0) > 0 ||
    (leg.top_bid_price ?? 0) > 0 ||
    (leg.top_ask_price ?? 0) > 0 ||
    (leg.oi ?? 0) > 0 ||
    (leg.volume ?? 0) > 0
  );
}
function sum<T>(arr: T[], f: (x: T) => number) { return arr.reduce((a, x) => a + (f(x) || 0), 0); }

(async () => {
  const id  = Number(process.env.OC_UNDERLYING_ID || 13); // NIFTY
  const seg = process.env.OC_SEGMENT || "IDX_I";
  const exp = process.env.OC_EXPIRY;                      // optional YYYY-MM-DD
  const windowSteps = Number(process.env.OC_WINDOW_STEPS || 15); // ±N steps around ATM
  const pcrSteps = Number(process.env.OC_PCR_STEPS || 3);        // PCR window around ATM

  const { expiry, data } = await getLiveOptionChain(id, seg, exp);

  // infer step & ATM
  const keys = Object.keys(data.oc);
  const step = strikeStepFromKeys(keys);
  const atm  = roundToStep(data.last_price, step);

  // normalize & window
  const norm = toNormalizedArray(data.oc);
  const windowed = norm.filter(r => Math.abs(r.strike - atm) <= windowSteps * step);
  const active = windowed.filter(r => isActive(r.ce) || isActive(r.pe));

  // PCR
  const ceOIAll = sum(windowed, r => r.ce?.oi ?? 0);
  const peOIAll = sum(windowed, r => r.pe?.oi ?? 0);
  const pcrAll  = ceOIAll > 0 ? (peOIAll / ceOIAll) : 0;

  const near = norm.filter(r => Math.abs(r.strike - atm) <= pcrSteps * step);
  const ceOINear = sum(near, r => r.ce?.oi ?? 0);
  const peOINear = sum(near, r => r.pe?.oi ?? 0);
  const pcrNear  = ceOINear > 0 ? (peOINear / ceOINear) : 0;

  console.log(`[${istNowString()}]`);
  console.log(`Expiry: ${expiry}`);
  console.log(`LTP(underlying): ${data.last_price}`);
  console.log(`Strike step: ${step} | ATM: ${atm}`);
  console.log(`Window: ±${windowSteps * step} → strikes ${atm - windowSteps*step}..${atm + windowSteps*step}`);
  console.log(`Active rows in window: ${active.length}/${windowed.length}`);
  console.log(`PCR(window ±${windowSteps*step}): ${pcrAll.toFixed(2)} | PCR(near ATM ±${pcrSteps*step}): ${pcrNear.toFixed(2)}\n`);

  // Top OI ladders
  const topCE = windowed
    .filter(r => (r.ce?.oi ?? 0) > 0)
    .sort((a, b) => (b.ce!.oi!) - (a.ce!.oi!))
    .slice(0, 10)
    .map(r => ({ strike: r.strike, oi: r.ce!.oi, ltp: r.ce!.last_price ?? 0 }));
  const topPE = windowed
    .filter(r => (r.pe?.oi ?? 0) > 0)
    .sort((a, b) => (b.pe!.oi!) - (a.pe!.oi!))
    .slice(0, 10)
    .map(r => ({ strike: r.strike, oi: r.pe!.oi, ltp: r.pe!.last_price ?? 0 }));

  console.log("Top 10 CE OI (window):");
  console.table(topCE);
  console.log("Top 10 PE OI (window):");
  console.table(topPE);

  // Compact table around ATM (active only)
  const table = active.map((r) => ({
    strike: r.strike,
    ce_ltp: r.ce?.last_price ?? 0,
    ce_iv:  r.ce?.implied_volatility ?? 0,
    ce_oi:  r.ce?.oi ?? 0,
    ce_bid: r.ce?.top_bid_price ?? 0,
    ce_ask: r.ce?.top_ask_price ?? 0,
    pe_ltp: r.pe?.last_price ?? 0,
    pe_iv:  r.pe?.implied_volatility ?? 0,
    pe_oi:  r.pe?.oi ?? 0,
    pe_bid: r.pe?.top_bid_price ?? 0,
    pe_ask: r.pe?.top_ask_price ?? 0,
  }));
  console.log("\nATM window (active) sample:");
  console.table(table.slice(0, 30));
})();
