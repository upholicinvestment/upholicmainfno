// src/bin/oc_watch.ts
import "dotenv/config";
import { MongoClient } from "mongodb";
import {
  fetchExpiryList,
  pickNearestExpiry,
  fetchOptionChainRaw,
  getLiveOptionChain,
  toNormalizedArray,
  DhanOptionLeg,
} from "../services/option_chain";
import { istNowString, istTimestamp } from "../utils/time";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

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
  // ---- Mongo init ----
  const uri = process.env.MONGO_URI || "mongodb://127.0.0.1:27017";
  const dbName = process.env.DB_NAME || "Upholic";
  const client = new MongoClient(uri, { maxPoolSize: 5 });
  await client.connect();
  const db = client.db(dbName);
  console.log(`✅ Mongo connected → ${uri}/${dbName}`);

  // Helpful indexes (safe if already exist)
  try {
    await db.collection("option_chain").createIndex(
      { underlying_security_id: 1, underlying_segment: 1, expiry: 1 },
      { unique: true }
    );
    await db.collection("option_chain").createIndex({ "strikes.strike": 1 });
    await db.collection("option_chain_ticks").createIndex(
      { underlying_security_id: 1, underlying_segment: 1, expiry: 1, ts: 1 }
    );
  } catch {}

  // ---- Watch params ----
  const id  = Number(process.env.OC_UNDERLYING_ID || 13); // NIFTY
  const seg = process.env.OC_SEGMENT || "IDX_I";
  const sym = process.env.OC_SYMBOL || "NIFTY";
  const intervalMs = Number(process.env.OC_LIVE_MS || 3100); // >= 3000 (API limit)
  const windowSteps = Number(process.env.OC_WINDOW_STEPS || 15);
  const pcrSteps = Number(process.env.OC_PCR_STEPS || 3);

  let expiry = (process.env.OC_EXPIRY || "").trim();

  // Resolve expiry once at start
  if (!expiry) {
    try {
      const exps = await fetchExpiryList(id, seg);
      const picked = pickNearestExpiry(exps);
      if (picked) {
        expiry = picked;
        await sleep(3100); // polite pause before first /optionchain
      } else {
        const res = await getLiveOptionChain(id, seg);
        expiry = res.expiry ?? expiry; // keep as-is if null
      }
    } catch {
      const res = await getLiveOptionChain(id, seg);
      expiry = res.expiry ?? expiry;   // keep as-is if null
    }
  }

  // Strong guard so we never call the API with an empty expiry
  if (!expiry) {
    throw new Error(
      "Unable to resolve an expiry. Set OC_EXPIRY in .env or ensure Dhan returns a valid expiry."
    );
  }

  console.log(`▶️  Live Option Chain for ${sym} ${id}/${seg} @ expiry ${expiry}`);
  console.log(`⏱️  Interval: ${intervalMs} ms (rate limit ≥ 3000 ms)`);

  let running = true;

  async function tick() {
    if (!running) return;
    try {
      const ts = new Date();              // true UTC instant
      const ts_ist = istTimestamp(ts);    // friendly IST string

      const { data } = await fetchOptionChainRaw(id, seg, expiry);
      const norm = toNormalizedArray(data.oc);

      // ---- SAVE: upsert latest snapshot into option_chain ----
      await db.collection("option_chain").updateOne(
        { underlying_security_id: id, underlying_segment: seg, expiry },
        {
          $set: {
            underlying_security_id: id,
            underlying_segment: seg,
            underlying_symbol: sym,
            expiry,
            last_price: data.last_price,
            strikes: norm,
            updated_at: ts,         // UTC Date (keep)
            updated_at_ist: ts_ist, // display helper
          },
        },
        { upsert: true }
      );

      // ---- SAVE: append time-series tick into option_chain_ticks ----
      await db.collection("option_chain_ticks").insertOne({
        underlying_security_id: id,
        underlying_segment: seg,
        underlying_symbol: sym,
        expiry,
        last_price: data.last_price,
        strikes: norm,
        ts,       // UTC Date
        ts_ist,   // IST string helper
      });

      // ---- Pretty console view ----
      const keys = Object.keys(data.oc);
      const step = strikeStepFromKeys(keys);
      const atm  = roundToStep(data.last_price, step);

      const windowed = norm.filter(r => Math.abs(r.strike - atm) <= windowSteps * step);
      const active = windowed.filter(r => isActive(r.ce) || isActive(r.pe));

      const ceOIAll = sum(windowed, r => r.ce?.oi ?? 0);
      const peOIAll = sum(windowed, r => r.pe?.oi ?? 0);
      const pcrAll  = ceOIAll > 0 ? peOIAll / ceOIAll : 0;

      const near = norm.filter(r => Math.abs(r.strike - atm) <= pcrSteps * step);
      const ceOINear = sum(near, r => r.ce?.oi ?? 0);
      const peOINear = sum(near, r => r.pe?.oi ?? 0);
      const pcrNear  = ceOINear > 0 ? peOINear / ceOINear : 0;

      const topCE = windowed
        .filter(r => (r.ce?.oi ?? 0) > 0)
        .sort((a, b) => (b.ce!.oi!) - (a.ce!.oi!))
        .slice(0, 8)
        .map(r => ({ strike: r.strike, oi: r.ce!.oi, ltp: r.ce!.last_price ?? 0 }));

      const topPE = windowed
        .filter(r => (r.pe?.oi ?? 0) > 0)
        .sort((a, b) => (b.pe!.oi!) - (a.pe!.oi!))
        .slice(0, 8)
        .map(r => ({ strike: r.strike, oi: r.pe!.oi, ltp: r.pe!.last_price ?? 0 }));

      console.clear();
      console.log(`[${istNowString()}] ${sym} • expiry ${expiry}`);
      console.log(`LTP: ${data.last_price} | step: ${step} | ATM: ${atm}`);
      console.log(`Window: ±${windowSteps * step} → strikes ${atm - windowSteps*step}..${atm + windowSteps*step}`);
      console.log(`Active rows in window: ${active.length}/${windowed.length}`);
      console.log(`PCR(window): ${pcrAll.toFixed(2)} | PCR(±${pcrSteps*step}): ${pcrNear.toFixed(2)}\n`);

      console.log("Top CE OI:");
      console.table(topCE);
      console.log("Top PE OI:");
      console.table(topPE);

      const table = active.slice(0, 25).map(r => ({
        strike: r.strike,
        ce_ltp: r.ce?.last_price ?? 0,
        ce_iv:  r.ce?.implied_volatility ?? 0,
        ce_oi:  r.ce?.oi ?? 0,
        pe_ltp: r.pe?.last_price ?? 0,
        pe_iv:  r.pe?.implied_volatility ?? 0,
        pe_oi:  r.pe?.oi ?? 0,
      }));
      console.log("ATM window (sample):");
      console.table(table);

    } catch (e: any) {
      console.error("Tick error:", e?.message || e);
    }
  }

  await tick(); // first pull
  const timer = setInterval(tick, intervalMs);

  const shutdown = async () => {
    running = false;
    clearInterval(timer);
    await client.close();
    console.log("\n👋 Stopped live feed & closed Mongo.");
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
})();
