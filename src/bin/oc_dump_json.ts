// src/bin/oc_dump_json.ts
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fetchExpiryList, pickNearestExpiry, fetchOptionChainRaw } from "../services/option_chain";
import { istNowString } from "../utils/time";

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

async function getExpiryWithRetry(id: number, seg: string, tries = 3) {
  let lastErr: any;
  for (let i = 0; i < tries; i++) {
    try {
      const exps = await fetchExpiryList(id, seg);
      const picked = pickNearestExpiry(exps);
      if (!picked) throw new Error("No expiry in list.");
      return picked;
    } catch (e) {
      lastErr = e;
      console.warn(`expirylist attempt ${i + 1}/${tries} failed:`, (e as any)?.message || e);
      await sleep(1500 * (i + 1));
    }
  }
  throw lastErr || new Error("expirylist failed");
}

(async () => {
  const id  = Number(process.env.OC_UNDERLYING_ID || 13);   // NIFTY
  const seg = process.env.OC_SEGMENT || "IDX_I";
  const sym = process.env.OC_SYMBOL || "UNDERLYING";

  let expiry = process.env.OC_EXPIRY;
  if (!expiry) {
    expiry = await getExpiryWithRetry(id, seg, 3);
    await sleep(3100); // rate-limit
  }

  const { data } = await fetchOptionChainRaw(id, seg, expiry);

  // Add helpful metadata while preserving exact Dhan shape at 'data'
  const now = new Date();
  const out = {
    underlying_security_id: id,
    underlying_segment: seg,
    symbol: sym,
    expiry,
    saved_at: now.toISOString(),      // UTC
    saved_at_ist: istNowString(),     // IST display
    data,                              // unchanged Dhan shape
  };

  const file = path.resolve(`./oc_${sym}_${expiry}.json`);
  fs.writeFileSync(file, JSON.stringify(out, null, 2), "utf8");
  console.log(`[${out.saved_at_ist}] Saved: ${file}`);
})();
