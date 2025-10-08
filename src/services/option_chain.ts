import axios from "axios";

/* ========= Env / headers ========= */
const DHAN_ACCESS_TOKEN = process.env.DHAN_API_KEY || "";
const DHAN_CLIENT_ID = process.env.DHAN_CLIENT_ID || "";

function requireEnv() {
  if (!DHAN_ACCESS_TOKEN || !DHAN_CLIENT_ID) {
    throw new Error("Missing DHAN_API_KEY or DHAN_CLIENT_ID in your environment.");
  }
}
function dhanHeaders() {
  return {
    "Content-Type": "application/json",
    "access-token": DHAN_ACCESS_TOKEN,
    "client-id": DHAN_CLIENT_ID,
  };
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/* ========= Types ========= */
export type DhanGreeks = { delta: number; theta: number; gamma: number; vega: number; };
export type DhanOptionLeg = {
  greeks: DhanGreeks;
  implied_volatility: number;
  last_price: number;
  oi: number;
  previous_close_price: number;
  previous_oi: number;
  previous_volume: number;
  top_ask_price: number;
  top_ask_quantity: number;
  top_bid_price: number;
  top_bid_quantity: number;
  volume: number;
};
export type DhanStrikeOC = { ce?: DhanOptionLeg; pe?: DhanOptionLeg };
export type DhanOCMap = Record<string, DhanStrikeOC>;
export type DhanOCResponse = { data: { last_price: number; oc: DhanOCMap } };

/* ========= Helpers ========= */
function istToday(): string {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Kolkata",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(new Date());
  const map = Object.fromEntries(parts.map(p => [p.type, p.value]));
  return `${map.year}-${map.month}-${map.day}`;
}

export function pickNearestExpiry(expiries: string[]): string | null {
  if (!expiries?.length) return null;
  const today = istToday();
  const sorted = expiries.slice().sort(); // YYYY-MM-DD lexicographic
  for (const e of sorted) if (e >= today) return e;
  return sorted[sorted.length - 1] ?? null;
}

/* ========= API calls ========= */
export async function fetchExpiryList(
  UnderlyingScrip: number,
  UnderlyingSeg: string
): Promise<string[]> {
  requireEnv();
  try {
    const res = await axios.post(
      "https://api.dhan.co/v2/optionchain/expirylist",
      { UnderlyingScrip, UnderlyingSeg },
      { headers: dhanHeaders(), timeout: 10000 }
    );
    const dates: string[] = res.data?.data || [];
    console.log(`📅 expiries: ${dates.length}`);
    return dates;
  } catch (e: any) {
    console.error("expirylist error:", e?.message || e);
    return [];
  }
}

/** Returns EXACT Dhan shape */
export async function fetchOptionChainRaw(
  UnderlyingScrip: number,
  UnderlyingSeg: string,
  Expiry: string
): Promise<DhanOCResponse> {
  requireEnv();
  const res = await axios.post(
    "https://api.dhan.co/v2/optionchain",
    { UnderlyingScrip, UnderlyingSeg, Expiry },
    { headers: dhanHeaders(), timeout: 15000 }
  );
  return { data: res.data?.data ?? { last_price: 0, oc: {} } };
}

/* ========= Convenience ========= */
export async function getLiveOptionChain(
  UnderlyingScrip: number,
  UnderlyingSeg: string,
  Expiry?: string
): Promise<{ expiry: string; data: { last_price: number; oc: DhanOCMap } }> {
  let expiry = Expiry;
  if (!expiry) {
    const expiries = await fetchExpiryList(UnderlyingScrip, UnderlyingSeg);
    const picked = pickNearestExpiry(expiries);
    if (!picked) throw new Error("No expiry available.");
    expiry = picked;
    await sleep(3100); // 1 call / 3s
  }
  const { data } = await fetchOptionChainRaw(UnderlyingScrip, UnderlyingSeg, expiry);
  return { expiry, data };
}

/* ========= Normalize ========= */
export function toNormalizedArray(oc: DhanOCMap) {
  return Object.entries(oc)
    .map(([k, v]) => ({ strike: Number(k), ...v }))
    .filter((r) => Number.isFinite(r.strike))
    .sort((a, b) => a.strike - b.strike);
}
