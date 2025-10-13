// src/services/quote.service.ts
import axios from "axios";
import { Db } from "mongodb";
import { Readable } from "stream";
import csvParser from "csv-parser";
import { scheduleQuote } from "../utils/dhanPacer"; // quote bucket

let db: Db | null = null;

export const setQuoteDatabase = (database: Db) => {
  db = database;
  // helpful indexes
  db.collection("market_quotes").createIndex({ timestamp: -1 }).catch(() => {});
  db.collection("market_quotes").createIndex({ security_id: 1, timestamp: -1 }).catch(() => {});
};

// Convert UTC to IST (store raw Date)
function getISTDate(): Date {
  return new Date();
}

// Check if market is open (9:15 AM - 3:30 PM IST)
function isMarketOpen(): boolean {
  const now = getISTDate();
  const minutes = now.getHours() * 60 + now.getMinutes();
  const day = now.getDay();
  if (day === 0 || day === 6) return false;
  return minutes >= 9 * 60 + 15 && minutes <= 15 * 60 + 30;
}

// Backoff-aware retry that ALWAYS goes through the quote bucket
async function quoteRetry<T>(fn: () => Promise<T>, tries = 3): Promise<T> {
  let lastErr: any;
  for (let i = 0; i < tries; i++) {
    try {
      return await scheduleQuote(fn);
    } catch (err: any) {
      lastErr = err;
      const status = err?.response?.status;
      if (status === 429 || (status >= 500 && status < 600)) {
        const wait = Math.min(8000, 1000 * (i + 1)); // 1s,2s,3s...
        console.log(`⏳ Rate limit (${status}): retrying in ${wait}ms...`);
        await new Promise((r) => setTimeout(r, wait));
        continue;
      }
      throw err;
    }
  }
  throw lastErr;
}

/* ========================= CSV → instruments ========================= */
/** Tolerant CSV row parser for BOTH:
 * - https://images.dhan.co/api-data/api-scrip-master-detailed.csv
 * - https://images.dhan.co/api-data/api-scrip-master.csv
 */
function parseInstrumentRow(row: any) {
  const security_id = parseInt(
    row["SECURITY_ID"] ?? row["SEM_SMST_SECURITY_ID"] ?? row["SMST_SECURITY_ID"] ?? "0",
    10
  );

  const trading_symbol =
    row["SYMBOL_NAME"] ??
    row["SEM_TRADING_SYMBOL"] ??
    row["TRADING_SYMBOL"] ??
    row["SEM_CUSTOM_SYMBOL"] ??
    "";

  const instrument_type =
    row["INSTRUMENT"] ??
    row["SEM_INSTRUMENT_NAME"] ??
    row["SEM_EXCH_INSTRUMENT_TYPE"] ??
    "";

  // prefer detailed, otherwise master’s SEM_EXPIRY_DATE
  const expiry_raw =
    row["SM_EXPIRY_DATE"] ??
    row["SEM_EXPIRY_DATE"] ??
    row["EXPIRY_DATE"] ??
    null;

  const strike_price = parseFloat(
    row["STRIKE_PRICE"] ?? row["SEM_STRIKE_PRICE"] ?? row["STRIKE"] ?? "0"
  );

  const option_type = row["OPTION_TYPE"] ?? row["SEM_OPTION_TYPE"] ?? row["OPT_TYPE"] ?? "";

  const expiry_flag = row["EXPIRY_FLAG"] ?? row["SEM_EXPIRY_FLAG"] ?? "";

  // normalize expiry to ISO yyyy-mm-dd (store both iso + raw)
  const expiry_date = expiry_raw ? normalizeExpiryInput(String(expiry_raw)) : null;

  return {
    security_id,
    trading_symbol,
    instrument_type,
    expiry_date, // normalized iso (if we could parse)
    expiry_raw: expiry_raw ?? null, // keep original for flexible matching
    strike_price,
    option_type,
    expiry_flag,
  };
}

/**
 * Fetch & store instrument metadata from Dhan Scrip Master (tolerates either CSV).
 * By default still uses *detailed*; set DHAN_SCRIP_CSV_URL to master if you want that instead.
 */
export const fetchAndStoreInstruments = async () => {
  try {
    const url =
      process.env.DHAN_SCRIP_CSV_URL?.trim() ||
      "https://images.dhan.co/api-data/api-scrip-master-detailed.csv";

    console.log("📡 Fetching instrument master from:", url);
    const response = await axios.get(url, { responseType: "stream", timeout: 30000 });

    const dataStream = response.data as Readable;
    const instruments: any[] = [];

    await new Promise<void>((resolve, reject) => {
      dataStream
        .pipe(csvParser())
        .on("data", (row: any) => {
          const parsed = parseInstrumentRow(row);
          if (parsed.security_id > 0) instruments.push(parsed);
        })
        .on("end", resolve)
        .on("error", reject);
    });

    if (!db) throw new Error("Database not initialized");
    await db.collection("instruments").deleteMany({});
    if (instruments.length > 0) {
      await db.collection("instruments").insertMany(instruments, { ordered: false });
      await db.collection("instruments").createIndex({ security_id: 1 }, { unique: true }).catch(() => {});
      await db.collection("instruments").createIndex({ instrument_type: 1, expiry_date: 1 }).catch(() => {});
      await db.collection("instruments").createIndex({ instrument_type: 1, expiry_raw: 1 }).catch(() => {});
    }

    console.log(`💾 Saved ${instruments.length} instruments to DB.`);
  } catch (err) {
    console.error("❌ Error fetching/storing instruments:", err);
  }
};

/* ========================= Marketfeed helpers ========================= */

/** Parse '28-10-2025 14:30:00' | '28-10-2025' | '2025-10-28' → '2025-10-28' */
function normalizeExpiryInput(expiry: string): string {
  if (!expiry) return "";
  const part = expiry.trim().split(/\s+/)[0]; // drop time if present
  if (/^\d{4}-\d{2}-\d{2}$/.test(part)) return part; // already ISO
  const m = part.match(/^(\d{2})-(\d{2})-(\d{4})$/);
  if (m) return `${m[3]}-${m[2]}-${m[1]}`;
  const d = new Date(expiry);
  if (!isNaN(d.getTime())) {
    const yyyy = d.getFullYear();
    const mm = String(d.getMonth() + 1).padStart(2, "0");
    const dd = String(d.getDate()).padStart(2, "0");
    return `${yyyy}-${mm}-${dd}`;
  }
  return expiry;
}

/** Try OHLC on both possible F&O segments just in case */
async function fetchOhlcAny(ids: number[]): Promise<Record<string, any>> {
  if (!ids?.length) return {};
  const segments = ["NSE_FNO", "NSE_FUT"]; // try FNO first, then FUT (some tenants use FUT for FUTSTK)
  for (const seg of segments) {
    try {
      const payload: any = { [seg]: ids };
      const response: any = await quoteRetry(
        () =>
          axios.post("https://api.dhan.co/v2/marketfeed/ohlc", payload, {
            headers: {
              "Content-Type": "application/json",
              "access-token": process.env.DHAN_API_KEY || "",
              "client-id": process.env.DHAN_CLIENT_ID || "",
            },
            timeout: 15000,
          }),
        3
      );
      const data = response?.data?.data?.[seg] || {};
      if (data && Object.keys(data).length > 0) return data;
      console.warn(`ℹ️ OHLC empty for ${seg}, trying next…`);
    } catch (err: any) {
      console.warn(`❗ OHLC error on ${seg}:`, err?.response?.status || err?.message || err);
    }
  }
  return {};
}

/** Try QUOTE on both segments (used as fallback to lift 'ohlc' and 'last_price') */
async function fetchQuoteAny(ids: number[]): Promise<Record<string, any>> {
  if (!ids?.length) return {};
  const segments = ["NSE_FNO", "NSE_FUT"];
  for (const seg of segments) {
    try {
      const payload: any = { [seg]: ids };
      const response: any = await quoteRetry(
        () =>
          axios.post("https://api.dhan.co/v2/marketfeed/quote", payload, {
            headers: {
              "Content-Type": "application/json",
              "access-token": process.env.DHAN_API_KEY || "",
              "client-id": process.env.DHAN_CLIENT_ID || "",
            },
            timeout: 15000,
          }),
        3
      );
      const data = response?.data?.data?.[seg] || {};
      if (data && Object.keys(data).length > 0) return data;
      console.warn(`ℹ️ QUOTE empty for ${seg}, trying next…`);
    } catch (err: any) {
      console.warn(`❗ QUOTE error on ${seg}:`, err?.response?.status || err?.message || err);
    }
  }
  return {};
}

/** Your original public helper (kept) */
export const fetchOhlc = async (ids: number[]): Promise<Record<string, any>> => {
  return fetchOhlcAny(ids);
};

/** Your original quote helper (kept) */
export const fetchMarketQuote = async (ids: number[]): Promise<Record<string, any>> => {
  return fetchQuoteAny(ids);
};

/* ========================= Persist market depth ========================= */

export const saveMarketQuote = async (data: Record<string, any>) => {
  try {
    if (!db) throw new Error("Database not initialized");

    const timestamp = new Date();
    const securityIds = Object.keys(data).map((id) => parseInt(id, 10));

    // Fetch instrument metadata for the batch
    const instruments = await db
      .collection("instruments")
      .find({ security_id: { $in: securityIds } })
      .toArray();

    const instrumentMap = Object.fromEntries(instruments.map((inst) => [inst.security_id, inst]));

    // Merge quote data with instrument metadata
    const documents = Object.entries(data).map(([security_id, details]: [string, any]) => {
      const sid = parseInt(security_id, 10);
      const instrument = instrumentMap[sid] || {};

      return {
        security_id: sid,
        trading_symbol: instrument.trading_symbol || "",
        instrument_type: instrument.instrument_type || "",
        expiry_date: instrument.expiry_date || null,
        strike_price: instrument.strike_price || 0,
        option_type: instrument.option_type || "",
        expiry_flag: instrument.expiry_flag || "",
        average_price: details?.average_price ?? 0,
        buy_quantity: details?.buy_quantity ?? 0,
        depth: details?.depth ?? { buy: [], sell: [] },
        exchange: "NSE_FNO",
        last_price: details?.last_price ?? 0,
        last_quantity: details?.last_quantity ?? 0,
        last_trade_time: details?.last_trade_time ?? "",
        lower_circuit_limit: details?.lower_circuit_limit ?? 0,
        net_change: details?.net_change ?? 0,
        ohlc: details?.ohlc ?? { open: 0, close: 0, high: 0, low: 0 },
        oi: details?.oi ?? 0,
        oi_day_high: details?.oi_day_high ?? 0,
        oi_day_low: details?.oi_day_low ?? 0,
        sell_quantity: details?.sell_quantity ?? 0,
        upper_circuit_limit: details?.upper_circuit_limit ?? 0,
        volume: details?.volume ?? 0,
        timestamp,
      };
    });

    if (documents.length > 0) {
      const result = await db.collection("market_quotes").insertMany(documents);
      const count =
        (result as any).insertedCount ?? Object.keys((result as any).insertedIds || {}).length;
      console.log(`💾 Saved ${count} Market Quote docs at ${timestamp.toLocaleString("en-IN")}.`);

      // Mirror last_price into ltp_history as a safety net
      const ltpDocs = documents.map((d) => ({
        security_id: d.security_id,
        LTP: d.last_price ?? 0,
        trading_symbol: d.trading_symbol,
        instrument_type: d.instrument_type,
        expiry_date: d.expiry_date,
        strike_price: d.strike_price ?? 0,
        option_type: d.option_type,
        expiry_flag: d.expiry_flag,
        timestamp: new Date(),
      }));
      if (ltpDocs.length) {
        await db.collection("ltp_history").insertMany(ltpDocs, { ordered: false });
      }
    }
  } catch (err) {
    console.error("❌ Error saving Market Quote:", err);
  }
};

/* ====================== FUTSTK OHLC by Expiry (NSE) ====================== */

/**
 * Flexible resolver:
 * - matches normalized ISO and raw 'dd-mm-yyyy [time]' from either CSV,
 * - if nothing matches, snaps to nearest FUTSTK expiry present in DB.
 */
async function getFutstkIdsForExpiry(
  expiryInput: string
): Promise<{ ids: number[]; meta: Record<number, any>; resolvedExpiry: string }> {
  if (!db) throw new Error("Database not initialized");

  const iso = normalizeExpiryInput(expiryInput);
  const [y, m, d] = iso.split("-");
  const dmy = `${d}-${m}-${y}`; // dd-mm-yyyy

  let docs = await db
    .collection("instruments")
    .find(
      {
        instrument_type: "FUTSTK",
        $or: [
          { expiry_date: iso },
          { expiry_raw: { $regex: `^${dmy}(?:\\b|\\s)` } },  // "28-10-2025" or "28-10-2025 14:30"
          { expiry_raw: { $regex: `^${iso}(?:\\b|\\s)` } },  // "2025-10-28 ..."
        ],
      },
      { projection: { security_id: 1, trading_symbol: 1, expiry_date: 1, expiry_raw: 1 } }
    )
    .toArray();

  let resolvedIso = iso;

  if (docs.length === 0) {
    // snap to nearest FUTSTK expiry in DB
    const all = await db.collection("instruments").find(
      { instrument_type: "FUTSTK" },
      { projection: { expiry_date: 1, expiry_raw: 1 } }
    ).toArray();

    const candidates: string[] = [];
    for (const r of all) {
      const cand =
        (r.expiry_date && typeof r.expiry_date === "string" && r.expiry_date) ||
        (r.expiry_raw && normalizeExpiryInput(String(r.expiry_raw))) ||
        null;
      if (cand) candidates.push(cand);
    }

    const target = new Date(iso + "T00:00:00Z").getTime();
    let best: { iso: string; diff: number } | null = null;
    for (const c of candidates) {
      const t = new Date(c + "T00:00:00Z").getTime();
      const diff = Math.abs(t - target);
      if (!best || diff < best.diff) best = { iso: c, diff };
    }
    if (best) {
      resolvedIso = best.iso;
      const [yy, mm, dd2] = resolvedIso.split("-");
      const altDmy = `${dd2}-${mm}-${yy}`;
      docs = await db
        .collection("instruments")
        .find(
          {
            instrument_type: "FUTSTK",
            $or: [
              { expiry_date: resolvedIso },
              { expiry_raw: { $regex: `^${altDmy}(?:\\b|\\s)` } },
              { expiry_raw: { $regex: `^${resolvedIso}(?:\\b|\\s)` } },
            ],
          },
          { projection: { security_id: 1, trading_symbol: 1, expiry_date: 1, expiry_raw: 1 } }
        )
        .toArray();
      console.log(`ℹ️ Resolved FUTSTK expiry '${expiryInput}' → '${resolvedIso}'`);
    }
  }

  const ids = docs.map((d) => Number(d.security_id)).filter(Number.isFinite);
  const meta: Record<number, any> = {};
  for (const d of docs) meta[Number(d.security_id)] = d;

  return { ids, meta, resolvedExpiry: resolvedIso };
}

/**
 * Fetch OHLC for all FUTSTK contracts on an expiry and persist into `nse_futstk_ohlc`.
 * Tries OHLC (NSE_FNO→NSE_FUT) then falls back to QUOTE (NSE_FNO→NSE_FUT).
 */
export const fetchAndSaveFutstkOhlc = async (
  expiryInput: string
): Promise<{ matched: number; fetched: number; inserted: number; resolvedExpiry?: string }> => {
  if (!db) throw new Error("Database not initialized");

  const { ids, meta, resolvedExpiry } = await getFutstkIdsForExpiry(expiryInput);
  if (!ids.length) {
    console.warn("⚠️ No FUTSTK instruments found for expiry:", expiryInput);
    return { matched: 0, fetched: 0, inserted: 0, resolvedExpiry };
  }

  // index for typical read pattern (by security + recency)
  try {
    await db.collection("nse_futstk_ohlc").createIndex({ security_id: 1, received_at: -1 });
  } catch {}

  const chunkSize = 1000; // Dhan limit per request
  let fetched = 0;
  let inserted = 0;

  for (let i = 0; i < ids.length; i += chunkSize) {
    const chunk = ids.slice(i, i + chunkSize);

    // 1) Try OHLC (both segments)
    let data = await fetchOhlcAny(chunk);

    // 2) Fallback to Quote (both segments) if OHLC is empty
    if (!data || Object.keys(data).length === 0) {
      console.warn("ℹ️ OHLC returned empty; falling back to Quote for this batch.");
      const q = await fetchQuoteAny(chunk);
      data = {};
      for (const [sid, det] of Object.entries<any>(q)) {
        (data as any)[sid] = {
          last_price: det?.last_price ?? 0,
          ohlc: det?.ohlc ?? { open: 0, high: 0, low: 0, close: Number(det?.last_price ?? 0) },
        };
      }
    }

    const now = new Date();

    const docs = Object.entries<any>(data).map(([sid, details]) => {
      fetched++;
      const m = meta[Number(sid)] || {};
      const o = details?.ohlc || {};
      return {
        security_id: Number(sid),
        trading_symbol: m.trading_symbol || "",
        expiry_date: m.expiry_date || resolvedExpiry || normalizeExpiryInput(expiryInput),
        instrument_type: "FUTSTK",
        exchange: "NSE_FNO",
        LTP: Number(details?.last_price ?? 0),
        open: Number(o?.open ?? 0),
        high: Number(o?.high ?? 0),
        low: Number(o?.low ?? 0),
        close: Number(o?.close ?? Number(details?.last_price ?? 0)),
        received_at: now,
      };
    });

    if (docs.length) {
      const res = await db!.collection("nse_futstk_ohlc").insertMany(docs, { ordered: false });
      inserted += (res as any).insertedCount ?? Object.keys((res as any).insertedIds || {}).length;
    }
  }

  console.log(
    `💾 FUTSTK OHLC saved | expiry=${resolvedExpiry || normalizeExpiryInput(expiryInput)} matched=${ids.length} fetched=${fetched} inserted=${inserted}`
  );
  return { matched: ids.length, fetched, inserted, resolvedExpiry };
};
