// server/src/routes/instruments.ts
import { Router, Request, Response, RequestHandler } from "express";
import csv from "csv-parser";
// ❌ DO NOT add router-level wildcard cors() here; app-level CORS already handles it
// import cors from "cors";
import fetch from "node-fetch";
import { Readable } from "stream";

const CSV_URL = "https://images.dhan.co/api-data/api-scrip-master.csv";
let instruments: any[] = [];
let loaded = false;

/* -------------------- Helpers -------------------- */

// Normalize: "26-03-2026 15:30" -> "2026-03-26"
function normalizeDhanDate(d: string | undefined): string {
  if (!d) return "";
  const justDate = d.split(" ")[0];
  if (/^\d{2}-\d{2}-\d{4}$/.test(justDate)) {
    const [dd, mm, yyyy] = justDate.split("-");
    return `${yyyy}-${mm}-${dd}`;
  }
  if (/^\d{4}-\d{2}-\d{2}$/.test(justDate)) return justDate;
  const parsed = new Date(justDate);
  return isNaN(parsed.getTime()) ? justDate : parsed.toISOString().slice(0, 10);
}
function parseISODate(d: string): Date | null {
  if (!d) return null;
  const iso = normalizeDhanDate(d);
  const dt = new Date(iso);
  return isNaN(dt.getTime()) ? null : dt;
}
function cleanExpiryDate(dateString: string | undefined): string {
  return normalizeDhanDate(dateString);
}
function deriveUnderlyingSymbol(instr: any): string {
  const t = (instr.SEM_TRADING_SYMBOL || "").toUpperCase();
  if (t.includes("-")) return t.split("-")[0];
  const c = (instr.SEM_CUSTOM_SYMBOL || "").toUpperCase();
  if (c) return c.split(" ")[0];
  const n = (instr.SEM_SYMBOL_NAME || "").toUpperCase();
  if (n.includes(" ")) return n.split(" ")[0];
  return n || t || "";
}
function smartFormatInput(input: string, currentExpiries: string[] = []) {
  const months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
  const monthRegex = new RegExp(`(${months.join("|")})`, "i");

  let parts = input
    .trim()
    .split(
      /(?<=[A-Za-z])(?=\d)|(?<=\d)(?=[A-Za-z])|(?=[A-Za-z])(?=Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)|[\s-]+/i
    )
    .map(p => p.trim())
    .filter(Boolean);

  if (parts.length === 1) {
    const match = parts[0].match(monthRegex);
    if (match && match.index && match.index > 0) {
      parts = [parts[0].substring(0, match.index).toUpperCase(), match[0]];
    }
  } else {
    parts = parts.map((p, i) =>
      i === 0 || !months.some(m => m.toLowerCase() === p.toLowerCase()) ? p.toUpperCase() : p
    );
  }

  let [sym] = parts;
  let symbol = sym || "";
  let strikePrice = parts.find(x => /^\d+$/.test(x)) || "";
  let otype = (parts.find(x => x === "CE" || x === "PE") || "").toUpperCase();
  let rest = parts.filter(x => x !== symbol && x !== strikePrice && x !== otype);

  let expiry = "";
  if (rest.length && currentExpiries.length) {
    const monthPart = rest.find(x => months.some(m => m.toLowerCase() === x.toLowerCase()));
    if (monthPart) {
      expiry =
        currentExpiries.find(date =>
          date.toLowerCase().includes(monthPart.toLowerCase())
        ) || "";
    }
  }
  return { symbol, strikePrice, optionType: otype, expiry };
}

async function loadCSVFromURL() {
  if (loaded && instruments.length) return;
  instruments = [];
  const response = await fetch(CSV_URL);
  const csvText = await response.text();
  await new Promise<void>((resolve, reject) => {
    Readable.from(csvText)
      .pipe(csv())
      .on("data", (row) => instruments.push(row))
      .on("end", resolve)
      .on("error", reject);
  });
  loaded = true;
}
loadCSVFromURL();

/* -------------------- Router -------------------- */

const router = Router();

// IF (and only if) you really want router-level CORS, make it match app CORS:
// const ORIGIN = process.env.CLIENT_URL || "http://localhost:5173";
// router.use(
//   cors({
//     origin: ORIGIN,
//     credentials: true,
//     methods: ["GET", "OPTIONS"],
//     allowedHeaders: ["Content-Type", "Authorization", "X-Debug-User"],
//   })
// );

/** Get unique underlyings (index names) for Exchange + Instrument (e.g., NSE + OPTIDX) */
router.get("/underlyings", (req: Request, res: Response) => {
  const exchange = ((req.query.exchange as string) || "").trim().toUpperCase();
  const instrument =
    ((req.query.instrument as string) ||
      (req.query.instrumentType as string) ||
      "").trim().toUpperCase();

  let matches = instruments.filter((instr) => {
    return (
      (!exchange ||
        (instr.SEM_EXM_EXCH_ID || "").toUpperCase() === exchange) &&
      (!instrument ||
        (instr.SEM_INSTRUMENT_NAME || "").toUpperCase() === instrument)
    );
  });

  const list = Array.from(
    new Set(matches.map((x) => deriveUnderlyingSymbol(x)).filter(Boolean))
  ).sort();

  res.json({ underlyings: list });
});

/** Expiry dates with optional filters */
router.get("/expiries", (req: Request, res: Response) => {
  const symbol = ((req.query.symbol as string) || "").trim().toUpperCase();
  const exchange = ((req.query.exchange as string) || "").trim().toUpperCase();
  const instrumentType =
    ((req.query.instrumentType as string) || "").trim().toUpperCase();
  const optionType =
    ((req.query.optionType as string) || "").trim().toUpperCase();
  const underlying =
    ((req.query.underlying as string) || "").trim().toUpperCase();
  const expiryType = ((req.query.expiryType as string) || "").toLowerCase();

  let matches = instruments.filter((instr) => {
    const sym = (instr.SEM_TRADING_SYMBOL || "").toUpperCase();
    const exch = (instr.SEM_EXM_EXCH_ID || "").toUpperCase();
    const instrName = (instr.SEM_INSTRUMENT_NAME || "").toUpperCase();
    const otype = (instr.SEM_OPTION_TYPE || "").toUpperCase();
    const und = deriveUnderlyingSymbol(instr);

    return (
      (!symbol || sym.startsWith(symbol)) &&
      (!exchange || exch === exchange) &&
      (!instrumentType || instrName === instrumentType) &&
      (!optionType || otype === optionType) &&
      (!underlying || und === underlying)
    );
  });

  let expiryDates = [
    ...new Set(matches.map((x) => cleanExpiryDate(x.SEM_EXPIRY_DATE)).filter(Boolean)),
  ].sort();

  let filterFunc = (date: string) => true;
  if (expiryType === "weekly") {
    filterFunc = (date: string) => {
      const d = parseISODate(date);
      if (!d) return false;
      return d.getDay() === 4 && d.getDate() < 25;
    };
  } else if (expiryType === "monthly") {
    filterFunc = (date: string) => {
      const d = parseISODate(date);
      if (!d) return false;
      const temp = new Date(d);
      temp.setDate(d.getDate() + 7);
      return d.getDay() === 4 && temp.getMonth() !== d.getMonth();
    };
  }
  expiryDates = expiryDates.filter(filterFunc);

  res.json({ expiryDates });
});

/** Symbol smart suggestions API */
const searchHandler: RequestHandler = (req: Request, res: Response) => {
  let query = ((req.query.query as string) || "").trim().toUpperCase();
  const exchange = ((req.query.exchange as string) || "").trim().toUpperCase();
  const instrumentType =
    ((req.query.instrumentType as string) || "").trim().toUpperCase();
  const optionType =
    ((req.query.optionType as string) || "").trim().toUpperCase();
  const strikePrice = ((req.query.strikePrice as string) || "").trim();
  const expiry = ((req.query.expiry as string) || "").trim();
  const underlying =
    ((req.query.underlying as string) || "").trim().toUpperCase();

  if (!query && !strikePrice && !optionType && !underlying) {
    res.json([]);
    return;
  }

  let formatted = smartFormatInput(query);
  let symbolMatch = formatted.symbol || query;
  let strikeMatch = strikePrice || formatted.strikePrice;
  let otypeMatch = optionType || formatted.optionType;

  let matches = instruments.filter((instr) => {
    const trading = (instr.SEM_TRADING_SYMBOL || "").toUpperCase();
    const symbolName = (instr.SEM_SYMBOL_NAME || "").toUpperCase();
    const instrumentName = (instr.SEM_INSTRUMENT_NAME || "").toUpperCase();
    const und = deriveUnderlyingSymbol(instr);

    let match =
      trading.includes(symbolMatch) ||
      symbolName.includes(symbolMatch) ||
      instrumentName.includes(symbolMatch);

    if (strikeMatch)
      match =
        match &&
        instr.SEM_STRIKE_PRICE &&
        instr.SEM_STRIKE_PRICE.toString().includes(strikeMatch);

    if (otypeMatch)
      match =
        match &&
        instr.SEM_OPTION_TYPE &&
        (instr.SEM_OPTION_TYPE as string).toUpperCase() === otypeMatch;

    if (exchange)
      match =
        match &&
        instr.SEM_EXM_EXCH_ID &&
        (instr.SEM_EXM_EXCH_ID as string).toUpperCase() === exchange;

    if (instrumentType)
      match =
        match &&
        instr.SEM_INSTRUMENT_NAME &&
        (instr.SEM_INSTRUMENT_NAME as string).toUpperCase() === instrumentType;

    if (expiry) match = match && cleanExpiryDate(instr.SEM_EXPIRY_DATE) === expiry;

    if (underlying) match = match && und === underlying;

    return match;
  });

  matches = matches.sort((a, b) => {
    const aSym = (a.SEM_TRADING_SYMBOL || a.SEM_SYMBOL_NAME || "").toUpperCase();
    const bSym = (b.SEM_TRADING_SYMBOL || b.SEM_SYMBOL_NAME || "").toUpperCase();
    const aStarts = aSym.startsWith(symbolMatch) ? 0 : 1;
    const bStarts = bSym.startsWith(symbolMatch) ? 0 : 1;
    if (aStarts !== bStarts) return aStarts - bStarts;
    return aSym.localeCompare(bSym);
  });

  res.json(
    matches.slice(0, 300).map((instr) => ({
      symbol: instr.SEM_TRADING_SYMBOL || instr.SEM_SYMBOL_NAME || "",
      exchangeId: instr.SEM_EXM_EXCH_ID,
      instrumentType: instr.SEM_INSTRUMENT_NAME || "",
      optionType: instr.SEM_OPTION_TYPE || "",
      strikePrice: instr.SEM_STRIKE_PRICE || "",
      instrumentName:
        instr.SEM_SYMBOL_NAME ||
        instr.SEM_CUSTOM_SYMBOL ||
        instr.SEM_INSTRUMENT_NAME ||
        "",
      segment: instr.SEM_SEGMENT || "",
      lotSize: Number(instr.SEM_LOT_UNITS) || "",
      expiry: cleanExpiryDate(instr.SEM_EXPIRY_DATE) || "",
      underlyingSymbol: deriveUnderlyingSymbol(instr) || "",
    }))
  );
};

router.get("/search", searchHandler);

export default router;
