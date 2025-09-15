import express, { Request, Response, NextFunction } from "express";
import { Db } from "mongodb";

/* ================== TYPES ================== */
export type PlannedTrade = {
  strategy: string;
  symbol: string;
  quantity: number;
  entry: number;
  tradeType: "BUY" | "SELL";
  stopLoss: number;
  target: number;
  reason: string;
  exchangeId?: string;
  instrumentType?: string;
  instrumentName?: string;
  segment?: string;
  lotSize?: string | number;
  expiry?: string;
  optionType?: string;
  strikePrice?: string;
  underlyingSymbol?: string;
};

export interface DailyPlan {
  userId?: string;
  date: string;            // YYYY-MM-DD
  planNotes: string;
  plannedTrades: PlannedTrade[];
  createdAt: Date;
  updatedAt: Date;
  confidenceLevel?: number;
  stressLevel?: number;
  distractions?: string;
  sleepHours?: number;
  mood?: string;
  focus?: number;
  energy?: number;
}

export type ExecutedTrade = {
  userId?: string;
  date: string;            // normalized on read from "date" or "tradingDate"
  symbol: string;
  quantity: number;
  entry: number;
  exit?: number;
  tradeType: "BUY" | "SELL";
  PnL?: number;
  exchangeId?: string;
  instrumentType?: string;
  instrumentName?: string;
  segment?: string;
  lotSize?: string | number;
  expiry?: string;
  optionType?: string;
  strikePrice?: string;
  underlyingSymbol?: string;
};

/* ================== UTILS ================== */

// Normalize to YYYY-MM-DD
function normalizeDate(dateStr: string | undefined | null): string {
  if (!dateStr) return "";
  const s = String(dateStr).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;

  let m = s.match(/^(\d{1,2})-(\d{1,2})-(\d{4})$/);
  if (m) {
    const [, d, mo, y] = m;
    return `${y}-${mo.padStart(2, "0")}-${d.padStart(2, "0")}`;
  }
  m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (m) {
    const [, d, mo, y] = m;
    return `${y}-${mo.padStart(2, "0")}-${d.padStart(2, "0")}`;
  }
  return s.slice(0, 10);
}

function monthFromCode(code: string) {
  const map: Record<string, string> = {
    F: "JAN", G: "FEB", H: "MAR", J: "APR", K: "MAY", M: "JUN",
    N: "JUL", Q: "AUG", U: "SEP", V: "OCT", X: "NOV", Z: "DEC", O: "OCT"
  };
  return map[code.toUpperCase()] || code.toUpperCase();
}

function parseFnOSymbol(sym: string) {
  let z = sym.match(/^([A-Z]+)-([A-Za-z]{3,})\s?(\d{4})-(\d+)-([A-Z]{2,})$/);
  if (z) {
    return {
      symbol: z[1].replace(/[\W_]/g, ""),
      expiryMonth: z[2].slice(0, 3).toUpperCase(),
      expiryYear: z[3],
      strike: parseFloat(z[4]),
      optionType: z[5].slice(0, 2).toUpperCase(),
      base: z[1].replace(/[\W_]/g, ""),
      raw: sym
    };
  }
  let a = sym.match(/^(?:OPTIDX|OPTSTK|FUTIDX|FUTSTK|BSXOPT|BSXFUT)? ?([A-Z]+)\s+([A-Za-z]{3,})\s+(\d{1,2})?\s?(\d{4})\s+([\d.]+)\s+([A-Z]{2,})(?:\s?\(.*\))?$/);
  if (a) {
    return {
      symbol: a[1].replace(/[\W_]/g, ""),
      expiryMonth: a[2].slice(0, 3).toUpperCase(),
      expiryDay: a[3] ? parseInt(a[3]) : undefined,
      expiryYear: a[4],
      strike: parseFloat(a[5]),
      optionType: a[6].slice(0, 2).toUpperCase(),
      base: a[1].replace(/[\W_]/g, ""),
      raw: sym
    };
  }
  let n = sym.match(/^([A-Z]+)(\d{2})([A-Z])(\d{1,2})(\d+)([A-Z]{2})$/);
  if (n) {
    return {
      symbol: n[1],
      expiryYear: "20" + n[2],
      expiryMonth: monthFromCode(n[3]),
      expiryDay: parseInt(n[4]),
      strike: parseFloat(n[5]),
      optionType: n[6],
      base: n[1],
      raw: sym
    };
  }
  return { symbol: sym.replace(/[\W_]/g, ""), raw: sym };
}

function expiryFuzzyMatch(p1: any, p2: any) {
  if (!p1 || !p2) return false;
  return (
    p1.symbol === p2.symbol &&
    (p1.strike === p2.strike || (p1.strike && p2.strike && Math.abs(Number(p1.strike) - Number(p2.strike)) < 0.01)) &&
    p1.optionType === p2.optionType &&
    p1.expiryMonth === p2.expiryMonth &&
    String(p1.expiryYear) === String(p2.expiryYear)
  );
}

function tradeObjMatch(planned: PlannedTrade, executed: ExecutedTrade) {
  const p1 = parseFnOSymbol(planned.symbol);
  const p2 = parseFnOSymbol(executed.symbol);
  return (
    expiryFuzzyMatch(p1, p2) &&
    planned.tradeType === executed.tradeType &&
    (planned.quantity === executed.quantity || !planned.quantity || !executed.quantity) &&
    Math.abs(Number(planned.entry) - Number(executed.entry)) < 1.0
  );
}

function asyncHandler(fn: (req: Request, res: Response, next: NextFunction) => Promise<any>) {
  return (req: Request, res: Response, next: NextFunction) =>
    Promise.resolve(fn(req, res, next)).catch(next);
}

function groupBy<T>(arr: T[], key: (t: T) => string) {
  const out: { [k: string]: T[] } = {};
  arr.forEach(t => {
    const k = key(t);
    if (!out[k]) out[k] = [];
    out[k].push(t);
  });
  return out;
}

/* ================== ROUTES ================== */

export default function registerDailyJournalRoutes(db: Db) {
  const router = express.Router();

  // GET plan for a date (per-user)
  router.get(
    "/plan",
    asyncHandler(async (req, res) => {
      const userId = (req as any).user?.id as string | undefined;
      if (!userId) return res.status(401).json({ error: "Unauthorized" });

      const { date } = req.query;
      if (!date || typeof date !== "string") {
        return res.status(422).json({ error: "date (YYYY-MM-DD) query param is required" });
      }
      const day = normalizeDate(date);

      const plan = await db
        .collection<DailyPlan>("daily_journal")
        .findOne({ userId, date: day });

      res.json(plan ?? {});
    })
  );

  // POST plan for a date (per-user upsert)
  router.post(
    "/plan",
    asyncHandler(async (req, res) => {
      const userId = (req as any).user?.id as string | undefined;
      if (!userId) return res.status(401).json({ error: "Unauthorized" });

      const {
        date,
        planNotes = "",
        plannedTrades,
        confidenceLevel,
        stressLevel,
        distractions,
        sleepHours,
        mood,
        focus,
        energy
      } = req.body || {};

      const day = normalizeDate(date);
      if (!day) {
        return res.status(422).json({ error: "date (YYYY-MM-DD) is required" });
      }
      if (!Array.isArray(plannedTrades)) {
        return res.status(422).json({ error: "plannedTrades must be an array" });
      }

      plannedTrades.forEach((t: any) => {
        if (typeof t.lotSize === "undefined") t.lotSize = "";
      });

      const now = new Date();
      try {
        await db.collection("daily_journal").updateOne(
          { userId, date: day },
          {
            $set: {
              userId,
              date: day,
              planNotes,
              plannedTrades,
              confidenceLevel,
              stressLevel,
              distractions,
              sleepHours,
              mood,
              focus,
              energy,
              updatedAt: now
            },
            $setOnInsert: { createdAt: now }
          },
          { upsert: true }
        );
        return res.json({ ok: true });
      } catch (e: any) {
        if (e?.code === 11000) {
          return res
            .status(409)
            .json({ error: "A plan for this user and date already exists (unique index)." });
        }
        throw e;
      }
    })
  );

  // GET executed trades for a date (per-user)
  router.get(
    "/executed",
    asyncHandler(async (req, res) => {
      const userId = (req as any).user?.id as string | undefined;
      if (!userId) return res.status(401).json({ error: "Unauthorized" });

      const { date } = req.query;
      if (!date || typeof date !== "string") {
        return res.status(422).json({ error: "date (YYYY-MM-DD) query param is required" });
      }
      const day = normalizeDate(date);

      const trades = await db
        .collection<ExecutedTrade>("executed_trades")
        .find({ userId, $or: [{ date: day }, { tradingDate: day }] })
        .toArray();

      res.json({ trades });
    })
  );

  // GET /comparison (per-user)
  router.get(
    "/comparison",
    asyncHandler(async (req, res) => {
      const userId = (req as any).user?.id as string | undefined;
      if (!userId) return res.status(401).json({ error: "Unauthorized" });

      const { date } = req.query;
      if (!date || typeof date !== "string") {
        return res.status(422).json({ error: "date (YYYY-MM-DD) query param is required" });
      }
      const day = normalizeDate(date);

      const planDoc = await db
        .collection<DailyPlan>("daily_journal")
        .findOne({ userId, date: day });

      const plannedTrades: PlannedTrade[] = planDoc?.plannedTrades || [];

      const executedDocs = await db
        .collection<any>("executed_trades")
        .find({ userId, $or: [{ date: day }, { tradingDate: day }] })
        .toArray();

      const executed: ExecutedTrade[] = executedDocs.map((doc: any) => ({
        date: normalizeDate(doc.date || doc.tradingDate),
        symbol: doc.symbol,
        quantity: Number(doc.quantity ?? 0),
        entry: Number(doc.entry ?? 0),
        exit: typeof doc.exit !== "undefined" ? Number(doc.exit) : undefined,
        tradeType: (doc.tradeType || "BUY").toUpperCase() === "SELL" ? "SELL" : "BUY",
        PnL: typeof doc.PnL !== "undefined" ? Number(doc.PnL) : undefined,
        exchangeId: doc.exchangeId,
        instrumentType: doc.instrumentType,
        instrumentName: doc.instrumentName,
        segment: doc.segment,
        lotSize: doc.lotSize,
        expiry: doc.expiry,
        optionType: doc.optionType,
        strikePrice: doc.strikePrice,
        underlyingSymbol: doc.underlyingSymbol
      }));

      if (executed.length === 0) {
        return res.json({
          status: "no-executions",
          planned: plannedTrades,
          insights: [],
          whatWentWrong: [],
          matched: 0,
          matchedTrades: [],
          missedTrades: plannedTrades,
          extraTrades: [],
          groupedExtras: {},
          totalPlanned: plannedTrades.length,
          executionPercent: 0,
          badge: "NO DATA",
          confidenceLevel: planDoc?.confidenceLevel ?? 5,
          stressLevel: planDoc?.stressLevel ?? 5,
          distractions: planDoc?.distractions ?? "",
          sleepHours: planDoc?.sleepHours ?? 7,
          mood: planDoc?.mood ?? "",
          focus: planDoc?.focus ?? 5,
          energy: planDoc?.energy ?? 5
        });
      }

      const matchedTrades: ExecutedTrade[] = [];
      const missedTrades: PlannedTrade[] = [];
      const matchedExecutedIndexes = new Set<number>();

      plannedTrades.forEach(pt => {
        const idx = executed.findIndex((et, i) =>
          tradeObjMatch(pt, et) && !matchedExecutedIndexes.has(i)
        );
        if (idx !== -1) {
          matchedTrades.push(executed[idx]);
          matchedExecutedIndexes.add(idx);
        } else {
          missedTrades.push(pt);
        }
      });

      const extraTrades: ExecutedTrade[] = executed.filter((_, i) => !matchedExecutedIndexes.has(i));
      const groupedExtras = groupBy(extraTrades, t =>
        `${t.symbol}__${t.tradeType}__${t.expiry || ""}__${t.strikePrice || ""}__${t.optionType || ""}`
      );

      const totalPlanned = plannedTrades.length;
      const executionPercent = totalPlanned ? Math.round((matchedTrades.length / totalPlanned) * 100) : 0;

      const insights: string[] = [];
      const whatWentWrong: string[] = [];

      if (matchedTrades.length) {
        insights.push(`Executed ${matchedTrades.length} of ${totalPlanned} planned trades (${executionPercent}%)`);
      }
      if (matchedTrades.length && matchedTrades.some(t => t.PnL && Math.abs(t.PnL) > 0.01)) {
        const bestMatch = matchedTrades.reduce((a, b) => ((a.PnL || 0) > (b.PnL || 0) ? a : b));
        insights.push(`Best trade: ${bestMatch.symbol} (${bestMatch.tradeType}) @ ₹${bestMatch.entry} (P&L: ₹${bestMatch.PnL})`);
        const avgPnL = matchedTrades.reduce((sum, t) => sum + (t.PnL || 0), 0) / matchedTrades.length;
        if (Math.abs(avgPnL) > 0.01) insights.push(`Average P&L (matched): ₹${avgPnL.toFixed(2)}`);
      }
      if (extraTrades.length) {
        insights.push(`You took ${extraTrades.length} unplanned trades (overtrading).`);
      }

      if (missedTrades.length) {
        whatWentWrong.push(`You missed ${missedTrades.length} planned trade${missedTrades.length > 1 ? "s" : ""}.`);
      }
      if (extraTrades.length) {
        const typeCount: Record<string, number> = {};
        extraTrades.forEach(t => {
          const key = `${t.symbol} ${t.tradeType}`;
          typeCount[key] = (typeCount[key] || 0) + 1;
        });
        const most = Object.entries(typeCount).sort((a, b) => b[1] - a[1])[0];
        if (most && most[1] > 1) {
          whatWentWrong.push(`Most common unplanned trade: ${most[0]} (${most[1]} times)`);
        }
        whatWentWrong.push(`Try to stick to your plan and avoid impulsive/unplanned trades.`);
      }
      if (!missedTrades.length && !extraTrades.length) {
        whatWentWrong.push("Great job! You stuck to your plan. Keep it up.");
      }

      function getBadge(p: number) {
        if (p >= 90) return "MASTER";
        if (p >= 75) return "EXPERT";
        if (p >= 60) return "SKILLED";
        return "LEARNING";
      }

      res.json({
        status: "ok",
        matched: matchedTrades.length,
        totalPlanned,
        executionPercent,
        badge: getBadge(executionPercent),
        matchedTrades,
        missedTrades,
        extraTrades: Object.values(groupedExtras).flat(),
        groupedExtras,
        insights,
        whatWentWrong,
        confidenceLevel: planDoc?.confidenceLevel ?? 5,
        stressLevel: planDoc?.stressLevel ?? 5,
        distractions: planDoc?.distractions ?? "",
        sleepHours: planDoc?.sleepHours ?? 7,
        mood: planDoc?.mood ?? "",
        focus: planDoc?.focus ?? 5,
        energy: planDoc?.energy ?? 5
      });
    })
  );

  return router;
}