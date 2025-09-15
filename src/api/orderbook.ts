import { Express, Request, Response, RequestHandler } from "express";
import type { Db } from "mongodb";

/** ---------- Tiny in-memory cache (1.5s) ---------- */
type CacheEntry = { expiresAt: number; payload: any };
const cache = new Map<string, CacheEntry>();
const CACHE_TTL_MS = 1500;

const cacheKey = (
  url: string,
  params?: Record<string, string | number | boolean | null | undefined>
) => {
  const u = new URL(url);
  if (params) {
    Object.entries(params).forEach(([k, v]) => {
      if (v !== undefined && v !== null) u.searchParams.set(k, String(v));
    });
  }
  return u.toString();
};

/** ---------- fetchJson with timeout + retries ---------- */
async function fetchJson<T>(
  url: string,
  opts: {
    method?: "GET" | "POST";
    params?: Record<string, string | number | boolean | null | undefined>;
    timeoutMs?: number;
    retries?: number;
    backoffMs?: number;
  } = {}
): Promise<T> {
  const method = opts.method ?? "GET";
  const timeoutMs = opts.timeoutMs ?? 10000;
  const retries = Math.max(0, opts.retries ?? 2);
  const backoffMs = Math.max(50, opts.backoffMs ?? 250);

  const u = new URL(url);
  if (opts.params) {
    for (const [k, v] of Object.entries(opts.params)) {
      if (v !== undefined && v !== null) u.searchParams.set(k, String(v));
    }
  }

  let attempt = 0;
  while (true) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const r = await fetch(u.toString(), {
        method,
        signal: controller.signal,
        headers: { Accept: "application/json" },
      });

      clearTimeout(timer);

      if (r.status >= 500 && attempt < retries) {
        attempt++;
        await new Promise((res) => setTimeout(res, backoffMs * Math.pow(2, attempt - 1)));
        continue;
      }

      if (!r.ok) {
        const text = await r.text().catch(() => "");
        let detail: any = text;
        try { detail = JSON.parse(text); } catch {}
        const err: any = new Error(`HTTP ${r.status}`);
        err.status = r.status;
        err.detail = detail || text || `HTTP ${r.status}`;
        throw err;
      }

      return (await r.json()) as T;
    } catch (e: any) {
      clearTimeout(timer);
      const isAbort = e?.name === "AbortError";
      const isNetwork =
        e?.code === "ECONNRESET" ||
        e?.code === "ECONNREFUSED" ||
        e?.code === "UND_ERR_CONNECT_TIMEOUT";
      if ((isAbort || isNetwork) && attempt < retries) {
        attempt++;
        await new Promise((res) => setTimeout(res, backoffMs * Math.pow(2, attempt - 1)));
        continue;
      }
      throw e;
    }
  }
}

/** ---------- Helpers ---------- */
const safeArray = (v: any): any[] => {
  if (Array.isArray(v)) return v;
  if (Array.isArray(v?.data)) return v.data;
  if (Array.isArray(v?.data?.data)) return v.data.data;
  return [];
};
const toNum = (x: any): number => {
  const n = typeof x === "number" ? x : Number(x);
  return Number.isFinite(n) ? n : 0;
};

// Build "DD-MMM-YYYY" for today in IST
const todayKeyIST = (): string => {
  const now = new Date();
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "Asia/Kolkata",
    day: "2-digit",
    month: "short",
    year: "numeric",
  }).formatToParts(now);
  const dd = parts.find(p => p.type === "day")?.value ?? "";
  const mon = parts.find(p => p.type === "month")?.value ?? "";
  const yy = parts.find(p => p.type === "year")?.value ?? "";
  return `${dd}-${mon}-${yy}`;
};

// Extract "DD-MMM-YYYY" from OB row times
const pickDateKeyFromOB = (x: any): string => {
  const raw = String(x.updatetime ?? x.exchorderupdatetime ?? x.exchtime ?? "");
  const key = raw.split(" ")[0] || "";
  return key;
};

// parse time like "03-Sep-2025 10:42:00" -> ms
const parseObTime = (s: any): number => {
  const t = String(s ?? "");
  const ms = Date.parse(t);
  return Number.isFinite(ms) ? ms : 0;
};
// prefer averageprice when present (>0), else price
const pickFillPrice = (x: any): number => {
  const avg = toNum(x.averageprice);
  if (avg > 0) return avg;
  const p = toNum(x.price);
  return Number.isFinite(p) ? p : 0;
};

// prefer avg price if present (>0), else price
const obPreferFillPrice = (x: any): number => {
  const avg = toNum(x?.averageprice);
  if (avg > 0) return avg;
  const p = toNum(x?.price);
  return Number.isFinite(p) ? p : 0;
};
const obParseTime = (x: any): number => {
  const s = String(x?.updatetime ?? x?.exchorderupdatetime ?? x?.exchtime ?? "");
  const ms = Date.parse(s);
  return Number.isFinite(ms) ? ms : 0;
};
// Unique row id (deterministic)
const obRowUID = (x: any) =>
  String(x?.uniqueorderid || "").trim() ||
  `${String(x?.orderid || "").trim()}|${String(x?.exchorderupdatetime || "").trim()}`;

/** ---------- userId helpers ---------- */
const getUserIdFromReq = (req: Request): string | undefined => {
  const fromHeader = String(req.get("x-user-id") || "").trim();
  if (fromHeader) return fromHeader;
  const fromQuery = String(req.query.userId || "").trim();
  if (fromQuery) return fromQuery;
  return undefined;
};

export function Orderbook(app: Express, _db: Db) {
  // Point to your Flask service (no env usage)
  // const FLASK_BASE_URL = "http://127.0.0.1:5000";
  const FLASK_BASE_URL = "https://tech.upholictech.com";
  const WEBHOOK_SECRET = "KesiNgrokSecret2025"; // keep as-is or move to config if you prefer
  const TV_PREFIX = "TV_";
  const FLASK_USER_PARAM = "user_id";

  const getStatus = (e: unknown) => (e as any)?.status ?? 502;
  const getDetail = (e: unknown) => (e as any)?.detail ?? String(e);

  /** ---------- passthrough endpoints (user-scoped) ---------- */
  const passthru =
  (path: string): RequestHandler =>
  async (req, res) => {
    const url = `${FLASK_BASE_URL}${path}`;
    const userId = getUserIdFromReq(req);

    // always send user_id to Flask
    const params: Record<string, any> = { secret: WEBHOOK_SECRET };
    if (userId) params[FLASK_USER_PARAM] = userId;

    const key = cacheKey(url, params);
    const now = Date.now();

    const cachedEntry = cache.get(key);
    if (cachedEntry && cachedEntry.expiresAt > now) {
      res.json(cachedEntry.payload);
      return;
    }

    try {
      const raw = await fetchJson<any>(url, {
        params,
        timeoutMs: 10000,
        retries: 2,
        backoffMs: 250,
      });
      cache.set(key, { expiresAt: now + CACHE_TTL_MS, payload: raw });
      res.json(raw);
    } catch (e) {
      res.status(getStatus(e)).json({ error: `${path.slice(1)}_failed`, detail: getDetail(e) });
    }
  };

  // app.get("/api/orderbook", passthru("/orderbook"));
  app.get("/api/orderbook", passthru("/angel/user/orderbook"));
  app.get("/api/tradebook", passthru("/tradebook"));
  app.get("/api/pnl",       passthru("/pnl"));

  /** ---------- /api/summary : TODAY (IST) ALGO-only; computed only from /orderbook ---------- */
  type Summary = {
    totalPnl: number;
    totalTrades: number;
    openPositions: number;
    successRatePct: number;
    riskReward: number;
  };

  const nowIST = (): Date =>
    new Date(new Date().toLocaleString("en-US", { timeZone: "Asia/Kolkata" }));


app.get("/api/summary", (async (req: Request, res: Response) => {
  try {
    // Require user: prevents writing under null
    const userId = getUserIdFromReq(req);
    if (!userId) return res.status(400).json({ error: "missing_user_id" });

    // Fetch user-scoped orderbook from Flask (snake_case param)
    const obRaw = await fetchJson<any>(`${FLASK_BASE_URL}/angel/user/orderbook`, {
      timeoutMs: 10000,
      retries: 2,
      backoffMs: 250,
      params: { secret: WEBHOOK_SECRET, user_id: userId },
    });

    const todayKey = todayKeyIST();

    type OB = {
      symbol: string;
      side: "BUY" | "SELL";
      qty: number;
      price: number;
      status: string;
      tag: string;
      t: number;
    };

    const obTodayAlgo: OB[] = safeArray(obRaw)
      .filter((x: any) => String(x?.ordertag ?? "").startsWith(TV_PREFIX)) // ALGO only
      .filter((x: any) => pickDateKeyFromOB(x) === todayKey)               // Today (IST)
      .map((x: any) => {
        const sideRaw = String(x?.transactiontype ?? "").toUpperCase();
        const side: "BUY" | "SELL" = sideRaw === "SELL" ? "SELL" : "BUY";
        return {
          symbol: String(x?.tradingsymbol ?? "").toUpperCase(),
          side,
          qty: toNum(x?.quantity ?? x?.filledshares ?? x?.filledqty ?? 0),
          price: pickFillPrice(x),
          status: String(x?.status ?? "").toLowerCase(),
          tag: String(x?.ordertag ?? ""),
          t: parseObTime(x?.updatetime ?? x?.exchorderupdatetime ?? x?.exchtime),
        };
      });

    const completeAlgoToday = obTodayAlgo
      .filter(r => r.status === "complete" && r.qty > 0 && Number.isFinite(r.price))
      .sort((a, b) => a.t - b.t);

    const totalTrades = completeAlgoToday.length;

    type Lot = { qty: number; price: number; side: "LONG" | "SHORT" };
    const books = new Map<string, Lot[]>();
    let realisedPnl = 0;
    let wins = 0, losses = 0;
    let sumWin = 0, sumLoss = 0;

    const pushLot = (sym: string, lot: Lot) => {
      const arr = books.get(sym) ?? [];
      arr.push(lot);
      books.set(sym, arr);
    };

    const closeAgainst = (sym: string, incomingSide: "BUY" | "SELL", qty: number, price: number) => {
      const inv = books.get(sym) ?? [];
      let remaining = qty;
      const matchFn = (l: Lot) => (incomingSide === "BUY" ? l.side === "SHORT" : l.side === "LONG");

      while (remaining > 0) {
        const idx = inv.findIndex(matchFn);
        if (idx === -1) break;
        const lot = inv[idx];
        const m = Math.min(remaining, lot.qty);

        let slicePnl = 0;
        if (lot.side === "LONG" && incomingSide === "SELL") slicePnl = (price - lot.price) * m;
        else if (lot.side === "SHORT" && incomingSide === "BUY") slicePnl = (lot.price - price) * m;

        realisedPnl += slicePnl;
        if (slicePnl > 0) { wins++; sumWin += slicePnl; }
        else if (slicePnl < 0) { losses++; sumLoss += Math.abs(slicePnl); }

        lot.qty -= m;
        remaining -= m;
        if (lot.qty === 0) inv.splice(idx, 1);
      }

      if (remaining > 0) {
        const side: Lot["side"] = incomingSide === "BUY" ? "LONG" : "SHORT";
        pushLot(sym, { qty: remaining, price, side });
      } else {
        books.set(sym, inv);
      }
    };

    for (const r of completeAlgoToday) {
      if (r.side === "BUY") closeAgainst(r.symbol, "BUY", r.qty, r.price);
      else closeAgainst(r.symbol, "SELL", r.qty, r.price);
    }

    const denom = wins + losses;
    const successRatePct = denom > 0 ? (wins / denom) * 100 : 0;
    const riskReward = sumLoss > 0 ? (sumWin / sumLoss) : 0;

    const openPositions = Array.from(books.values()).filter(lots =>
      lots.reduce((s, l) => s + l.qty, 0) !== 0
    ).length;

    type Summary = {
      totalPnl: number;
      totalTrades: number;
      openPositions: number;
      successRatePct: number;
      riskReward: number;
    };

    const payload: Summary = {
      totalPnl: Math.round(realisedPnl * 100) / 100,
      totalTrades,
      openPositions,
      successRatePct: Math.round(successRatePct * 10) / 10,
      riskReward: Math.round(riskReward * 100) / 100,
    };

    // Upsert per (userId, dateKey)
    const summaryColl = _db.collection("api_summary");
    await summaryColl.updateOne(
      { userId, dateKey: todayKey },
      {
        $set: {
          ...payload,
          userId,
          dateKey: todayKey,
          ts: nowIST(),
          source: "api/summary",
        },
        $setOnInsert: { createdAt: nowIST() },
        $currentDate: { updatedAt: true },
      },
      { upsert: true }
    );

    res.json(payload);
  } catch (e) {
    res.status(getStatus(e)).json({ error: "summary_failed", detail: getDetail(e) });
  }
}) as RequestHandler);


  /** ---------- /api/strategies/summary (same as you had; not user-mounted collection) ---------- */
  app.get("/api/strategies/summary", (async (req: Request, res: Response) => {
    try {
      const coll = _db.collection("executions");

      const from = req.query.from ? new Date(String(req.query.from)) : undefined;
      const to   = req.query.to   ? new Date(String(req.query.to))   : undefined;
      const strategyNameFilter = req.query.strategy ? String(req.query.strategy) : undefined;

      const match: any = {};
      if (from) match.ts = { ...(match.ts || {}), $gte: from };
      if (to)   match.ts = { ...(match.ts || {}), $lte: to };
      if (strategyNameFilter) match.strategyName = strategyNameFilter;

      const cursor = coll.find(match, {
        projection: { _id: 0, ts: 1, strategyName: 1, symbol: 1, side: 1, qty: 1, price: 1, sl: 1 }
      }).sort({ strategyName: 1, symbol: 1, ts: 1 });

      const execs = await cursor.toArray();

      type Lot = { qty: number; price: number; sl?: number | null };
      type Metrics = { pnl: number; trades: number; wins: number; rrSum: number; rrCount: number };

      const byStream = new Map<string, any[]>();
      for (const e of execs) {
        const k = `${String(e.strategyName)}||${String(e.symbol).toUpperCase()}`;
        if (!byStream.has(k)) byStream.set(k, []);
        byStream.get(k)!.push(e);
      }

      const byStrategy = new Map<string, Metrics>();
      const ensure = (name: string) => {
        if (!byStrategy.has(name)) byStrategy.set(name, { pnl: 0, trades: 0, wins: 0, rrSum: 0, rrCount: 0 });
        return byStrategy.get(name)!;
      };

      const isNum = (n: any) => typeof n === "number" && Number.isFinite(n);

      const closeLots = (
        fromLots: Lot[],
        qtyToClose: number,
        exitPrice: number,
        isClosingLong: boolean
      ) => {
        let realizedPnl = 0, tradesClosed = 0, wins = 0, rrSumDelta = 0, rrCountDelta = 0, closedQty = 0;

        while (qtyToClose > 0 && fromLots.length > 0) {
          const lot = fromLots[0];
          const take = Math.min(qtyToClose, lot.qty);

          const entry = lot.price;
          const pnlPerUnit = isClosingLong ? (exitPrice - entry) : (entry - exitPrice);
          const pnlSlice = pnlPerUnit * take;

          realizedPnl += pnlSlice;
          closedQty += take;

          lot.qty -= take;
          const fullyClosed = lot.qty === 0;

          if (fullyClosed) {
            tradesClosed += 1;
            if (pnlSlice > 0) wins += 1;

            if (isNum(lot.sl) && (lot.sl as number) !== entry) {
              const riskPerUnit = Math.abs(entry - (lot.sl as number));
              if (riskPerUnit > 0) {
                rrSumDelta += (pnlPerUnit / riskPerUnit);
                rrCountDelta += 1;
              }
            }
            fromLots.shift();
          }
          qtyToClose -= take;
        }

        return { realizedPnl, tradesClosed, wins, rrSumDelta, rrCountDelta, closedQty };
      };

      for (const [, stream] of byStream.entries()) {
        const strategyName = String(stream[0].strategyName);
        const m = ensure(strategyName);

        const longLots: Lot[] = [];
        const shortLots: Lot[] = [];

        for (const e of stream) {
          const side = String(e.side ?? "").toUpperCase();
          const qty  = Math.max(0, Number(e.qty) || 0);
          const price = Number(e.price) || 0;
          const sl = isNum(e.sl) ? Number(e.sl) : null;
          if (!qty) continue;

          if (side === "BUY") {
            const c = closeLots(shortLots, qty, price, /*isClosingLong=*/false);
            m.pnl     += c.realizedPnl;
            m.trades  += c.tradesClosed;
            m.wins    += c.wins;
            m.rrSum   += c.rrSumDelta;
            m.rrCount += c.rrCountDelta;

            const remain = qty - c.closedQty;
            if (remain > 0) longLots.push({ qty: remain, price, sl });

          } else if (side === "SELL") {
            const c = closeLots(longLots, qty, price, /*isClosingLong=*/true);
            m.pnl     += c.realizedPnl;
            m.trades  += c.tradesClosed;
            m.wins    += c.wins;
            m.rrSum   += c.rrSumDelta;
            m.rrCount += c.rrCountDelta;

            const remain = qty - c.closedQty;
            if (remain > 0) shortLots.push({ qty: remain, price, sl });

          } else if (side === "EXIT") {
            const c1 = closeLots(longLots, longLots.reduce((a, l) => a + l.qty, 0), price, true);
            m.pnl     += c1.realizedPnl;
            m.trades  += c1.tradesClosed;
            m.wins    += c1.wins;
            m.rrSum   += c1.rrSumDelta;
            m.rrCount += c1.rrCountDelta;

            const c2 = closeLots(shortLots, shortLots.reduce((a, l) => a + l.qty, 0), price, false);
            m.pnl     += c2.realizedPnl;
            m.trades  += c2.tradesClosed;
            m.wins    += c2.wins;
            m.rrSum   += c2.rrSumDelta;
            m.rrCount += c2.rrCountDelta;
          }
        }
      }

      const data = Array.from(byStrategy.entries()).map(([strategyName, m]) => {
        const winRate = m.trades ? (m.wins / m.trades) * 100 : 0;
        const avgRR = m.rrCount ? (m.rrSum / m.rrCount) : null;
        return {
          strategyName,
          pnl: Number(m.pnl.toFixed(2)),
          trades: m.trades,
          winRatePct: Number(winRate.toFixed(2)),
          avgRR: avgRR === null ? null : Number(avgRR.toFixed(2)),
        };
      }).sort((a, b) => b.pnl - a.pnl);

      res.json({ ok: true, data });
    } catch (e: any) {
      res.status(500).json({ ok: false, error: e?.message || "Server error" });
    }
  }) as RequestHandler);

  // ===== Indexes (user-scoped) =====
  _db.collection("api_summary").createIndex({ userId: 1, dateKey: 1 }, { unique: true }).catch(() => {});
  const obRawColl = _db.collection("orderbook_raw");
  obRawColl.createIndex({ userId: 1, uid: 1 }, { unique: true }).catch(() => {});
  obRawColl.createIndex({ userId: 1, ordertag: 1, status: 1 }).catch(() => {});
  _db.collection("tv_signal_tags")
     .createIndex({ userId: 1, orderTag: 1 }, { unique: true }).catch(() => {});
  _db.collection("tv_signal_tags")
     .createIndex({ userId: 1, strategyName: 1 }).catch(() => {});

  /** ----------
   * GET /api/summary/history?limit=14  (user-scoped)
   * ---------- */
  app.get("/api/summary/history", (async (req, res) => {
    try {
      const userId = getUserIdFromReq(req);
      const limit = Math.min(100, Math.max(1, Number(req.query.limit) || 14));
      const coll = _db.collection("api_summary");

      const docs = await coll
        .find({ userId: userId || null }, { projection: { _id: 0 } })
        .sort({ updatedAt: -1, ts: -1 })
        .limit(limit)
        .toArray();

      res.json({ ok: true, data: docs });
    } catch (e: any) {
      res.status(500).json({ ok: false, error: e?.message || "Server error" });
    }
  }) as RequestHandler);

  /** ----------
   * GET /api/summary/by-date?dateKey=04-Sep-2025  (user-scoped)
   * ---------- */
  app.get("/api/summary/by-date", (async (req, res) => {
    try {
      const userId = getUserIdFromReq(req);
      const dateKey = String(req.query.dateKey || "").trim();
      if (!dateKey) return res.status(400).json({ ok: false, error: "Missing dateKey" });

      const coll = _db.collection("api_summary");
      const doc = await coll.findOne({ userId: userId || null, dateKey }, { projection: { _id: 0 } });

      if (!doc) return res.status(404).json({ ok: false, error: "Not found" });
      res.json({ ok: true, data: doc });
    } catch (e: any) {
      res.status(500).json({ ok: false, error: e?.message || "Server error" });
    }
  }) as RequestHandler);

  /** ---------- SAVE RAW ORDERBOOK (user-scoped) ---------- */
  const ALGO_PREFIX = (typeof TV_PREFIX !== "undefined" ? TV_PREFIX : "TV_");

  /**
   * GET /api/orderbook/save-raw
   * Fetches Flask /orderbook for this user and upserts ALGO rows into Mongo
   */
  app.get("/api/orderbook/save-raw", (async (req, res) => {
    try {
      const userId = getUserIdFromReq(req) || null;

      // const raw = await fetchJson<any>(`${FLASK_BASE_URL}/orderbook`, {
      // const raw = await fetchJson<any>(`${FLASK_BASE_URL}/angel/user/orderbook`, {
      //   timeoutMs: 10000, retries: 2, backoffMs: 250,
      //   params: { secret: WEBHOOK_SECRET, ...(userId ? { userId } : {}) }
      // });

      const raw = await fetchJson<any>(`${FLASK_BASE_URL}/angel/user/orderbook`, {
        timeoutMs: 10000, retries: 2, backoffMs: 250,
        params: { secret: WEBHOOK_SECRET, ...(userId ? { user_id: userId } : {}) }
      });
    // console.log(raw)
      const rows: any[] = safeArray(raw);

      const algoRows = rows.filter((x) => String(x?.ordertag ?? "").startsWith(ALGO_PREFIX));

      if (!algoRows.length) {
        return res.json({ ok: true, total: 0, inserted: 0, updated: 0, skipped: 0 });
      }

      let inserted = 0, updated = 0, skipped = 0;
      for (const x of algoRows) {
        const uid = obRowUID(x);
        if (!uid) { skipped++; continue; }

        const doc = { ...x, uid, userId };
        const r = await obRawColl.updateOne(
          { userId, uid },
          {
            $set: doc,
            $setOnInsert: { createdAt: new Date() },
            $currentDate: { updatedAt: true },
          },
          { upsert: true }
        );

        if (r.upsertedCount) inserted++;
        else if (r.modifiedCount) updated++;
      }

      res.json({ ok: true, total: algoRows.length, inserted, updated, skipped });
    } catch (e: any) {
      res.status(500).json({ ok: false, error: e?.message || "Save failed" });
    }
  }) as RequestHandler);

  /** =================== P&L per trade (from orderbook) — user-scoped =================== **/
  type SideDir = "LONG" | "SHORT";
  interface TradeSliceDoc {
    sliceKey: string;
    symbol: string;
    side: SideDir;
    qty: number;
    entryPrice: number;
    exitPrice: number;
    pnlPerUnit: number;
    pnl: number;
    volume: number;
    profitOrLoss: "profit" | "loss" | "breakeven";
    entry: { t: number; orderUid: string; updatetime: string };
    exit:  { t: number; orderUid: string; updatetime: string };
    dateKey: string;
    tag: string;
    userId: string | null;
    createdAt?: Date;
    updatedAt?: Date;
  }
  const pnlColl = _db.collection<TradeSliceDoc>("pnl_trades");
  pnlColl.createIndex({ userId: 1, sliceKey: 1 }, { unique: true }).catch(() => {});
  pnlColl.createIndex({ userId: 1, dateKey: 1, symbol: 1 }).catch(() => {});

  app.get("/api/pnl/trades/save", (async (req, res) => {
    try {
      const userId = getUserIdFromReq(req) || null;

      // const raw = await fetchJson<any>(`${FLASK_BASE_URL}/orderbook`, {
      // const raw = await fetchJson<any>(`${FLASK_BASE_URL}/angel/user/orderbook`, {
      //   timeoutMs: 10000, retries: 2, backoffMs: 250,
      //   params: { secret: WEBHOOK_SECRET, ...(userId ? { userId } : {}) }
      // });

      const raw = await fetchJson<any>(`${FLASK_BASE_URL}/angel/user/orderbook`, {
  timeoutMs: 10000, retries: 2, backoffMs: 250,
  params: { secret: WEBHOOK_SECRET, ...(userId ? { user_id: userId } : {}) }
});

      const rows: any[] = safeArray(raw);

      type Ev = {
        sym: string;
        side: "BUY" | "SELL";
        qty: number;
        price: number;
        t: number;
        uid: string;
        updatetime: string;
        dateKey: string;
        tag: string;
        raw: any;
      };

      const events: Ev[] = rows
        .filter(r => String(r?.ordertag ?? "").startsWith(ALGO_PREFIX))
        .filter(r => String(r?.status ?? r?.orderstatus ?? "").toLowerCase() === "complete")
        .filter(r => toNum(r?.filledshares ?? r?.filledqty ?? 0) > 0)
        .map(r => {
          const sideRaw = String(r?.transactiontype ?? "").toUpperCase();
          const side: "BUY" | "SELL" = sideRaw === "SELL" ? "SELL" : "BUY";
          return {
            sym: String(r?.tradingsymbol ?? "").toUpperCase(),
            side,
            qty: toNum(r?.filledshares ?? r?.filledqty ?? r?.quantity ?? 0),
            price: obPreferFillPrice(r),
            t: obParseTime(r),
            uid: obRowUID(r),
            updatetime: String(r?.updatetime ?? r?.exchorderupdatetime ?? r?.exchtime ?? ""),
            dateKey: pickDateKeyFromOB(r) || todayKeyIST(),
            tag: String(r?.ordertag ?? ""),
            raw: r,
          };
        })
        .filter(e => e.qty > 0 && Number.isFinite(e.price))
        .sort((a, b) => a.t - b.t);

      type Lot = {
        qty: number;
        price: number;
        t: number;
        orderUid: string;
        updatetime: string;
        tag: string;
        side: SideDir;
      };

      const longs = new Map<string, Lot[]>();
      const shorts = new Map<string, Lot[]>();

      let inserted = 0, updated = 0, totalSlices = 0;

      const upsertSlice = async (d: Omit<TradeSliceDoc, "userId">) => {
        const fullDoc: TradeSliceDoc = { ...d, userId };
        const r = await pnlColl.updateOne(
          { userId, sliceKey: d.sliceKey },
          {
            $set: fullDoc,
            $setOnInsert: { createdAt: new Date() },
            $currentDate: { updatedAt: true },
          },
          { upsert: true }
        );
        if (r.upsertedCount) inserted++;
        else if (r.modifiedCount) updated++;
      };

      for (const e of events) {
        if (e.side === "BUY") {
          // BUY closes SHORTs then remainder LONG
          let remaining = e.qty;
          const book = shorts.get(e.sym) ?? [];
          while (remaining > 0 && book.length > 0) {
            const lot = book[0];
            const take = Math.min(remaining, lot.qty);
            const pnlPerUnit = (lot.price - e.price);
            const pnl = pnlPerUnit * take;
            const volume = take * (lot.price + e.price);
            const profitOrLoss = pnl > 0 ? "profit" : pnl < 0 ? "loss" : "breakeven";

            await upsertSlice({
              sliceKey: `${e.sym}|SHORT|${lot.t}|${e.t}|${lot.price}|${e.price}|${take}`,
              symbol: e.sym,
              side: "SHORT",
              qty: take,
              entryPrice: lot.price,
              exitPrice: e.price,
              pnlPerUnit,
              pnl,
              volume,
              profitOrLoss,
              entry: { t: lot.t, orderUid: lot.orderUid, updatetime: lot.updatetime },
              exit:  { t: e.t,   orderUid: e.uid,        updatetime: e.updatetime },
              dateKey: e.dateKey,
              tag: lot.tag || e.tag,
            });
            totalSlices++;

            lot.qty -= take;
            remaining -= take;
            if (lot.qty === 0) book.shift();
          }
          shorts.set(e.sym, book);

          if (remaining > 0) {
            const arr = longs.get(e.sym) ?? [];
            arr.push({
              qty: remaining,
              price: e.price,
              t: e.t,
              orderUid: e.uid,
              updatetime: e.updatetime,
              tag: e.tag,
              side: "LONG",
            });
            longs.set(e.sym, arr);
          }
        } else {
          // SELL closes LONGs then remainder SHORT
          let remaining = e.qty;
          const book = longs.get(e.sym) ?? [];
          while (remaining > 0 && book.length > 0) {
            const lot = book[0];
            const take = Math.min(remaining, lot.qty);
            const pnlPerUnit = (e.price - lot.price);
            const pnl = pnlPerUnit * take;
            const volume = take * (lot.price + e.price);
            const profitOrLoss = pnl > 0 ? "profit" : pnl < 0 ? "loss" : "breakeven";

            await upsertSlice({
              sliceKey: `${e.sym}|LONG|${lot.t}|${e.t}|${lot.price}|${e.price}|${take}`,
              symbol: e.sym,
              side: "LONG",
              qty: take,
              entryPrice: lot.price,
              exitPrice: e.price,
              pnlPerUnit,
              pnl,
              volume,
              profitOrLoss,
              entry: { t: lot.t, orderUid: lot.orderUid, updatetime: lot.updatetime },
              exit:  { t: e.t,   orderUid: e.uid,        updatetime: e.updatetime },
              dateKey: e.dateKey,
              tag: lot.tag || e.tag,
            });
            totalSlices++;

            lot.qty -= take;
            remaining -= take;
            if (lot.qty === 0) book.shift();
          }
          longs.set(e.sym, book);

          if (remaining > 0) {
            const arr = shorts.get(e.sym) ?? [];
            arr.push({
              qty: remaining,
              price: e.price,
              t: e.t,
              orderUid: e.uid,
              updatetime: e.updatetime,
              tag: e.tag,
              side: "SHORT",
            });
            shorts.set(e.sym, arr);
          }
        }
      }

      res.json({
        ok: true,
        totals: { events: events.length, slices: totalSlices, inserted, updated },
      });
    } catch (e: any) {
      res.status(500).json({ ok: false, error: e?.message || "Server error" });
    }
  }) as RequestHandler);

  /** ========================================================================
   *  /api/strategies/pnl — user-scoped join: tv_signal_tags & orderbook_raw
   * ======================================================================== */
  app.get("/api/strategies/pnl", (async (req: Request, res: Response) => {
    try {
      const userId = getUserIdFromReq(req) || null;

      const strategyParam = (req.query.strategy ? String(req.query.strategy) : "").trim();
      const strategies = strategyParam
        ? strategyParam.split(",").map(s => s.trim()).filter(Boolean)
        : null;

      const from = req.query.from ? new Date(String(req.query.from)) : undefined;
      const to   = req.query.to   ? new Date(String(req.query.to))   : undefined;
      const fromMs = from?.getTime();
      const toMs   = to?.getTime();

      const tagsColl = _db.collection("tv_signal_tags");
      const obColl   = _db.collection("orderbook_raw");

      const tagMatch: any = { userId };
      if (strategies && strategies.length) tagMatch.strategyName = { $in: strategies };

      const tagDocs = await tagsColl.find(tagMatch, { projection: { _id: 0, orderTag: 1, strategyName: 1 } }).toArray();
      if (!tagDocs.length) return res.json({ ok: true, data: [] });

      const tagToStrategy = new Map<string, string>();
      for (const d of tagDocs) {
        tagToStrategy.set(String(d.orderTag), String(d.strategyName || ""));
      }

      const allTags = Array.from(tagToStrategy.keys());
      const obRows = await obColl.find(
        { userId, ordertag: { $in: allTags } },
        {
          projection: {
            _id: 0,
            ordertag: 1,
            status: 1,
            transactiontype: 1,
            side: 1,
            tradingsymbol: 1,
            symbol: 1,
            quantity: 1,
            qty: 1,
            filledshares: 1,
            filledqty: 1,
            averageprice: 1,
            price: 1,
            updatetime: 1,
            exchorderupdatetime: 1,
            exchtime: 1,
            timestamp: 1,
            createdAt: 1
          }
        }
      ).toArray();

      type Evt = { strategyName: string; symbol: string; side: "BUY" | "SELL"; qty: number; price: number; t: number; };
      const events: Evt[] = [];
      for (const r of obRows) {
        const status = String(r.status ?? "").toLowerCase();
        if (!status.includes("complete")) continue;

        const tag = String(r.ordertag ?? "");
        const strat = tagToStrategy.get(tag);
        if (!strat) continue;

        const sideRaw = String(r.transactiontype ?? r.side ?? "").toUpperCase();
        const side: "BUY" | "SELL" = sideRaw === "SELL" ? "SELL" : "BUY";
        const qty = toNum(r.filledshares ?? r.filledqty ?? r.quantity ?? r.qty ?? 0);
        const price = pickFillPrice(r);
        if (!(qty > 0) || !(price > 0)) continue;

        const t = parseObTime(r.updatetime ?? r.exchorderupdatetime ?? r.exchtime ?? r.timestamp ?? r.createdAt);
        if (fromMs && (!t || t < fromMs)) continue;
        if (toMs   && (!t || t > toMs)) continue;

        const symbol = String(r.tradingsymbol ?? r.symbol ?? "").toUpperCase();
        events.push({ strategyName: strat, symbol, side, qty, price, t });
      }

      if (!events.length) return res.json({ ok: true, data: [] });

      type Lot = { qty: number; price: number; side: "LONG" | "SHORT" };
      const byStrategy = new Map<string, Evt[]>();
      for (const e of events) {
        if (!byStrategy.has(e.strategyName)) byStrategy.set(e.strategyName, []);
        byStrategy.get(e.strategyName)!.push(e);
      }

      const out = [] as Array<{
        strategyName: string;
        pnl: number;
        orders: number;
        roundTrips: number;
        wins: number;
        losses: number;
        winRatePct: number;
        rnr: number | null;
        openPositions: number;
      }>;

      for (const [strategyName, rows] of byStrategy.entries()) {
        rows.sort((a, b) => a.t - b.t);

        const books = new Map<string, Lot[]>();
        const pushLot = (sym: string, lot: Lot) => {
          const arr = books.get(sym) ?? [];
          arr.push(lot);
          books.set(sym, arr);
        };

        let pnl = 0;
        let orders = rows.length;
        let roundTrips = 0;
        let wins = 0, losses = 0;
        let sumWin = 0, sumLoss = 0;

        const closeAgainst = (sym: string, incomingSide: "BUY" | "SELL", qty: number, price: number) => {
          const inv = books.get(sym) ?? [];
          let remaining = qty;
          const matchFn = (l: Lot) => (incomingSide === "BUY" ? l.side === "SHORT" : l.side === "LONG");

          while (remaining > 0) {
            const idx = inv.findIndex(matchFn);
            if (idx === -1) break;
            const lot = inv[idx];
            const m = Math.min(remaining, lot.qty);

            let slicePnl = 0;
            if (lot.side === "LONG" && incomingSide === "SELL") slicePnl = (price - lot.price) * m;
            else if (lot.side === "SHORT" && incomingSide === "BUY") slicePnl = (lot.price - price) * m;

            pnl += slicePnl;
            lot.qty -= m;
            remaining -= m;

            if (lot.qty === 0) {
              roundTrips += 1;
              if (slicePnl > 0) { wins += 1; sumWin += slicePnl; }
              else if (slicePnl < 0) { losses += 1; sumLoss += Math.abs(slicePnl); }
              inv.splice(idx, 1);
            } else {
              if (slicePnl > 0) { wins += 1; sumWin += slicePnl; }
              else if (slicePnl < 0) { losses += 1; sumLoss += Math.abs(slicePnl); }
            }
          }

          if (remaining > 0) {
            const side: Lot["side"] = incomingSide === "BUY" ? "LONG" : "SHORT";
            pushLot(sym, { qty: remaining, price, side });
          } else {
            books.set(sym, inv);
          }
        };

        for (const r of rows) {
          if (r.side === "BUY") closeAgainst(r.symbol, "BUY", r.qty, r.price);
          else closeAgainst(r.symbol, "SELL", r.qty, r.price);
        }

        const openPositions = Array.from(books.values()).filter(lots =>
          lots.reduce((s, l) => s + l.qty, 0) !== 0
        ).length;

        const denom = wins + losses;
        const winRatePct = denom ? (wins / denom) * 100 : 0;
        const rnr = sumLoss > 0 ? (sumWin / sumLoss) : (wins > 0 ? null : null);

        out.push({
          strategyName,
          pnl: Number(pnl.toFixed(2)),
          orders,
          roundTrips,
          wins,
          losses,
          winRatePct: Number(winRatePct.toFixed(2)),
          rnr: rnr === null ? null : Number(rnr.toFixed(2)),
          openPositions
        });
      }

      let data = out;
      if (!strategies) {
        data = out.sort((a, b) => b.pnl - a.pnl);
      } else {
        const orderMap = new Map(strategies.map((s, i) => [s, i]));
        data = out.sort((a, b) => (orderMap.get(a.strategyName) ?? 9999) - (orderMap.get(b.strategyName) ?? 9999));
      }

      res.json({ ok: true, from: from?.toISOString() ?? null, to: to?.toISOString() ?? null, data });
    } catch (e: any) {
      res.status(500).json({ ok: false, error: e?.message || "Server error" });
    }
  }) as RequestHandler);
}
