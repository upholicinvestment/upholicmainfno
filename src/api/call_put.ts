// src/api/call_put.ts
import type { Express, Request, Response, RequestHandler } from "express";
import type { Db } from "mongodb";
import crypto from "crypto";

/* ----------------------------- Types ------------------------------ */
type Leg = { oi?: number | null };

type StrikeRow = {
  strike: number;
  ce?: Leg | null;
  pe?: Leg | null;
};

type TickDoc = {
  underlying_security_id: number;
  underlying_segment: string;
  expiry: string;           // ISO yyyy-mm-dd
  ts: Date;                 // tick timestamp (UTC)
  last_price: number;       // underlying LTP
  strikes: StrikeRow[];     // normalized strikes
};

/* --------------------------- Utilities ---------------------------- */
const LOG = (process.env.OC_LOG_VERBOSE ?? "true").toLowerCase() === "true";
const log = (...a: unknown[]) => { if (LOG) console.log("[OI]", ...a); };

const toISO = (d: Date | string | number) => new Date(d).toISOString();

function detectStrikeStep(rows: StrikeRow[]): number {
  const uniques = Array.from(
    new Set(rows.map(r => Number(r?.strike)).filter(Number.isFinite))
  ).sort((a, b) => a - b);

  for (let i = 1; i < uniques.length; i++) {
    const diff = Math.abs(uniques[i] - uniques[i - 1]);
    if (diff > 0) return diff;
  }
  return 50; // sensible default for NIFTY
}

const roundToStep = (px: number, step: number) => Math.round(px / step) * step;
const minutesToMs = (min: number) => Math.max(1, Math.floor(min)) * 60 * 1000;
const binKey = (ts: Date, binMs: number) => Math.floor(ts.getTime() / binMs) * binMs;

// IST-aligned “trading day” window derived from a UTC timestamp
function tradingWindowBySinceMin(latestTs: Date, sinceMin: number): { start: Date; end: Date } {
  // We keep it simple: `sinceMin` minutes back from latest, up to latest.
  const end = new Date(latestTs);
  const start = new Date(end.getTime() - minutesToMs(sinceMin));
  return { start, end };
}

/* --------------------------- Route impls -------------------------- */
export default function registerNiftyRoutes(app: Express, db: Db) {
  const ticksCol = db.collection<TickDoc>("option_chain_ticks");

  /** ============= Existing: Fixed ATM endpoint ============= */
  const atmHandler: RequestHandler = async (req: Request, res: Response): Promise<void> => {
    try {
      const intervalMin = Math.max(1, parseInt(String(req.query.interval || "3"), 10));
      const id = Number(process.env.OC_UNDERLYING_ID ?? 13);
      const seg = String(process.env.OC_SEGMENT ?? "IDX_I");

      const latestArr = await ticksCol
        .find({ underlying_security_id: id, underlying_segment: seg })
        .sort({ ts: -1 })
        .limit(1)
        .toArray();

      if (!latestArr.length) {
        res.status(404).json({ error: "No option_chain_ticks for underlying/segment." });
        return;
      }
      const latest = latestArr[0];
      const { start, end } = tradingWindowBySinceMin(latest.ts, 24 * 60);

      const docs = await ticksCol
        .find(
          {
            underlying_security_id: id,
            underlying_segment: seg,
            expiry: latest.expiry,
            ts: { $gte: start, $lt: end },
          },
          { projection: { ts: 1, last_price: 1, strikes: 1, expiry: 1 } as const }
        )
        .sort({ ts: 1 })
        .toArray();

      if (!docs.length) {
        res.status(404).json({ error: "No ticks found in the date window." });
        return;
      }

      // step + fixed ATM at latest
      let step = 50;
      for (const d of docs) {
        if (Array.isArray(d.strikes) && d.strikes.length) { step = detectStrikeStep(d.strikes); break; }
      }
      const fixedAtm = roundToStep(latest.last_price, step);
      log("ATM window (fixed)", { expiry: latest.expiry, step, fixedAtm, start: toISO(start), end: toISO(end) });

      const binMs = minutesToMs(intervalMin);
      type AtmBin = { ts: Date; callOI: number; putOI: number };
      const bins = new Map<number, AtmBin>();

      let lastCE = 0;
      let lastPE = 0;

      for (const doc of docs) {
        const key = binKey(new Date(doc.ts), binMs);

        let ceOI = lastCE;
        let peOI = lastPE;

        if (Array.isArray(doc.strikes)) {
          const row = (doc.strikes as StrikeRow[]).find(r => r?.strike === fixedAtm);
          if (row) {
            const c = Number(row?.ce?.oi ?? NaN);
            const p = Number(row?.pe?.oi ?? NaN);
            if (Number.isFinite(c)) ceOI = c;
            if (Number.isFinite(p)) peOI = p;
          }
        }

        lastCE = ceOI;
        lastPE = peOI;
        bins.set(key, { ts: new Date(key), callOI: ceOI, putOI: peOI });
      }

      const series = Array.from(bins.entries())
        .sort((a, b) => a[0] - b[0])
        .map(([, v]) => ({
          timestamp: v.ts.toISOString(),
          atmStrike: fixedAtm,
          callOI: v.callOI,
          putOI: v.putOI,
        }));

      res.json({ expiry: latest.expiry, step, atmStrike: fixedAtm, series });
    } catch (e: any) {
      console.error("ATM handler error:", e?.message || e);
      res.status(500).json({ error: "Internal server error" });
    }
  };

  /** ============= Existing: OVERALL endpoint ============= */
  const overallHandler: RequestHandler = async (req: Request, res: Response): Promise<void> => {
    try {
      const intervalMin = Math.max(1, parseInt(String(req.query.interval || "3"), 10));
      const id = Number(process.env.OC_UNDERLYING_ID ?? 13);
      const seg = String(process.env.OC_SEGMENT ?? "IDX_I");

      const latestArr = await ticksCol
        .find({ underlying_security_id: id, underlying_segment: seg })
        .sort({ ts: -1 })
        .limit(1)
        .toArray();

      if (!latestArr.length) {
        res.status(404).json({ error: "No option_chain_ticks for underlying/segment." });
        return;
      }
      const latest = latestArr[0];
      const { start, end } = tradingWindowBySinceMin(latest.ts, 24 * 60);

      const docs = await ticksCol
        .find(
          {
            underlying_security_id: id,
            underlying_segment: seg,
            expiry: latest.expiry,
            ts: { $gte: start, $lt: end },
          },
          { projection: { ts: 1, strikes: 1, expiry: 1 } as const }
        )
        .sort({ ts: 1 })
        .toArray();

      if (!docs.length) {
        res.status(404).json({ error: "No ticks found in the date window." });
        return;
      }

      const binMs = minutesToMs(intervalMin);
      type TotalBin = { ts: Date; callOI: number; putOI: number };
      const bins = new Map<number, TotalBin>();

      for (const doc of docs) {
        const key = binKey(new Date(doc.ts), binMs);
        const strikes = Array.isArray(doc.strikes) ? (doc.strikes as StrikeRow[]) : [];
        const sumCE = strikes.reduce<number>((acc, r) => acc + (Number(r?.ce?.oi ?? 0) || 0), 0);
        const sumPE = strikes.reduce<number>((acc, r) => acc + (Number(r?.pe?.oi ?? 0) || 0), 0);
        bins.set(key, { ts: new Date(key), callOI: sumCE, putOI: sumPE });
      }

      const step = (() => {
        for (const d of docs) if (Array.isArray(d.strikes) && d.strikes.length) return detectStrikeStep(d.strikes);
        return 50;
      })();

      const series = Array.from(bins.entries())
        .sort((a, b) => a[0] - b[0])
        .map(([, v]) => ({
          timestamp: v.ts.toISOString(),
          callOI: v.callOI,
          putOI: v.putOI,
        }));

      res.json({ expiry: latest.expiry, step, series });
    } catch (e: any) {
      console.error("OVERALL handler error:", e?.message || e);
      res.status(500).json({ error: "Internal server error" });
    }
  };

  /** ============= NEW: BULK endpoint (ETag + 24h backfill) ============= */
  const bulkHandler: RequestHandler = async (req: Request, res: Response): Promise<void> => {
    try {
      const id = Number(process.env.OC_UNDERLYING_ID ?? 13);
      const seg = String(process.env.OC_SEGMENT ?? "IDX_I");

      // intervals
      const rawIntervals = String(req.query.intervals ?? "3,15,30,60")
        .split(",")
        .map((x) => Number(x.trim()))
        .filter((n) => [3, 5, 15, 30, 60].includes(n));
      const intervals = rawIntervals.length ? Array.from(new Set(rawIntervals)).sort((a, b) => a - b) : [3, 15, 30, 60];

      // window: default 24h
      const sinceMin = Math.max(1, Number(req.query.sinceMin ?? 1440));

      // latest tick → expiry and window
      const latestArr = await ticksCol
        .find({ underlying_security_id: id, underlying_segment: seg })
        .sort({ ts: -1 })
        .limit(1)
        .toArray();

      if (!latestArr.length) {
        res.status(404).json({ error: "No option_chain_ticks for underlying/segment." });
        return;
      }
      const latest = latestArr[0];
      const { start, end } = tradingWindowBySinceMin(latest.ts, sinceMin);

      // load once
      const docs = await ticksCol
        .find(
          {
            underlying_security_id: id,
            underlying_segment: seg,
            expiry: latest.expiry,
            ts: { $gte: start, $lt: end },
          },
          { projection: { ts: 1, last_price: 1, strikes: 1 } as const }
        )
        .sort({ ts: 1 })
        .toArray();

      if (!docs.length) {
        res.status(404).json({ error: "No ticks found in the window." });
        return;
      }

      // step + fixed ATM
      let step = 50;
      for (const d of docs) {
        if (Array.isArray(d.strikes) && d.strikes.length) { step = detectStrikeStep(d.strikes); break; }
      }
      const fixedAtm = roundToStep(latest.last_price, step);

      // Build per-interval bins
      type Pt = { timestamp: string; callOI: number; putOI: number };

      const buildAtmSeries = (binMin: number): Pt[] => {
        const binMs = minutesToMs(binMin);
        const bins = new Map<number, { ts: Date; callOI: number; putOI: number }>();
        let lastCE = 0;
        let lastPE = 0;

        for (const doc of docs) {
          const key = binKey(new Date(doc.ts), binMs);

          let ce = lastCE;
          let pe = lastPE;

          if (Array.isArray(doc.strikes)) {
            const row = (doc.strikes as StrikeRow[]).find(r => r?.strike === fixedAtm);
            if (row) {
              const c = Number(row?.ce?.oi ?? NaN);
              const p = Number(row?.pe?.oi ?? NaN);
              if (Number.isFinite(c)) ce = c;
              if (Number.isFinite(p)) pe = p;
            }
          }
          lastCE = ce;
          lastPE = pe;
          bins.set(key, { ts: new Date(key), callOI: ce, putOI: pe });
        }

        return Array.from(bins.entries())
          .sort((a, b) => a[0] - b[0])
          .map(([, v]) => ({ timestamp: v.ts.toISOString(), callOI: v.callOI, putOI: v.putOI }));
      };

      const buildOverallSeries = (binMin: number): Pt[] => {
        const binMs = minutesToMs(binMin);
        const bins = new Map<number, { ts: Date; callOI: number; putOI: number }>();

        for (const doc of docs) {
          const key = binKey(new Date(doc.ts), binMs);
          const strikes = Array.isArray(doc.strikes) ? (doc.strikes as StrikeRow[]) : [];
          const sumCE = strikes.reduce<number>((acc, r) => acc + (Number(r?.ce?.oi ?? 0) || 0), 0);
          const sumPE = strikes.reduce<number>((acc, r) => acc + (Number(r?.pe?.oi ?? 0) || 0), 0);
          bins.set(key, { ts: new Date(key), callOI: sumCE, putOI: sumPE });
        }

        return Array.from(bins.entries())
          .sort((a, b) => a[0] - b[0])
          .map(([, v]) => ({ timestamp: v.ts.toISOString(), callOI: v.callOI, putOI: v.putOI }));
      };

      const atmRows: Record<string, Pt[]> = {};
      const ovRows:  Record<string, Pt[]> = {};
      for (const m of intervals) {
        atmRows[String(m)] = buildAtmSeries(m);
        ovRows[String(m)]  = buildOverallSeries(m);
      }

      // ETag: stable hash over expiry + last ts + counts
      const lastTs = docs[docs.length - 1]?.ts?.toISOString() || "";
      const etagBasis = JSON.stringify({
        exp: latest.expiry,
        last: lastTs,
        atm: Object.fromEntries(Object.entries(atmRows).map(([k, v]) => [k, v.length])),
        ov:  Object.fromEntries(Object.entries(ovRows).map(([k, v]) => [k, v.length])),
        atmStrike: fixedAtm,
        step,
      });
      const etag = `"oi-${crypto.createHash("md5").update(etagBasis).digest("hex")}"`;

      const ifNoneMatch = req.headers["if-none-match"];
      if (ifNoneMatch && ifNoneMatch === etag) {
        res.status(304).end();
        return;
      }

      res.setHeader("ETag", etag);
      res.json({
        expiry: latest.expiry,
        atm: { step, atmStrike: fixedAtm, rows: atmRows },
        overall: { rows: ovRows },
      });
    } catch (e: any) {
      console.error("OI BULK error:", e?.message || e);
      res.status(500).json({ error: "Internal server error" });
    }
  };

  app.get("/api/nifty/atm", atmHandler);          // kept for backward-compat
  app.get("/api/nifty/overall", overallHandler);  // kept for backward-compat
  app.get("/api/oi/bulk", bulkHandler);           // NEW: bulk + ETag + 24h
}
