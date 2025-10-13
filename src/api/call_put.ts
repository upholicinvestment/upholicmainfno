// src/api/call_put.ts
import type { Express, Request, Response, RequestHandler } from "express";
import type { Db } from "mongodb";

const LOG_ON = (process.env.LOG_ATM ?? "true").toLowerCase() === "true";
const log  = (...a: any[]) => LOG_ON && console.log("[ATM]", ...a);
const warn = (...a: any[]) => console.warn("[ATM]", ...a);
const err  = (...a: any[]) => console.error("[ATM]", ...a);

const iso = (d?: Date | string | number | null) =>
  d ? new Date(d).toISOString() : null;

export default function registerNiftyRoutes(app: Express, db: Db) {
  const atmStrikesTimeline: RequestHandler = async (req: Request, res: Response): Promise<void> => {
    const t0 = Date.now();
    try {
      const intervalMin = Math.max(1, parseInt((req.query.interval as string) ?? "3", 10));
      const id  = Number(process.env.OC_UNDERLYING_ID ?? 13);
      const seg = process.env.OC_SEGMENT ?? "IDX_I";

      log("▶ hit /api/nifty/atm-strikes-timeline", { intervalMin, id, seg });

      const ticks = db.collection("option_chain_ticks");

      // 1) latest tick to detect expiry + day window
      const latest = await ticks
        .find({ underlying_security_id: id, underlying_segment: seg })
        .sort({ ts: -1 })
        .limit(1)
        .toArray();

      if (!latest.length) {
        warn("no ticks found for underlying/segment; is watcher running?");
        res.status(404).json({ error: "No option_chain_ticks found for the given underlying." });
        return;
      }

      const { expiry, ts: latestTs } = latest[0] as any;
      log("latest tick", { expiry, latestTs: iso(latestTs) });

      // (UTC) trading day of latest
      const d = new Date(latestTs);
      const dayStart = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
      const dayEnd = new Date(dayStart);
      dayEnd.setUTCDate(dayEnd.getUTCDate() + 1);
      log("date window", { dayStart: iso(dayStart), dayEnd: iso(dayEnd) });

      // 2) fetch all ticks for that day+expiry
      const docs = await ticks.find(
        {
          underlying_security_id: id,
          underlying_segment: seg,
          expiry,
          ts: { $gte: dayStart, $lt: dayEnd },
        },
        { projection: { _id: 0, ts: 1, last_price: 1, strikes: 1 } }
      ).sort({ ts: 1 }).toArray();

      if (!docs.length) {
        warn("no ticks inside date window for detected expiry");
        res.status(404).json({ error: "No ticks in date window for detected expiry." });
        return;
      }

      log("fetched ticks", {
        count: docs.length,
        first: iso(docs[0].ts),
        last: iso(docs[docs.length - 1].ts),
      });

      // 3) bin & compute ATM OI
      const binMs = intervalMin * 60 * 1000;
      const detectStep = (sample: Array<{ strike: number }>): number => {
        const arr = Array.from(new Set(sample.map(s => Number(s.strike)).filter(Number.isFinite)))
          .sort((a, b) => a - b);
        for (let i = 1; i < arr.length; i++) {
          const diff = Math.abs(arr[i] - arr[i - 1]);
          if (diff > 0) return diff;
        }
        return 50;
      };
      const roundToStep = (px: number, step: number) => Math.round(px / step) * step;

      let strikeStep = 50;
      type Bin = { ts: Date; ltp: number; ceOI: number | null; peOI: number | null; callTs: Date | null; putTs: Date | null; };
      const bins = new Map<number, Bin>();

      // detect step from first doc that has strikes
      for (const doc of docs) {
        if (Array.isArray(doc.strikes) && doc.strikes.length) {
          strikeStep = detectStep(doc.strikes as any);
          break;
        }
      }
      log("strike step", { strikeStep });

      for (const doc of docs) {
        const ts = new Date(doc.ts);
        const ltp = Number(doc.last_price ?? 0);
        if (!Number.isFinite(ltp) || ltp <= 0) continue;

        const bkey = Math.floor(ts.getTime() / binMs) * binMs;
        const atm = roundToStep(ltp, strikeStep);

        let ce: number | null = null, pe: number | null = null;
        if (Array.isArray(doc.strikes)) {
          const row = (doc.strikes as any[]).find(s => s?.strike === atm);
          if (row) {
            ce = Number(row?.ce?.oi ?? null);
            pe = Number(row?.pe?.oi ?? null);
          }
        }

        bins.set(bkey, {
          ts,
          ltp,
          ceOI: Number.isFinite(ce as any) ? (ce as number) : null,
          peOI: Number.isFinite(pe as any) ? (pe as number) : null,
          callTs: ce != null ? ts : null,
          putTs:  pe != null ? ts : null,
        });
      }

      if (!bins.size) {
        warn("no bins computed from ticks");
        res.status(404).json({ error: "No bins computed from ticks." });
        return;
      }

      const result = Array.from(bins.entries())
        .sort((a, b) => a[0] - b[0])
        .map(([epoch, v]) => {
          const atmStrike = Math.round(v.ltp / strikeStep) * strikeStep;
          return {
            atmStrike,
            niftyLTP: v.ltp,
            timestamp: iso(epoch)!,
            callOI: v.ceOI,
            putOI: v.peOI,
            callTimestamp: iso(v.callTs),
            putTimestamp: iso(v.putTs),
          };
        });

      log("bins summary", { bins: bins.size, first: result[0], last: result[result.length - 1] });
      log("done in ms", Date.now() - t0);

      res.json({ expiry, step: strikeStep, atmStrikes: result });
      return;
    } catch (e: any) {
      err("handler error", e?.message || e);
      res.status(500).json({ error: "Internal server error" });
      return;
    }
  };

  app.get("/api/nifty/atm-strikes-timeline", atmStrikesTimeline);
}
