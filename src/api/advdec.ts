// src/api/advdec.ts
import { Express, Request, Response } from "express";
import { Db } from "mongodb";
import crypto from "crypto";

/* ---------- Helpers ---------- */

// IST label for a Date
function labelIST(d: Date): string {
  return d.toLocaleTimeString("en-IN", {
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    timeZone: "Asia/Kolkata",
  });
}

function minutesAgoDate(min: number): Date {
  return new Date(Date.now() - Math.max(1, Math.floor(min)) * 60_000);
}

/** Build an ETag string from a simple identity summary */
function buildETag(identity: unknown) {
  const basis = JSON.stringify(identity);
  return `"advdec-${crypto.createHash("md5").update(basis).digest("hex")}"`;
}

/**
 * Run aggregation for a single bin size. The logic:
 *  - Filter FUTSTK (optionally a single expiry)
 *  - Restrict to received_at >= cutoff
 *  - Truncate to IST "slots" using $dateTrunc with binSize
 *  - For each (slot, security_id) keep the latest tick
 *  - Group by slot to count advances/declines
 *  - Sort by slot ascending
 */
async function fetchSeriesForBin(db: Db, binSize: number, sinceMin: number, expiry?: string) {
  const cutoff = minutesAgoDate(sinceMin);

  const baseMatch: Record<string, any> = {
    instrument_type: "FUTSTK",
    exchange: "NSE_FNO",
    received_at: { $gte: cutoff },
  };
  if (expiry) baseMatch.expiry_date = expiry;

  const pipeline: any[] = [
    { $match: baseMatch },
    {
      $addFields: {
        slot: {
          $dateTrunc: {
            date: "$received_at",
            unit: "minute",
            binSize,
            timezone: "Asia/Kolkata",
          },
        },
      },
    },
    { $sort: { received_at: -1 } },
    {
      $group: {
        _id: { slot: "$slot", security_id: "$security_id" },
        latest: { $first: "$$ROOT" },
      },
    },
    {
      $group: {
        _id: "$_id.slot",
        stocks: {
          $push: {
            LTP: "$latest.LTP",
            close: "$latest.close",
          },
        },
      },
    },
    { $sort: { _id: 1 } },
    {
      $project: {
        _id: 1,
        advances: {
          $size: {
            $filter: {
              input: "$stocks",
              as: "s",
              cond: { $gt: [{ $toDouble: "$$s.LTP" }, { $toDouble: "$$s.close" }] },
            },
          },
        },
        declines: {
          $size: {
            $filter: {
              input: "$stocks",
              as: "s",
              cond: { $lt: [{ $toDouble: "$$s.LTP" }, { $toDouble: "$$s.close" }] },
            },
          },
        },
      },
    },
  ];

  const docs = await db.collection("nse_futstk_ticks").aggregate(pipeline, { allowDiskUse: true }).toArray();

  const series = docs.map((d) => {
    const ts = new Date(d._id);
    return {
      timestamp: ts.toISOString(),
      time: labelIST(ts),
      advances: Number(d.advances || 0),
      declines: Number(d.declines || 0),
      total: Number(d.advances || 0) + Number(d.declines || 0),
    };
  });

  const latest = series.at(-1) || null;
  const current = latest
    ? { advances: latest.advances, declines: latest.declines, total: latest.total }
    : { advances: 0, declines: 0, total: 0 };

  return { series, current, lastSlotISO: latest?.timestamp || null };
}

/* ========================================================================== */
/*                            ROUTES (default + bulk)                          */
/* ========================================================================== */

export function AdvDec(app: Express, db: Db) {
  /**
   * Legacy/simple endpoint (kept for compatibility):
   * GET /api/advdec?bin=5&expiry=YYYY-MM-DD
   * Returns { current, chartData } for ONE bin only.
   */
  app.get("/api/advdec", async (req: Request, res: Response): Promise<void> => {
    try {
      const binSize = Math.max(1, Number(req.query.bin) || 5);
      const sinceMin = Math.max(1, Number(req.query.sinceMin) || 1440); // 24h backfill by default
      const expiryParam =
        typeof req.query.expiry === "string" && req.query.expiry.trim()
          ? req.query.expiry.trim()
          : undefined;

      const { series, current } = await fetchSeriesForBin(db, binSize, sinceMin, expiryParam);
      res.setHeader("Cache-Control", "no-store");
      res.json({
        current,
        chartData: series.map(({ time, advances, declines }) => ({ time, advances, declines })),
      });
    } catch (err) {
      console.error("Error in /api/advdec:", err);
      res.status(500).json({
        error: "Internal Server Error",
        message: err instanceof Error ? err.message : "Unknown error",
      });
    }
  });

  /**
   * NEW: Bulk endpoint with ETag + 24h backfill:
   * GET /api/advdec/bulk?intervals=3,5,15,30&sinceMin=1440&expiry=YYYY-MM-DD
   * Returns rows for ALL requested bins in a single payload.
   */
  app.get("/api/advdec/bulk", async (req: Request, res: Response): Promise<void> => {
    try {
      const raw = String(req.query.intervals ?? "3,5,15,30")
        .split(",")
        .map((x) => Number(x.trim()))
        .filter((n) => [1, 3, 5, 10, 15, 30, 60].includes(n));
      const intervals = raw.length ? Array.from(new Set(raw)).sort((a, b) => a - b) : [5];

      const sinceMin = Math.max(1, Number(req.query.sinceMin) || 1440); // default 24h
      const expiryParam =
        typeof req.query.expiry === "string" && req.query.expiry.trim()
          ? req.query.expiry.trim()
          : undefined;

      // Run per interval; small number (e.g. 3-4) is fine.
      const rows: Record<
        string,
        Array<{ timestamp: string; time: string; advances: number; declines: number; total: number }>
      > = {};
      let lastISO: string | null = null;
      let current = { advances: 0, declines: 0, total: 0 };

      for (const m of intervals) {
        const { series, current: cur, lastSlotISO } = await fetchSeriesForBin(db, m, sinceMin, expiryParam);
        rows[String(m)] = series;
        if (!lastISO || (lastSlotISO && lastSlotISO > lastISO)) lastISO = lastSlotISO;
        // For a representative "current", prefer the smallest bin if present
        if (m === Math.min(...intervals)) current = cur;
      }

      // ETag: identity depends on last slot + counts/lengths across all intervals
      const identity = {
        lastISO,
        keys: Object.fromEntries(Object.entries(rows).map(([k, v]) => [k, v.length])),
        cur: current,
        expiry: expiryParam || null,
        sinceMin,
      };
      const etag = buildETag(identity);

      const ifNoneMatch = req.headers["if-none-match"];
      if (ifNoneMatch && ifNoneMatch === etag) {
        res.status(304).end();
        return;
      }

      res.setHeader("ETag", etag);
      res.setHeader("Cache-Control", "no-store");
      res.json({ current, rows, lastISO });
    } catch (err) {
      console.error("Error in /api/advdec/bulk:", err);
      res.status(500).json({
        error: "Internal Server Error",
        message: err instanceof Error ? err.message : "Unknown error",
      });
    }
  });
}
