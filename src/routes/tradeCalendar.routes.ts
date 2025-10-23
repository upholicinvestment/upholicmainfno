import { Router, Request, Response } from "express";
import { Db } from "mongodb";

function monthRangeStrings(year: number, month1to12: number) {
  const m = Math.max(1, Math.min(12, month1to12));
  const startStr = `${year}-${String(m).padStart(2, "0")}-01`;
  const nextMonth = m === 12 ? 1 : m + 1;
  const nextYear = m === 12 ? year + 1 : year;
  const endStr = `${nextYear}-${String(nextMonth).padStart(2, "0")}-01`;
  return { startStr, endStr };
}

export default function registerTradeCalendarRoutes(app: unknown, db: Db): Router {
  const router = Router();
  const col = db.collection("journal_day_stats");

  // GET /trade-calendar/month?year=2025&month=9
  router.get("/month", async (req: Request, res: Response): Promise<void> => {
    try {
      const userId = (req as any).user?.id || (req.query.userId as string | undefined) || null;
      if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }

      const year = Number(req.query.year);
      const month = Number(req.query.month);
      if (!year || !month) { res.status(400).json({ error: "year and month (1-12) are required" }); return; }

      const { startStr, endStr } = monthRangeStrings(year, month);
      const filter = { userId, isSuperseded: false, tradingDate: { $gte: startStr, $lt: endStr } };

      const days = await col
        .find(filter)
        .project({ _id: 0, tradingDate: 1, tradeCount: 1, netPnl: 1, winRate: 1, profitFactor: 1, bestTradePnl: 1 })
        .sort({ tradingDate: 1 })
        .toArray();

      const full = await col.find(filter).toArray();

      let totalTrades = 0, netPnl = 0, wins = 0, losses = 0, gp = 0, gl = 0;
      let bestDay: { date: string | null; netPnl: number } = { date: null, netPnl: 0 };

      for (const d of full) {
        totalTrades += d.tradeCount || 0;
        netPnl += d.netPnl || 0;
        wins += d.wins || 0;
        losses += d.losses || 0;
        gp += d.grossProfit || 0;
        gl += d.grossLoss || 0;
        if (!bestDay.date || (d.netPnl || 0) > bestDay.netPnl) bestDay = { date: d.tradingDate, netPnl: d.netPnl || 0 };
      }

      const winRate = (wins + losses) ? wins / (wins + losses) : 0;
      const profitFactor = gl ? gp / gl : (gp > 0 ? Infinity : 0);

      res.json({
        days,
        monthSummary: {
          totalTrades,
          netPnl: Math.round(netPnl * 100) / 100,
          winRate,
          profitFactor,
          bestDay,
        },
      });
    } catch (e: any) {
      res.status(500).json({ error: e?.message || "Failed to build month" });
    }
  });

  // GET /trade-calendar/day/2025-09-08
  router.get("/day/:date", async (req: Request, res: Response): Promise<void> => {
    try {
      const userId = (req as any).user?.id || (req.query.userId as string | undefined) || null;
      if (!userId) { res.status(401).json({ error: "Unauthorized" }); return; }

      const tradingDate = String(req.params.date).slice(0, 10);
      const snap: any = await col.findOne({ userId, tradingDate, isSuperseded: false });

      if (!snap) { res.json({ snapshot: null, executedLegs: [] }); return; }

      // backfill PF if missing
      if (snap.profitFactor == null) {
        const gp = snap.grossProfit || 0, gl = snap.grossLoss || 0;
        snap.profitFactor = gl ? gp / gl : (gp > 0 ? Infinity : 0);
      }

      const trades = await db
        .collection("executed_trades")
        .find({ userId, tradingDate })
        .project({ _id: 0 })
        .toArray();

      res.json({ snapshot: snap, executedLegs: trades });
    } catch (e: any) {
      res.status(500).json({ error: e?.message || "Failed to load day" });
    }
  });

  return router;
}
