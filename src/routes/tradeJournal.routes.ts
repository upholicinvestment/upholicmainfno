import { Router, Request, Response } from "express";
import { tradeJournalUpload, parseUniversalTradebook, processTrades, Stats, Trade } from "../services/tradeJournal";
import fs from "fs";
import path from "path";
import { Db } from "mongodb";
import { md5, upsertOrderbookMeta, freezeDaySnapshotsFromRoundTrips } from "../services/snapshots";

// --- Date Normalizer (YYYY-MM-DD) ---
function normalizeTradeDate(dateStr: string | undefined | null): string {
  if (!dateStr) return "";
  if (/^\d{4}-\d{2}-\d{2}/.test(dateStr)) return dateStr.slice(0, 10);

  // Try M/D/YYYY or MM/DD/YYYY
  let mdy = dateStr.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (mdy) {
    let [_, m, d, y] = mdy;
    return `${y}-${m.padStart(2, "0")}-${d.padStart(2, "0")}`;
  }

  // Try D/M/YYYY or DD/MM/YYYY
  let dmy = dateStr.match(/^(\d{1,2})-(\d{1,2})-(\d{4})/);
  if (dmy) {
    let [_, d, m, y] = dmy;
    return `${y}-${m.padStart(2, "0")}-${d.padStart(2, "0")}`;
  }

  // Fallback: slice to 10
  return dateStr.slice(0, 10);
}

export default function registerTradeJournalRoutes(db: Db) {
  const router = Router();

  // keep last stats per user (avoid cross-user leakage)
  const lastStatsByUser = new Map<string, Stats>();

  router.post(
  "/upload-orderbook",
  tradeJournalUpload.single("orderbook"),
  async (req: Request, res: Response): Promise<void> => {
    const softOk = (payload: any): void => {
      res.status(200).json({ ok: true, ...payload });
    };
    const softFail = (code: string, message: string): void => {
      res.status(200).json({ ok: false, code, message });
    };

    if (!req.file) {
      softFail("NO_FILE", "No file uploaded.");
      return;
    }

    const filePath = path.resolve(req.file.path);
    const originalName = req.file.originalname;
    const fileSize = req.file.size;

    const cleanup = (): void => {
      try {
        if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
      } catch {
        /* ignore */
      }
    };

    // Only CSVs are accepted (prevents parser errors)
    const isCsv =
      /\.csv$/i.test(originalName) ||
      (req.file.mimetype && /csv/i.test(req.file.mimetype || ""));
    if (!isCsv) {
      cleanup();
      softFail("BAD_TYPE", "Only .csv files are accepted.");
      return;
    }

    try {
      const userId = (req as any).user?.id as string | undefined;
      if (!userId) {
        cleanup();
        softFail("UNAUTHORIZED", "Unauthorized.");
        return;
      }

      // Read bytes once for hash, then parse
      const rawBytes = fs.readFileSync(filePath);
      const fileHash = md5(rawBytes);

      // Parse — any error is a human error (wrong CSV), not a server fault
      let trades: Trade[] = [];
      try {
        trades = await parseUniversalTradebook(filePath);
      } catch {
        cleanup();
        softFail("WRONG_CSV", "Wrong CSV. Please upload the original orderbook CSV.");
        return;
      }

      if (!trades.length) {
        cleanup();
        softFail("WRONG_CSV", "Wrong CSV. Please upload the original orderbook CSV.");
        return;
      }

      // 1) Stats
      const stats = processTrades(trades);
      lastStatsByUser.set(userId, stats);

      // 2) Bulk upserts
      const ops: any[] = [];
      for (const t of trades) {
        if (!t.Date || !t.Symbol || !t.Direction || !t.Price || !t.Quantity) continue;

        const tradingDate = normalizeTradeDate(t.Date);
        if (!tradingDate) continue;

        const symbol = String(t.Symbol).trim();
        const tradeType = String(t.Direction).toUpperCase(); // BUY/SELL
        const entry = Number(t.Price);
        const quantity = Number(t.Quantity);

        ops.push({
          updateOne: {
            filter: { userId, tradingDate, symbol, tradeType, entry, quantity },
            update: {
              $setOnInsert: {
                userId,
                tradingDate,
                symbol,
                tradeType,
                entry,
                quantity,
                createdAt: new Date(),
              },
            },
            upsert: true,
          },
        });
      }

      let upserted = 0;
      if (ops.length) {
        const result = await db.collection("executed_trades").bulkWrite(ops, { ordered: false });
        upserted = result?.upsertedCount ?? 0;
      }

      // 3) Meta + snapshots
      const broker = null;
      const sourceId = await upsertOrderbookMeta(db, {
        fileHash,
        sourceName: originalName,
        size: fileSize,
        userId,
        broker,
      });

      await freezeDaySnapshotsFromRoundTrips(db, {
        userId,
        sourceId,
        broker,
        roundTrips: stats.trades,
      });

      cleanup();
      softOk({
        message: `Orderbook uploaded, ${upserted} executed trade(s) recorded (new), and day snapshots frozen.`,
        daysFrozen: Array.from(
          new Set(stats.trades.map((rt) => normalizeTradeDate(rt.exit.Date)))
        ).filter(Boolean).length,
        fileHash,
      });
      return;
    } catch {
      cleanup();
      // even unexpected faults “soft-fail” to avoid red logs in the browser
      softFail("SERVER_ERROR", "Could not process the file. Please try again.");
      return;
    }
  }
);


  router.get("/stats", (req: Request, res: Response) => {
    const userId = (req as any).user?.id as string | undefined;
    const s = userId ? lastStatsByUser.get(userId) : null;
    if (!s) {
      res.status(200).json({
        netPnl: 0,
        tradeWinPercent: 0,
        profitFactor: 0,
        dayWinPercent: 0,
        avgWinLoss: { avgWin: 0, avgLoss: 0 },
        upholicScore: 0,
        upholicPointers: { patience: 0, demonFinder: [], planOfAction: [] },
        trades: [],
        tradeDates: [],
        empty: true,
        totalBadTradeCost: 0,
        goodPracticeCounts: {},
      });
      return;
    }
    res.status(200).json(s);
  });

  return router;
}
