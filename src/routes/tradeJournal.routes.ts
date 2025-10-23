
import { Router, Request, Response } from "express";
import {
  tradeJournalUpload,
  parseUniversalTradebook,
  processTrades,
  Stats,
  Trade,
  standardDemons,
  standardGood,
} from "../services/tradeJournal";
import fs from "fs";
import path from "path";
import { Db } from "mongodb";
import {
  md5,
  upsertOrderbookMeta,
  freezeDaySnapshotsFromTradesPairedRaw, // <-- new freezer
} from "../services/snapshots";

/* ---------- Date Normalizer (YYYY-MM-DD) ---------- */
function normalizeTradeDate(dateStr: string | undefined | null): string {
  if (!dateStr) return "";
  if (/^\d{4}-\d{2}-\d{2}/.test(dateStr)) return dateStr.slice(0, 10);

  const mdy = dateStr.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (mdy) { const [, m, d, y] = mdy; return `${y}-${m.padStart(2,"0")}-${d.padStart(2,"0")}`; }

  const dmy = dateStr.match(/^(\d{1,2})[\/-](\d{1,2})[\/-](\d{4})/);
  if (dmy) { const [, d, m, y] = dmy; return `${y}-${m.padStart(2,"0")}-${d.padStart(2,"0")}`; }

  const dMonY = dateStr.match(/^(\d{1,2})-([A-Za-z]{3})-(\d{4})/);
  if (dMonY) {
    const [, d, mon, y] = dMonY;
    const idx = ["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"].indexOf(mon.toLowerCase());
    if (idx >= 0) return `${y}-${String(idx+1).padStart(2,"0")}-${String(d).padStart(2,"0")}`;
  }

  return dateStr.slice(0, 10);
}

export default function registerTradeJournalRoutes(app: unknown, db: Db) {
  const router = Router();
  const lastStatsByUser = new Map<string, Stats>();

  router.post(
    "/upload-orderbook",
    tradeJournalUpload.single("orderbook"),
    async (req: Request, res: Response): Promise<void> => {

      // helpers MUST not return a Response (TS fix)
      const sendOk = (payload: any): void => {
        res.status(200).json({ ok: true, ...payload });
      };
      const sendErr = (code: string, message: string): void => {
        res.status(200).json({ ok: false, code, message });
      };

      if (!req.file) { sendErr("NO_FILE", "No file uploaded."); return; }

      const filePath = path.resolve(req.file.path);
      const originalName = req.file.originalname || "orderbook.csv";
      const fileSize = req.file.size ?? 0;

      const cleanup = (): void => { try { if (fs.existsSync(filePath)) fs.unlinkSync(filePath); } catch {} };

      const isCsv = /\.csv$/i.test(originalName) ||
        /csv|ms-excel|text\/plain/i.test((req.file.mimetype || "").toLowerCase());
      if (!isCsv) { cleanup(); sendErr("BAD_TYPE", "Only .csv files are accepted."); return; }

      try {
        const userId = (req as any).user?.id as string | undefined;
        if (!userId) { cleanup(); sendErr("UNAUTHORIZED", "Unauthorized."); return; }

        const rawBytes = fs.readFileSync(filePath);
        const fileHash = md5(rawBytes);

        let trades: Trade[] = [];
        try { trades = await parseUniversalTradebook(filePath); }
        catch { cleanup(); sendErr("WRONG_CSV", "Wrong CSV. Please upload the original orderbook CSV."); return; }
        if (!trades.length) { cleanup(); sendErr("WRONG_CSV", "Wrong CSV. Please upload the original orderbook CSV."); return; }

        // 1) Stats (paired-raw basis)
        const stats = processTrades(trades);
        lastStatsByUser.set(userId, stats);

        // 2) Upsert executed_trades idempotently
        const ops: any[] = [];
        for (const t of trades) {
          if (!t.Date || !t.Symbol || !t.Direction || !t.Price || !t.Quantity) continue;
          const tradingDate = normalizeTradeDate(t.Date); if (!tradingDate) continue;

          ops.push({
            updateOne: {
              filter: {
                userId, tradingDate,
                symbol: String(t.Symbol).trim(),
                tradeType: String(t.Direction).toUpperCase(),
                entry: Number(t.Price),
                quantity: Number(t.Quantity),
              },
              update: { $setOnInsert: {
                userId, tradingDate,
                symbol: String(t.Symbol).trim(),
                tradeType: String(t.Direction).toUpperCase(),
                entry: Number(t.Price),
                quantity: Number(t.Quantity),
                createdAt: new Date(),
              }},
              upsert: true,
            }
          });
        }
        let upserted = 0;
        if (ops.length) {
          const r = await db.collection("executed_trades").bulkWrite(ops, { ordered: false });
          upserted = r?.upsertedCount ?? 0;
        }

        // 3) Meta + freeze daily snapshots on the new basis (with roundTrips for metadata)
        const broker = null;
        const sourceId = await upsertOrderbookMeta(db, { fileHash, sourceName: originalName, size: fileSize, userId, broker });
        await freezeDaySnapshotsFromTradesPairedRaw(db, {
          userId, sourceId, broker, trades, roundTrips: stats.trades,
        });

        cleanup();
        sendOk({
          message: `Orderbook uploaded, ${upserted} executed trade(s) recorded (new), and day snapshots frozen.`,
          daysFrozen: Array.from(new Set(trades.map(t => normalizeTradeDate(t.Date)).filter(Boolean))).length,
          fileHash,
          pnlBasis: (stats as any).pnlBasis ?? "PAIRED_RAW",
          headline: {
            netPnl: stats.netPnl,
            charges: (stats as any).pairedTotals?.charges,
            buyQty: (stats as any).pairedTotals?.buyQty,
            sellQty: (stats as any).pairedTotals?.sellQty,
          },
          totalsCheck: (stats as any).totalsCheck,
        });
      } catch {
        cleanup();
        sendErr("SERVER_ERROR", "Could not process the file. Please try again.");
      }
    }
  );

  router.get("/stats", (req: Request, res: Response) => {
    const userId = (req as any).user?.id as string | undefined;
    const s = userId ? lastStatsByUser.get(userId) : null;

    if (!s) {
      const empty: Stats = {
        netPnl: 0,
        // @ts-ignore keep compatible with older Stats
        pnlBasis: "PAIRED_RAW",
        // @ts-ignore
        totalsCheck: { netPnlFromScrips: 0, chargesFromScrips: 0 },

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
        totalGoodTradeProfit: 0,
        // @ts-ignore
        badTradeCounts: {},
        // @ts-ignore
        goodTradeCounts: {},
        standardDemons,
        standardGood,
        enteredTooSoonCount: 0,

        scripSummary: [],

        // @ts-ignore for backward compatibility
        pairedTotals: { buyQty: 0, sellQty: 0, avgBuy: 0, avgSell: 0, charges: 0, netPnl: 0 },
        // @ts-ignore
        openPositions: [],
      };
      res.status(200).json(empty);
      return;
    }

    res.status(200).json(s);
  });

  return router;
}
