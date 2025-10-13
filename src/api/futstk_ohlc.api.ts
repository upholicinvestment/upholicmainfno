// src/api/futstk_ohlc.api.ts
import type { Express, RequestHandler } from "express";
import { fetchAndSaveFutstkOhlc, fetchOhlc } from "../services/quote.service";

export default function registerFutstkOhlcRoutes(app: Express) {
  /**
   * GET /api/futstk/ohlc?expiry=2025-10-30
   * Also accepts: 28-10-2025 or 28-10-2025 14:30:00
   * Persists into `nse_futstk_ohlc`
   */
  const handler: RequestHandler = async (req, res) => {
    const expiryParam = req.query.expiry;
    if (!expiryParam || typeof expiryParam !== "string" || !expiryParam.trim()) {
      res.status(400).json({
        error:
          "Query param 'expiry' is required. Example: /api/futstk/ohlc?expiry=2025-10-30",
      });
      return;
    }

    try {
      const summary = await fetchAndSaveFutstkOhlc(expiryParam);
      res.json({ expiry: expiryParam, ...summary });
    } catch (err: any) {
      console.error("FUTSTK OHLC route error:", err?.message || err);
      res.status(500).json({ error: "Internal server error" });
    }
  };

  /**
   * GET /api/futstk/ohlc/debug?sid=52509
   * - Quick probe to see if OHLC responds for a single id
   * - Does not write to DB
   */
  const debugHandler: RequestHandler = async (req, res) => {
    const sidParam = req.query.sid;
    const sid = Number(sidParam);
    if (!sid || !Number.isFinite(sid)) {
      res.status(400).json({ error: "Pass a numeric sid, e.g. /api/futstk/ohlc/debug?sid=52509" });
      return;
    }
    try {
      const data = await fetchOhlc([sid]);
      res.json({
        sid,
        hasData: !!data && Object.keys(data).length > 0,
        data: (data as any)?.[sid] ?? null,
      });
    } catch (err: any) {
      res.status(500).json({ error: err?.message || String(err) });
    }
  };

  app.get("/api/futstk/ohlc", handler);
  app.get("/api/futstk/ohlc/debug", debugHandler);
}
