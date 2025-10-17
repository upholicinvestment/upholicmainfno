// // server/src/controllers/vi_fast.controller.ts
// import type { RequestHandler } from "express";
// import type { Db } from "mongodb";
// import { ensureRedis } from "../redis";
// import { computeRowsFromDBWindow } from "../services/oc_signal";

// type Row = { volatility: number; time: string; signal: "Bullish" | "Bearish"; spot: number };

// let _db: Db | undefined;
// export const viSetDb = (db: Db) => { _db = db; };
// const requireDb = () => { if (!_db) throw new Error("DB not set"); return _db; };

// const clean = (s: string) => (s || "").trim();

// function viKeyRows(u: number, exp: string, iv: number) {
//   return `vi:rows:${u}:${exp}:${iv}m`; // Redis List (oldest -> newest)
// }
// function viKeyMeta(u: number, exp: string, iv: number) {
//   return `vi:meta:${u}:${exp}:${iv}m`; // Redis Hash (expiry,resolvedAt,updatedAt)
// }
// function viChan(u: number, exp: string, iv: number) {
//   return `vi:rt:${u}:${exp}:${iv}m`; // PubSub channel
// }

// async function resolveActiveExpiry(underlying: number, seg = "IDX_I"): Promise<string | null> {
//   const db = requireDb();
//   const collOC = db.collection("option_chain");
//   const snap = await collOC.find(
//     { underlying_security_id: underlying, underlying_segment: seg },
//     { projection: { expiry: 1, updated_at: 1 } }
//   ).sort({ updated_at: -1, _id: -1 }).limit(1).toArray();
//   if (snap.length && snap[0]?.expiry) {
//     const v = snap[0].expiry;
//     return typeof v === "string" ? v.slice(0,10) : new Date(v as any).toISOString().slice(0,10);
//   }
//   const tick = await requireDb().collection("option_chain_ticks")
//     .find({ underlying_security_id: underlying, underlying_segment: seg })
//     .project({ expiry: 1, ts: 1 })
//     .sort({ ts: -1, _id: -1 }).limit(1).toArray();
//   if (tick.length && tick[0]?.expiry) {
//     const v = tick[0].expiry;
//     return typeof v === "string" ? v.slice(0,10) : new Date(v as any).toISOString().slice(0,10);
//   }
//   return null;
// }

// /** Seed/overwrite the Redis list with rows (kept oldest->newest) */
// async function seedRows(u: number, exp: string, iv: number, rows: Row[]) {
//   const { pub } = await ensureRedis();
//   const key = viKeyRows(u, exp, iv);
//   const meta = viKeyMeta(u, exp, iv);
//   const pipe = pub.multi();
//   pipe.del(key);
//   if (rows.length) {
//     // RPUSH to keep order oldest->newest
//     pipe.rPush(key, rows.map(r => JSON.stringify(r)));
//   }
//   pipe.hSet(meta, { expiry: exp, updatedAt: new Date().toISOString() });
//   pipe.expire(key, 60 * 60);   // 1h
//   pipe.expire(meta, 60 * 60);
//   await pipe.exec();
// }

// /** Append the newest row and publish to live channel */
// async function appendAndPublish(u: number, exp: string, iv: number, row: Row) {
//   const { pub } = await ensureRedis();
//   const key = viKeyRows(u, exp, iv);
//   const meta = viKeyMeta(u, exp, iv);
//   await pub.multi()
//     .rPush(key, JSON.stringify(row))
//     .lTrim(key, -600, -1) // keep last 600
//     .hSet(meta, { updatedAt: new Date().toISOString() })
//     .publish(viChan(u, exp, iv), JSON.stringify(row))
//     .exec();
// }

// async function readCache(u: number, exp: string, iv: number, limit = 200): Promise<Row[]> {
//   const { pub } = await ensureRedis();
//   // last 'limit' items from right
//   const items = await pub.lRange(viKeyRows(u, exp, iv), -limit, -1);
//   return items.map(s => JSON.parse(s) as Row);
// }

// /* ============= GET /api/vi/rows (cold load from Redis, fallback DB) ============= */
// export const getViRowsFast: RequestHandler = async (req, res) => {
//   try {
//     requireDb();
//     const underlying = Number(req.query.underlying ?? 13);
//     const seg = clean(String(req.query.segment || "IDX_I"));
//     const iv = Math.max(1, Math.min(30, Number(req.query.intervalMin || 3)));
//     const requestedExp = (String(req.query.expiry || "auto") || "").slice(0,10);
//     const limitParam = Number(req.query.limit);
//     const limit = Number.isFinite(limitParam) ? Math.min(1000, Math.max(1, limitParam)) : 200;

//     let expiry = requestedExp.toLowerCase() === "auto" ? (await resolveActiveExpiry(underlying, seg)) : requestedExp;
//     if (!expiry) { res.status(404).json({ error: "no_active_expiry" }); return; }

//     // 1) Try Redis first
//     const cached = await readCache(underlying, expiry, iv, limit);
//     if (cached.length) {
//       res.setHeader("Cache-Control", "no-store");
//       res.setHeader("X-Resolved-Expiry", expiry);
//       res.json(cached);
//       return;
//     }

//     // 2) Fallback to DB, then seed Redis
//     const mode = (String(req.query.mode ?? "level") === "delta") ? "delta" : "level";
//     const unitParam = String(req.query.unit ?? "bps").toLowerCase();
//     const unit = unitParam === "points" ? "points" : unitParam === "pct" ? "pct" : "bps";
//     const signalMode = String(req.query.signalMode ?? "price") as "price" | "delta" | "hybrid";
//     const windowSteps = Number.isFinite(Number(req.query.windowSteps)) ? Math.max(1, Number(req.query.windowSteps)) : 5;
//     const width = Number.isFinite(Number(req.query.width)) ? Math.max(50, Number(req.query.width)) : 300;
//     const classify = String(req.query.classify ?? "1") !== "0";

//     const rows: Row[] = await computeRowsFromDBWindow(
//       process.env.MONGO_URI || "mongodb://localhost:27017",
//       process.env.MONGO_DB_NAME || process.env.DB_NAME || "Upholic",
//       underlying,
//       expiry,
//       1_000_000,
//       { mode, unit, signalMode, windowSteps, width, classify, intervalMin: iv }
//     ) as any;

//     await seedRows(underlying, expiry, iv, rows);
//     res.setHeader("Cache-Control", "no-store");
//     res.setHeader("X-Resolved-Expiry", expiry);
//     res.json(rows.slice(-limit));
//     return;
//   } catch (e: any) {
//     console.error("[getViRowsFast]", e?.message || e);
//     if (!res.headersSent) res.status(500).json({ error: "server_error", detail: String(e?.message || e) });
//     return;
//   }
// };

// /* ============= SSE /api/vi/stream (live updates via Redis PubSub) ============= */
// export const sseViStream: RequestHandler = async (req, res) => {
//   try {
//     const underlying = Number(req.query.underlying ?? 13);
//     const seg = clean(String(req.query.segment || "IDX_I"));
//     const iv = Math.max(1, Math.min(30, Number(req.query.intervalMin || 3)));
//     const requestedExp = (String(req.query.expiry || "auto") || "").slice(0,10);
//     let expiry = requestedExp.toLowerCase() === "auto" ? (await resolveActiveExpiry(underlying, seg)) : requestedExp;
//     if (!expiry) { res.status(404).json({ error: "no_active_expiry" }); return; }

//     const { sub } = await ensureRedis();
//     const channel = viChan(underlying, expiry, iv);

//     // SSE headers
//     res.writeHead(200, {
//       "Content-Type": "text/event-stream",
//       "Cache-Control": "no-cache, no-transform",
//       Connection: "keep-alive",
//       "X-Accel-Buffering": "no",
//     });
//     res.write(`event: meta\ndata: ${JSON.stringify({ expiry })}\n\n`);

//     const onMsg = (message: string) => {
//       res.write(`data: ${message}\n\n`); // send latest row only
//     };

//     await sub.subscribe(channel, onMsg);

//     // keepalive
//     const keep = setInterval(() => res.write(":\n\n"), 20_000);

//     req.on("close", async () => {
//       clearInterval(keep);
//       try { await sub.unsubscribe(channel, onMsg); } catch {}
//       res.end();
//     });
//     return;
//   } catch (e: any) {
//     console.error("[sseViStream]", e?.message || e);
//     if (!res.headersSent) res.status(500).json({ error: "server_error" });
//     return;
//   }
// };
