// // server/src/services/vi_feeder.ts
// import type { Db } from "mongodb";
// import { ensureRedis } from "../redis";
// import { computeRowsFromDBWindow } from "../services/oc_signal";
// import { setTimeout as sleep } from "timers/promises";

// type Row = { volatility: number; time: string; signal: "Bullish" | "Bearish"; spot: number };

// function keyRows(u:number, e:string, iv:number){ return `vi:rows:${u}:${e}:${iv}m`; }
// function chan(u:number, e:string, iv:number){ return `vi:rt:${u}:${e}:${iv}m`; }

// async function resolveActiveExpiry(db: Db, underlying: number, seg = "IDX_I"): Promise<string | null> {
//   const snap = await db.collection("option_chain")
//     .find({ underlying_security_id: underlying, underlying_segment: seg })
//     .project({ expiry: 1, updated_at: 1 })
//     .sort({ updated_at: -1, _id: -1 }).limit(1).toArray();
//   if (snap.length && snap[0]?.expiry) {
//     const v = snap[0].expiry;
//     return typeof v === "string" ? v.slice(0,10) : new Date(v as any).toISOString().slice(0,10);
//   }
//   const tick = await db.collection("option_chain_ticks")
//     .find({ underlying_security_id: underlying, underlying_segment: seg })
//     .project({ expiry: 1, ts: 1 })
//     .sort({ ts: -1, _id: -1 }).limit(1).toArray();
//   if (tick.length && tick[0]?.expiry) {
//     const v = tick[0].expiry;
//     return typeof v === "string" ? v.slice(0,10) : new Date(v as any).toISOString().slice(0,10);
//   }
//   return null;
// }

// export async function startViFeeder(db: Db) {
//   if (String(process.env.VI_FEEDER_ENABLED || "true").toLowerCase() !== "true") return;

//   const { pub } = await ensureRedis();

//   const underlying = Number(process.env.NIFTY_SECURITY_ID || 13);
//   const seg = process.env.NIFTY_UNDERLYING_SEG || "IDX_I";
//   const timeframes = [3, 5, 15, 30];

//   console.log("[vi_feeder] starting…");

//   (async function loop() {
//     while (true) {
//       try {
//         const expiry = await resolveActiveExpiry(db, underlying, seg);
//         if (!expiry) { await sleep(5_000); continue; }

//         for (const iv of timeframes) {
//           const rows = await computeRowsFromDBWindow(
//             process.env.MONGO_URI || "mongodb://localhost:27017",
//             process.env.MONGO_DB_NAME || process.env.DB_NAME || "Upholic",
//             underlying,
//             expiry,
//             1_000_000,
//             { mode:"level", unit:"bps", signalMode:"price", windowSteps:5, width:300, classify:true, intervalMin: iv }
//           ) as any as Row[];

//           if (!rows.length) continue;

//           const latest = rows[rows.length - 1];
//           const k = keyRows(underlying, expiry, iv);
//           const c = chan(underlying, expiry, iv);

//           // Avoid duplicate publishes
//           const last = await pub.lRange(k, -1, -1);
//           const lastRow = last.length ? (JSON.parse(last[0]) as Row) : null;
//           const changed = !lastRow
//             || lastRow.time !== latest.time
//             || lastRow.volatility !== latest.volatility
//             || Math.round(lastRow.spot) !== Math.round(latest.spot);

//           if (changed) {
//             await pub.multi()
//               .rPush(k, JSON.stringify(latest))
//               .lTrim(k, -600, -1)
//               .publish(c, JSON.stringify(latest))
//               .exec();
//           }
//         }
//       } catch (e:any) {
//         console.error("[vi_feeder]", e?.message || e);
//       } finally {
//         await sleep(5_000); // tick every 5s
//       }
//     }
//   })();
// }
