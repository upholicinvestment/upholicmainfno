// // server/src/redis.ts
// import { createClient } from "redis";

// let _pub: ReturnType<typeof createClient> | null = null;
// let _sub: ReturnType<typeof createClient> | null = null;

// export async function ensureRedis() {
//   if (!_pub) {
//     _pub = createClient({ url: process.env.REDIS_URL || "redis://127.0.0.1:6379" });
//     _pub.on("error", (e) => console.error("[redis pub]", e));
//     await _pub.connect();
//   }
//   if (!_sub) {
//     _sub = _pub.duplicate();
//     _sub.on("error", (e) => console.error("[redis sub]", e));
//     await _sub.connect();
//   }
//   return { pub: _pub!, sub: _sub! };
// }
