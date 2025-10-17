// // server/src/routes/vi_fast.routes.ts
// import { Router } from "express";
// import { getViRowsFast, sseViStream } from "../controllers/vi_fast.controller";

// const r = Router();
// r.get("/vi/rows", getViRowsFast);     // cold load from Redis; DB on miss
// r.get("/vi/stream", sseViStream);     // live rows via SSE
// export default r;
