// src/controllers/gex_bulk.rou.ts
import { Router } from "express";
import { getNiftyGexBulk } from "../controllers/gex_bulk.controller";

const r = Router();

// PUBLIC bulk endpoint (no auth). Example:
// /api/gex/nifty/bulk?scope=today
// /api/gex/nifty/bulk?scope=since&sinceMin=1440
r.get("/gex/nifty/bulk", getNiftyGexBulk);

export default r;
