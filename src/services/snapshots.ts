// server/src/services/snapshots.ts
import { Db, ObjectId } from "mongodb";
import crypto from "crypto";

/* ---------- Types (aligned to journal stack) ---------- */
type Trade = {
  Date: string;
  Time?: string;
  Symbol: string;
  Direction: "Buy" | "Sell";
  Quantity: number;
  Price: number;
  PnL: number;
  Charges?: number;
  NetPnL: number;
};

export type RoundTrip = {
  symbol: string;
  entry: Trade;
  exit: Trade;
  legs: Trade[];
  PnL: number;
  NetPnL: number;
  holdingMinutes: number;
  Demon?: string;
  DemonArr?: string[];
  GoodPractice?: string;
  GoodPracticeArr?: string[];
  isBadTrade?: boolean;
  isGoodTrade?: boolean;
};

export type DaySnapshotDoc = {
  _id?: ObjectId;
  userId: string | null;      // store as string
  tradingDate: string;        // "YYYY-MM-DD"
  sourceId: ObjectId;         // orderbook doc id
  broker?: string | null;
  version: number;
  isSuperseded: boolean;
  frozenAt: Date;

  tradeCount: number;
  netPnl: number;
  grossProfit: number;
  grossLoss: number;
  wins: number;
  losses: number;
  winRate: number;
  profitFactor: number;
  bestTradePnl: number;
  worstTradePnl: number;
  fees: number;

  symbolCount: number;
  longCount: number;
  shortCount: number;
};

const round2 = (n: number) => Math.round(n * 100) / 100;
const round4 = (n: number) => Math.round(n * 10000) / 10000;

export function md5(buf: Buffer | string) {
  return crypto.createHash("md5").update(buf).digest("hex");
}

function normalizeDate10(d: string | undefined) {
  if (!d) return "";
  const s = String(d).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;

  // DD/MM/YYYY or MM/DD/YYYY (assume DD/MM by default for Indian data)
  const m1 = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (m1) {
    const [, a, b, y] = m1;
    const day = Number(a) > 12 ? a : b;
    const mon = Number(a) > 12 ? b : a;
    return `${y}-${mon.padStart(2, "0")}-${day.padStart(2, "0")}`;
  }

  // DD-MM-YYYY
  const m2 = s.match(/^(\d{1,2})-(\d{1,2})-(\d{4})$/);
  if (m2) {
    const [, d2, m2o, y] = m2;
    return `${y}-${m2o.padStart(2, "0")}-${d2.padStart(2, "0")}`;
  }

  return s.slice(0, 10);
}

/* ---------- Index bootstrap (call this once at startup) ---------- */
export async function ensureCalendarIndexes(db: Db) {
  /* ===== orderbooks: unique (userId, fileHash) ===== */
  const ob = db.collection("orderbooks");

  // Clean up any legacy unique { fileHash: 1 } index that lacks userId
  try {
    const idxs = await ob.listIndexes().toArray().catch(() => []);
    for (const i of idxs) {
      const key = (i.key ?? {}) as Record<string, 1 | -1>;
      const isLegacy = key.fileHash === 1 && !("userId" in key);
      // don't touch the default _id_
      if (i.name !== "_id_" && isLegacy) {
        try {
          await ob.dropIndex(i.name);
          console.log(`🧹 Dropped legacy orderbooks index: ${i.name}`);
        } catch (e) {
          console.warn("⚠ Could not drop legacy orderbooks index:", e);
        }
      }
    }
  } catch {
    /* ignore */
  }

  // Ensure (userId, fileHash) unique exists (any name)
  try {
    const have = await ob.listIndexes().toArray().catch(() => []);
    const exists = have.some((i: any) => {
      const k = i.key ?? {};
      return i.unique && k.userId === 1 && k.fileHash === 1;
    });
    if (!exists) {
      await ob.createIndex(
        { userId: 1, fileHash: 1 },
        { unique: true, background: true, name: "uniq_user_fileHash" }
      );
      console.log("✅ Ensured orderbooks unique index { userId: 1, fileHash: 1 }");
    }
  } catch (e: any) {
    // Ignore "Index already exists with a different name"
    if (e?.code !== 85) throw e;
    console.log("ℹ️ orderbooks {userId,fileHash} index already exists (different name).");
  }

  /* ===== journal_day_stats: read index ===== */
  const jds = db.collection("journal_day_stats");
  try {
    await jds.createIndex(
      { userId: 1, tradingDate: 1, isSuperseded: 1 },
      { background: true, name: "stats_user_date_activeflag" }
    );
  } catch {
    /* ignore races */
  }

  /* ===== journal_day_stats: partial unique (only active docs) ===== */
  try {
    const jIdx = await jds.listIndexes().toArray().catch(() => []);
    const hasPartialUnique = jIdx.some((i: any) => {
      const k = i.key ?? {};
      const p = i.partialFilterExpression ?? {};
      return i.unique && k.userId === 1 && k.tradingDate === 1 && p.isSuperseded === false;
    });
    if (!hasPartialUnique) {
      await jds.createIndex(
        { userId: 1, tradingDate: 1 },
        {
          unique: true,
          partialFilterExpression: { isSuperseded: false },
          background: true,
          name: "uniq_user_date_active",
        }
      );
      console.log("✅ Ensured journal_day_stats partial unique { userId, tradingDate } on active docs");
    }
  } catch (e: any) {
    if (e?.code !== 85) throw e;
    console.log("ℹ️ journal_day_stats partial unique index already exists (different name).");
  }
}

/**
 * Freeze daily snapshots from round-trips (exit.Date is the trade day).
 */
export async function freezeDaySnapshotsFromRoundTrips(
  db: Db,
  opts: {
    userId?: string | null;
    sourceId: ObjectId;
    broker?: string | null;
    roundTrips: RoundTrip[];
  }
) {
  const userIdStr: string | null = opts.userId ?? null;
  const col = db.collection<DaySnapshotDoc>("journal_day_stats");

  // Group by normalized exit date
  const byDate = new Map<string, RoundTrip[]>();
  for (const rt of opts.roundTrips) {
    const d = normalizeDate10(rt.exit?.Date);
    if (!d) continue;
    if (!byDate.has(d)) byDate.set(d, []);
    byDate.get(d)!.push(rt);
  }

  for (const [tradingDate, rts] of byDate) {
    let tradeCount = rts.length;
    let netPnl = 0, grossProfit = 0, grossLoss = 0, wins = 0, losses = 0, fees = 0;
    let bestTradePnl = -Infinity, worstTradePnl = Infinity;
    let longCount = 0, shortCount = 0;
    const symbols = new Set<string>();

    for (const rt of rts) {
      symbols.add(rt.symbol);
      if (rt.entry?.Direction === "Buy") longCount++; else shortCount++;
      const pnl = Number(rt.PnL || 0);
      netPnl += pnl;
      if (pnl > 0) { wins++; grossProfit += pnl; }
      else if (pnl < 0) { losses++; grossLoss += Math.abs(pnl); }
      if (pnl > bestTradePnl) bestTradePnl = pnl;
      if (pnl < worstTradePnl) worstTradePnl = pnl;

      // fees += Number(rt.entry?.Charges ?? 0) + Number(rt.exit?.Charges ?? 0);
    }

    const totalClosed = wins + losses;
    const winRate = totalClosed ? wins / totalClosed : 0;
    const profitFactor = grossLoss ? (grossProfit / grossLoss) : 0;

    // Supersede any active snapshot for this user+day
    await col.updateMany(
      { userId: userIdStr, tradingDate, isSuperseded: false },
      { $set: { isSuperseded: true } }
    );

    const doc: DaySnapshotDoc = {
      userId: userIdStr,
      tradingDate,
      sourceId: opts.sourceId,
      broker: opts.broker ?? null,
      version: 1,
      isSuperseded: false,
      frozenAt: new Date(),

      tradeCount,
      netPnl: round2(netPnl),
      grossProfit: round2(grossProfit),
      grossLoss: round2(grossLoss),
      wins,
      losses,
      winRate: round4(winRate),
      profitFactor: round2(profitFactor),
      bestTradePnl: isFinite(bestTradePnl) ? round2(bestTradePnl) : 0,
      worstTradePnl: isFinite(worstTradePnl) ? round2(worstTradePnl) : 0,
      fees: round2(fees),

      symbolCount: symbols.size,
      longCount,
      shortCount,
    };

    await col.insertOne(doc);
  }
}

/** Create (or reuse) an orderbook meta doc and return its _id */
export async function upsertOrderbookMeta(
  db: Db,
  opts: { fileHash: string; sourceName?: string; size?: number; userId?: string | null; broker?: string | null }
): Promise<ObjectId> {
  const obCol = db.collection("orderbooks");
  const userId = opts.userId ?? null;

  // Per-user dedupe by (userId, fileHash)
  const existing = await obCol.findOne({ userId, fileHash: opts.fileHash });
  if (existing?._id) return existing._id as ObjectId;

  const ins = await obCol.insertOne({
    userId,
    fileHash: opts.fileHash,
    sourceName: opts.sourceName || null,
    size: opts.size || null,
    broker: opts.broker || null,
    uploadedAt: new Date(),
  });
  return ins.insertedId;
}
