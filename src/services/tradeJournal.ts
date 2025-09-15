import multer from "multer";
import csvParser from "csv-parser";
import fs from "fs";
import path from "path";

// ----------- TYPES -----------
export interface Trade {
  Date: string;
  Time?: string;
  Symbol: string;
  Direction: "Buy" | "Sell";
  Quantity: number;
  Price: number;
  PnL: number;
  Charges?: number;
  NetPnL: number;
  stopDistance?: number;
  executed?: boolean;
  Demon?: string;
  DemonArr?: string[];
  GoodPractice?: string;
  GoodPracticeArr?: string[];
  isBadTrade?: boolean;
  isGoodTrade?: boolean;

  // internal helpers
  _fullQty?: number;
  buyPriceRaw?: number;
  sellPriceRaw?: number;
}

export interface RoundTrip {
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
}

export interface ScripSummaryRow {
  symbol: string;
  quantity: number;
  avgBuy: number;
  avgSell: number;
  charges: number;
  netRealized: number;
}

export interface Stats {
  netPnl: number;
  tradeWinPercent: number;
  profitFactor: number;
  dayWinPercent: number;
  avgWinLoss: { avgWin: number; avgLoss: number };
  upholicScore: number;
  upholicPointers: {
    patience: number;
    demonFinder: string[];
    planOfAction: string[];
  };
  trades: RoundTrip[];
  tradeDates: string[];
  empty: boolean;
  totalBadTradeCost: number;
  totalGoodTradeProfit: number;
  badTradeCounts: Record<string, { count: number; totalCost: number }>;
  goodTradeCounts: Record<string, { count: number; totalProfit: number }>;
  standardDemons: string[];
  standardGood: string[];
  enteredTooSoonCount: number;

  // NEW
  scripSummary: ScripSummaryRow[];
}

// ----------- CONSTANTS -----------
export const standardDemons = [
  "POOR RISK/REWARD TRADE", "HELD LOSS TOO LONG", "PREMATURE EXIT",
  "REVENGE TRADING", "OVERTRADING", "WRONG POSITION SIZE",
  "CHASED ENTRY", "MISSED STOP LOSS"
];

export const standardGood = [
  "GOOD RISK/REWARD", "PROPER ENTRY", "PROPER EXIT",
  "FOLLOWED PLAN", "STOP LOSS RESPECTED", "HELD FOR TARGET", "DISCIPLINED"
];

// ----------- UTIL -----------
const r2 = (n: number) => Math.round((n + Number.EPSILON) * 100) / 100;

// ----------- MULTER UPLOAD -----------
export const tradeJournalUpload = multer({ dest: "uploads/" });

// ----------- DATE NORMALIZER -----------
function normalizeTradeDate(dateStr: string | undefined | null): string {
  if (!dateStr) return "";
  if (/^\d{4}-\d{2}-\d{2}/.test(dateStr)) return dateStr.slice(0, 10);
  const mdy = dateStr.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (mdy) { const [_, m, d, y] = mdy; return `${y}-${m.padStart(2,"0")}-${d.padStart(2,"0")}`; }
  const dmy = dateStr.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (dmy) { const [_, d, m, y] = dmy; return `${y}-${m.padStart(2,"0")}-${d.padStart(2,"0")}`; }
  return dateStr.slice(0, 10);
}

// ----------- CSV PARSER -----------
function findTradeTableHeaderIndex(lines: string[]): number {
  const possibleHeaders = [
    "Scrip/Contract,Buy/Sell,Buy Price",
    "symbol,isin,trade_date",
    "Scrip Name,Trade Type,Trade Date"
  ];
  return lines.findIndex(line =>
    possibleHeaders.some(header =>
      line.replace(/\s/g, '').toLowerCase().startsWith(header.replace(/\s/g, '').toLowerCase())
    )
  );
}

export async function parseUniversalTradebook(filePath: string): Promise<Trade[]> {
  const raw = fs.readFileSync(filePath, "utf-8");
  const lines = raw.split(/\r?\n/);
  const headerIdx = findTradeTableHeaderIndex(lines);
  if (headerIdx === -1) throw new Error("No recognizable trade table found!");
  const tradeTable = lines.slice(headerIdx).join("\n");
  const tempCsv = filePath + ".parsed.csv";
  fs.writeFileSync(tempCsv, tradeTable);

  return new Promise((resolve, reject) => {
    const trades: Trade[] = [];
    fs.createReadStream(tempCsv)
      .pipe(csvParser())
      .on("data", (row: any) => {
        // Zerodha-like
        if (row["symbol"]) {
          let date = row["trade_date"] || "";
          let time = "";
          if (row["order_execution_time"]) {
            if (row["order_execution_time"].includes("T")) {
              const [d, t] = row["order_execution_time"].split("T");
              date = d; time = t;
            } else if (row["order_execution_time"].includes(" ")) {
              const [d, t] = row["order_execution_time"].split(" ");
              date = d; time = t;
            }
          } else if (row["trade_time"]) time = row["trade_time"];

          const qty = parseInt(row["quantity"]) || 0;
          trades.push({
            Date: normalizeTradeDate(date),
            Time: time,
            Symbol: row["symbol"],
            Direction: row["trade_type"]?.toLowerCase() === "buy" ? "Buy" : "Sell",
            Price: parseFloat(row["price"]) || 0,
            Quantity: qty,
            _fullQty: qty,
            PnL: 0,
            Charges: 0,
            NetPnL: 0,
          });
        }
        // Angel/Upstox/ICICI-like
        else if (row["Scrip/Contract"]) {
          const side = row["Buy/Sell"]?.toLowerCase();
          const buyRaw  = parseFloat(row["Buy Price"])  || 0;
          const sellRaw = parseFloat(row["Sell Price"]) || 0;
          const price = side === "buy" ? buyRaw : sellRaw;

          let date = row["Date"] || row["Trade Date"] || "";
          let time = row["Time"] || row["Trade Time"] || row["Order Time"] || row["TradeDateTime"] || "";
          if (typeof time === "string" && time.includes(" ")) {
            const [d, t] = time.split(" "); date = d; time = t;
          }
          if (typeof time === "string" && !/\d{2}:\d{2}/.test(time)) time = "";

          const exchangeTurnover =
            (parseFloat(row["Exchange Turnover Charges"]) || 0) ||
            (parseFloat(row["Exchange Turnover"]) || 0);

          const chargeFields = [
            "Brokerage","GST","STT","Sebi Tax","Stamp Duty","Other Charges","IPFT Charges"
          ];
          const baseCharges = chargeFields.reduce((s, k) => s + (parseFloat(row[k]) || 0), 0);
          const charges = baseCharges + (exchangeTurnover || 0);

          const qty = parseInt(row["Quantity"]) || 0;

          trades.push({
            Date: normalizeTradeDate(date),
            Time: time,
            Symbol: row["Scrip/Contract"],
            Direction: side === "buy" ? "Buy" : "Sell",
            Price: price,
            Quantity: qty,
            _fullQty: qty,
            PnL: 0,
            Charges: charges,
            NetPnL: 0,
            buyPriceRaw: buyRaw || undefined,
            sellPriceRaw: sellRaw || undefined,
          });
        }
      })
      .on("end", () => { fs.unlinkSync(tempCsv); resolve(trades); })
      .on("error", (err) => { fs.unlinkSync(tempCsv); reject(err); });
  });
}

// ----------- Helper: broker-like baseline from raw rows -----------
function computeBaselineNetFromRaw(trades: Trade[]): number | null {
  const hasRaw = trades.some(t => t.buyPriceRaw !== undefined || t.sellPriceRaw !== undefined);
  if (!hasRaw) return null;
  let grossSell = 0, grossBuy = 0, allCharges = 0;
  for (const t of trades) {
    const q = t.Quantity || 0;
    if (t.sellPriceRaw && q) grossSell += t.sellPriceRaw * q;
    if (t.buyPriceRaw  && q) grossBuy  += t.buyPriceRaw  * q;
    allCharges += (t.Charges || 0);
  }
  return r2(grossSell - grossBuy - allCharges);
}

// ----------- Helper: detect open positions -----------
function hasOpenPositions(trades: Trade[]): boolean {
  const net: Record<string, number> = {};
  for (const t of trades) {
    const q = t.Quantity || 0;
    const s = t.Symbol || "";
    if (!s || !q) continue;
    net[s] = (net[s] || 0) + (t.Direction === "Buy" ? q : -q);
  }
  return Object.values(net).some(v => Math.abs(v) > 0);
}

// ----------- ROUND TRIP PAIRING (with charge proration) -----------
function pairRoundTrips(trades: Trade[]): RoundTrip[] {
  const sorted = trades
    .filter(t => t.Symbol && t.Direction && t.Quantity && t.Price !== undefined)
    .sort((a, b) => new Date(`${a.Date}T${a.Time || "00:00"}`).getTime() - new Date(`${b.Date}T${b.Time || "00:00"}`).getTime());

  const roundTrips: RoundTrip[] = [];
  const openPositions: Record<string, Trade[]> = {};

  const prorate = (charges: number | undefined, usedQty: number, fullQty?: number) => {
    const c = charges || 0;
    const f = fullQty && fullQty > 0 ? fullQty : usedQty;
    if (f <= 0) return 0;
    return c * (usedQty / f);
  };

  for (const trade of sorted) {
    const symbol = trade.Symbol!;
    if (!openPositions[symbol]) openPositions[symbol] = [];
    const openLegs = openPositions[symbol];

    if (openLegs.length > 0 && trade.Direction !== openLegs[0].Direction) {
      let qtyToClose = trade.Quantity!;
      while (openLegs.length && qtyToClose > 0) {
        const entryLeg = openLegs[0];
        const closeQty = Math.min(qtyToClose, entryLeg.Quantity!);

        const entryLegUsed: Trade = {
          ...entryLeg,
          Quantity: closeQty,
          Charges: prorate(entryLeg.Charges, closeQty, entryLeg._fullQty),
          _fullQty: entryLeg._fullQty
        };

        if (entryLeg.Quantity! > closeQty) entryLeg.Quantity! -= closeQty;
        else openLegs.shift();

        const exitLeg: Trade = {
          ...trade,
          Quantity: closeQty,
          Charges: prorate(trade.Charges, closeQty, trade._fullQty),
          _fullQty: trade._fullQty
        };

        const gross =
          entryLegUsed.Direction === "Buy"
            ? (exitLeg.Price! - entryLegUsed.Price!) * closeQty
            : (entryLegUsed.Price! - exitLeg.Price!) * closeQty;

        const sliceCharges = (entryLegUsed.Charges || 0) + (exitLeg.Charges || 0);
        const pnl = gross - sliceCharges;

        const entryDT = new Date(`${entryLegUsed.Date}T${entryLegUsed.Time || "00:00"}`);
        const exitDT = new Date(`${exitLeg.Date}T${exitLeg.Time || "00:00"}`);
        const holdingMinutes = Math.round((exitDT.getTime() - entryDT.getTime()) / 60000);

        roundTrips.push({
          symbol,
          entry: entryLegUsed,
          exit: exitLeg,
          legs: [entryLegUsed, exitLeg],
          PnL: pnl,
          NetPnL: pnl,
          holdingMinutes,
        });

        qtyToClose -= closeQty;
      }
      if (qtyToClose > 0) {
        const remainder: Trade = {
          ...trade,
          Quantity: qtyToClose,
          _fullQty: trade._fullQty,
          Charges: prorate(trade.Charges, qtyToClose, trade._fullQty)
        };
        openPositions[symbol].push(remainder);
      }
    } else {
      openLegs.push({ ...trade, _fullQty: trade._fullQty ?? trade.Quantity });
    }
  }
  return roundTrips;
}

// ----------- RECONCILIATION -----------
function reconcileRoundTripsToBaseline(roundTrips: RoundTrip[], baselineNet: number | null) {
  if (baselineNet === null || roundTrips.length === 0) return;
  const sumPairs = roundTrips.reduce((s, r) => s + r.PnL, 0);
  const delta = r2(baselineNet - sumPairs);
  if (Math.abs(delta) < 0.01) return;

  let totalAbs = roundTrips.reduce((s, r) => s + Math.abs(r.PnL), 0);
  if (totalAbs === 0) totalAbs = roundTrips.length;

  let allocated = 0;
  for (let i = 0; i < roundTrips.length; i++) {
    const r = roundTrips[i];
    const weight = totalAbs === roundTrips.length ? 1 / roundTrips.length : Math.abs(r.PnL) / totalAbs;
    const adj = (i === roundTrips.length - 1) ? r2(delta - allocated) : r2(delta * weight);
    r.PnL = r2(r.PnL + adj);
    r.NetPnL = r.PnL;
    allocated = r2(allocated + adj);
  }
}

// ----------- ANALYSIS -----------
export function processTrades(trades: Trade[]): Stats {
  // params
  const minGoodRR = 1.2, maxRiskPercent = 2.0, overtradeLimit = 5;
  const earlyEntryCutoff = "09:20", revengeWindowMins = 15, SLTolerance = 1.3;

  // pair
  const roundTrips = pairRoundTrips(trades);

  // reconcile only if no open positions
  const baselineNet = hasOpenPositions(trades) ? null : computeBaselineNetFromRaw(trades);
  reconcileRoundTripsToBaseline(roundTrips, baselineNet);

  // aggregates
  let wins = 0, losses = 0, profitSum = 0, lossSum = 0;
  const pnlByDate: Record<string, number> = {};
  const tradeDates: string[] = [];
  const capital = 100000;
  let prevLossExitTime: Date | null = null;
  let prevLossDirection: "Buy" | "Sell" | null = null;

  for (const rt of roundTrips) {
    const d = rt.exit.Date;
    pnlByDate[d] = (pnlByDate[d] || 0) + rt.PnL;
    if (!tradeDates.includes(d)) tradeDates.push(d);
    if (rt.PnL > 0) { wins++; profitSum += rt.PnL; }
    else if (rt.PnL < 0) { losses++; lossSum += Math.abs(rt.PnL); }
  }

  const avgWin = wins ? profitSum / wins : 0;
  const avgLoss = losses ? lossSum / losses : 0;

  const badTagSummary: Record<string, { count: number, totalCost: number }> = {};
  const goodTagSummary: Record<string, { count: number, totalProfit: number }> = {};
  standardDemons.forEach(t => badTagSummary[t] = { count: 0, totalCost: 0 });
  standardGood.forEach(t => goodTagSummary[t] = { count: 0, totalProfit: 0 });

  let totalBadTradeCost = 0;
  let totalGoodTradeProfit = 0;
  let enteredTooSoonCount = 0;
  const dayOrdinal: Record<string, number> = {};

  for (const t of roundTrips) {
    const demons: string[] = [];
    const good: string[] = [];

    const day = t.exit.Date;
    dayOrdinal[day] = (dayOrdinal[day] || 0) + 1;
    const tradeNumberToday = dayOrdinal[day];

    if (t.PnL > 0 && t.entry.stopDistance && t.entry.stopDistance > 0) {
      const riskAmt = (t.entry.stopDistance) * (t.entry.Quantity || 1);
      const rr = riskAmt > 0 ? t.PnL / riskAmt : 0;
      if (rr < minGoodRR) demons.push("POOR RISK/REWARD TRADE");
    }
    if (t.PnL < 0 && t.holdingMinutes > 90) demons.push("HELD LOSS TOO LONG");
    if (t.PnL > 0 && t.holdingMinutes < 8 && t.PnL < avgWin * 0.8) demons.push("PREMATURE EXIT");
    if (t.PnL < 0 && Math.abs(t.PnL) > Math.abs(avgLoss * SLTolerance)) demons.push("MISSED STOP LOSS");

    if (t.entry.Time && t.entry.Time < earlyEntryCutoff) { demons.push("CHASED ENTRY"); enteredTooSoonCount++; }

    const riskAmtApprox = Math.abs((t.entry.Price - t.exit.Price) * (t.entry.Quantity || 1));
    if (riskAmtApprox > (capital * maxRiskPercent) / 100 || (avgLoss > 0 && riskAmtApprox > avgLoss * 2.5)) {
      demons.push("WRONG POSITION SIZE");
    }
    if (tradeNumberToday > overtradeLimit) demons.push("OVERTRADING");

    if (prevLossExitTime && t.entry.Time && t.PnL < 0 && prevLossDirection === t.entry.Direction) {
      const entryDT = new Date(`${t.entry.Date}T${t.entry.Time}`);
      const minsDiff = Math.round((entryDT.getTime() - prevLossExitTime.getTime()) / 60000);
      if (minsDiff >= 0 && minsDiff <= revengeWindowMins) demons.push("REVENGE TRADING");
    }
    if (t.PnL < 0) { prevLossExitTime = new Date(`${t.exit.Date}T${t.exit.Time || "00:00"}`); prevLossDirection = t.entry.Direction; }

    if (t.PnL > 0 && avgLoss > 0 && t.PnL >= Math.abs(avgLoss * 1.2)) good.push("GOOD RISK/REWARD");

    const notEarly = !t.entry.Time || t.entry.Time >= earlyEntryCutoff;
    const respectedSL = (t.PnL <= 0 && Math.abs(t.PnL) <= Math.abs(avgLoss * SLTolerance)) || t.PnL > 0;

    if (!demons.includes("CHASED ENTRY") && notEarly && respectedSL) good.push("PROPER ENTRY");
    if (!demons.includes("PREMATURE EXIT") && !demons.includes("MISSED STOP LOSS")) good.push("PROPER EXIT");
    if (t.PnL < 0 && Math.abs(t.PnL) <= Math.abs(avgLoss * SLTolerance)) good.push("STOP LOSS RESPECTED");
    if (t.PnL > 0 && t.holdingMinutes > 12 && t.PnL > Math.abs(avgLoss * 1.2)) good.push("HELD FOR TARGET");
    if (demons.length === 0 && good.length >= 2) good.push("DISCIPLINED");

    t.DemonArr = Array.from(new Set(demons));
    t.GoodPracticeArr = Array.from(new Set(good));
    t.Demon = t.DemonArr.join(", ");
    t.GoodPractice = t.GoodPracticeArr.join(", ");

    t.isBadTrade = t.DemonArr.length > 0 && t.PnL < 0;
    t.isGoodTrade = t.GoodPracticeArr.length >= 2 && t.DemonArr.length === 0 && (t.PnL > 0 || t.GoodPracticeArr.includes("STOP LOSS RESPECTED"));

    if (t.isBadTrade && t.PnL < 0) {
      totalBadTradeCost += Math.abs(t.PnL);
      const main = t.DemonArr[0]; if (main) { (badTagSummary[main] ||= { count: 0, totalCost: 0 }).count++; badTagSummary[main].totalCost += Math.abs(t.PnL); }
    }
    if (t.isGoodTrade && t.PnL > 0) {
      totalGoodTradeProfit += t.PnL;
      const main = t.GoodPracticeArr[0]; if (main) { (goodTagSummary[main] ||= { count: 0, totalProfit: 0 }).count++; goodTagSummary[main].totalProfit += t.PnL; }
    }
  }

  const totalTrades = roundTrips.length;
  const tradeWinPercent = totalTrades ? (wins / totalTrades) * 100 : 0;
  const profitFactor = lossSum === 0 ? (profitSum > 0 ? Infinity : 0) : profitSum / lossSum;
  const dayWinPercent = Object.keys(pnlByDate).length ? (Object.values(pnlByDate).filter(v => v > 0).length / Object.keys(pnlByDate).length * 100) : 0;

  const demonFinder = Object.entries(badTagSummary).sort((a,b) => b[1].count - a[1].count).slice(0,3).map(([d]) => d);
  const actionableByDemon: Record<string,string> = {
    "POOR RISK/REWARD TRADE":"Only take setups with ≥1.5R potential; predefine targets and partial exits.",
    "HELD LOSS TOO LONG":"Use hard SL and exit immediately when hit—no averaging down or hoping.",
    "PREMATURE EXIT":"Trail stops using structure; take partial at 1R and let the rest run.",
    "REVENGE TRADING":"After a loss, enforce a 15–30 min cooldown—skip the very next signal.",
    "OVERTRADING":"Cap to 5 trades/day; stop after 2 consecutive losses for the session.",
    "WRONG POSITION SIZE":"Risk ≤2% per trade; size via calculator using stop distance.",
    "CHASED ENTRY":"Avoid early entries; wait for retest/limit fill and skip first 5 minutes.",
    "MISSED STOP LOSS":"Place OCO protective stops with the entry and never cancel them."
  };
  const candidateActions: string[] = [];
  demonFinder.forEach(d => actionableByDemon[d] && candidateActions.push(actionableByDemon[d]));
  if (tradeWinPercent < 40) candidateActions.push("Trade only A+ setups for a week; skip marginal conditions.");
  if (!Number.isFinite(profitFactor) || profitFactor < 1) candidateActions.push("Tighten losses and let winners run using R-based take-profits.");
  if (enteredTooSoonCount > 0) candidateActions.push("No entries before 09:20—let structure form before engaging.");
  const planOfAction = Array.from(new Set(candidateActions)).slice(0, 3);
  const upholicScore = Math.min(100, (80 * 0.4) + (tradeWinPercent * 0.3) + (dayWinPercent * 0.3));

  // header Net P&L (reconciled)
  const netPnl = r2(roundTrips.reduce((s, r) => s + r.PnL, 0));

  // ===== DIRECTION-AWARE SCRIP SUMMARY (uses reconciled P&L for net, back-solves charges) =====
  type Agg = { qty: number; grossBuy: number; grossSell: number; reconNet: number; };
  const per = new Map<string, Agg>();

  for (const rt of roundTrips) {
    const sym = rt.symbol;
    const q = rt.entry.Quantity || 0; // per slice
    let grossBuy = 0, grossSell = 0;

    if (rt.entry.Direction === "Buy") {
      grossBuy  = (rt.entry.Price || 0) * q;
      grossSell = (rt.exit.Price  || 0) * q;
    } else {
      grossBuy  = (rt.exit.Price  || 0) * q;   // buy at exit
      grossSell = (rt.entry.Price || 0) * q;   // sell at entry
    }

    const a = per.get(sym) ?? { qty: 0, grossBuy: 0, grossSell: 0, reconNet: 0 };
    a.qty      += q;
    a.grossBuy += grossBuy;
    a.grossSell+= grossSell;
    a.reconNet += rt.PnL;   // <-- use reconciled slice P&L
    per.set(sym, a);
  }

  const scripSummary: ScripSummaryRow[] = Array.from(per.entries()).map(([symbol, a]) => {
    const avgBuy  = a.qty ? r2(a.grossBuy  / a.qty) : 0;
    const avgSell = a.qty ? r2(a.grossSell / a.qty) : 0;
    const charges = r2((a.grossSell - a.grossBuy) - a.reconNet); // back-solve so rows sum to header
    return {
      symbol,
      quantity: a.qty,
      avgBuy,
      avgSell,
      charges,
      netRealized: r2(a.reconNet),
    };
  }).sort((x, y) => y.netRealized - x.netRealized);

  return {
    netPnl,
    tradeWinPercent,
    profitFactor,
    dayWinPercent,
    avgWinLoss: { avgWin, avgLoss },
    upholicScore: Math.max(0, Math.round(upholicScore)),
    upholicPointers: { patience: 80, demonFinder, planOfAction },
    trades: roundTrips,
    tradeDates,
    empty: roundTrips.length === 0,
    totalBadTradeCost,
    totalGoodTradeProfit,
    badTradeCounts: badTagSummary,
    goodTradeCounts: goodTagSummary,
    standardDemons,
    standardGood,
    enteredTooSoonCount,

    // new table
    scripSummary,
  };
}
