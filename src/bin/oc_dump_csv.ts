import "dotenv/config";
import fs from "fs";
import path from "path";
import { getLiveOptionChain, toNormalizedArray } from "../services/option_chain";
import { istNowString } from "../utils/time";

(async () => {
  const id  = Number(process.env.OC_UNDERLYING_ID || 13);
  const seg = process.env.OC_SEGMENT || "IDX_I";
  const exp = process.env.OC_EXPIRY; // optional; nearest if unset

  const { expiry, data } = await getLiveOptionChain(id, seg, exp);
  const rows = toNormalizedArray(data.oc);

  const headers = [
    "strike",
    "ce_last_price","ce_implied_volatility","ce_oi","ce_prev_close","ce_prev_oi","ce_prev_vol",
    "ce_bid","ce_bid_qty","ce_ask","ce_ask_qty","ce_volume",
    "ce_delta","ce_theta","ce_gamma","ce_vega",
    "pe_last_price","pe_implied_volatility","pe_oi","pe_prev_close","pe_prev_oi","pe_prev_vol",
    "pe_bid","pe_bid_qty","pe_ask","pe_ask_qty","pe_volume",
    "pe_delta","pe_theta","pe_gamma","pe_vega"
  ];

  const csv = [
    headers.join(","),
    ...rows.map(r => [
      r.strike,
      r.ce?.last_price ?? "", r.ce?.implied_volatility ?? "", r.ce?.oi ?? "", r.ce?.previous_close_price ?? "", r.ce?.previous_oi ?? "", r.ce?.previous_volume ?? "",
      r.ce?.top_bid_price ?? "", r.ce?.top_bid_quantity ?? "", r.ce?.top_ask_price ?? "", r.ce?.top_ask_quantity ?? "", r.ce?.volume ?? "",
      r.ce?.greeks?.delta ?? "", r.ce?.greeks?.theta ?? "", r.ce?.greeks?.gamma ?? "", r.ce?.greeks?.vega ?? "",
      r.pe?.last_price ?? "", r.pe?.implied_volatility ?? "", r.pe?.oi ?? "", r.pe?.previous_close_price ?? "", r.pe?.previous_oi ?? "", r.pe?.previous_volume ?? "",
      r.pe?.top_bid_price ?? "", r.pe?.top_bid_quantity ?? "", r.pe?.top_ask_price ?? "", r.pe?.top_ask_quantity ?? "", r.pe?.volume ?? "",
      r.pe?.greeks?.delta ?? "", r.pe?.greeks?.theta ?? "", r.pe?.greeks?.gamma ?? "", r.pe?.greeks?.vega ?? ""
    ].join(","))
  ].join("\n");

  const sym = process.env.OC_SYMBOL || "UNDERLYING";
  const file = path.resolve(`./oc_${sym}_${expiry}.csv`);
  fs.writeFileSync(file, csv, "utf8");
  console.log(`[${istNowString()}] CSV saved: ${file}`);
})();
