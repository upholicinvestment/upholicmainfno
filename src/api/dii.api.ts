import { Express } from "express";
import { Db } from "mongodb";
import dayjs from "dayjs";

export default function DIIRoutes(app: Express, db: Db) {
 // Fetch DII Option Index change & Nifty value
 
 app.get("/api/DII_Index_Opt/data", async (req, res) => {
 
   try {
 
     const nseColl = db.collection("nse");
 
     const niftyColl = db.collection("Nifty");
 
 
 
     const rows = await nseColl
 
       .find({ "Client Type": "DII" }, {
 
         projection: {
 
           Date: 1,
 
           "Option Index Call Long": 1,
 
           "Option Index Call Short": 1,
 
           "Option Index Put Long": 1,
 
           "Option Index Put Short": 1,
 
           _id: 0
 
         }
 
       })
 
       .sort({ Date: 1 })
 
       .toArray();
 
 
 
     const niftyRows = await niftyColl
 
       .find({}, { projection: { Date: 1, Close: 1, _id: 0 } })
 
       .toArray();
 
 
 
     const niftyMap: Record<string, number> = {};
 
 
     niftyRows.forEach(nifty => {
 
       const norm = dayjs(nifty.Date, "DD-MMM-YYYY").format("YYYY-MM-DD");
 
       niftyMap[norm] = Number(nifty.Close);
 
     });
 
 
 
     const result = rows.map(row => ({
 
       Date: row.Date,
 
       DII_Call_Change:
 
         (row["Option Index Call Long"] || 0) - (row["Option Index Call Short"] || 0),
 
       DII_Put_Change:
 
         (row["Option Index Put Long"] || 0) - (row["Option Index Put Short"] || 0),
 
       NIFTY_Value: niftyMap[row.Date] ?? null,
 
     }));
 
 
 
     res.json(result);
 
   } catch (err) {
 
     console.error("Error in /api/DII_Index_Opt/data:", err);
 
     res.status(500).json({ error: "Internal Server Error" });
 
   }
 
 });
 
 
 
 // Fetch DII Index Futures change & Nifty value (with manualPrevNetOI)
 
 app.get("/api/DII_Index_Fut/data", async (req, res) => {
 
   try {
 
     const nseColl = db.collection("nse");
 
     const niftyColl = db.collection("Nifty");
 
 
 
     const rows = await nseColl
 
       .find({ "Client Type": "DII" }, {
 
         projection: {
 
           Date: 1,
 
           "Future Index Long": 1,
 
           "Future Index Short": 1,
 
           _id: 0
 
         }
 
       })
 
       .sort({ Date: 1 })
 
       .toArray();
 
 
 
     const niftyRows = await niftyColl
 
       .find({}, { projection: { Date: 1, Close: 1, _id: 0 } })
 
       .toArray();
 
 
 
     const niftyMap: Record<string, number> = {};
 
 
     niftyRows.forEach(nifty => {
 
       const norm = dayjs(nifty.Date, "DD-MMM-YYYY").format("YYYY-MM-DD");
 
       niftyMap[norm] = Number(nifty.Close);
 
     });
 
 
 
     const manualPrevNetOI = 75721;
 
     let prevNetOI = manualPrevNetOI;
 
 
 
     const resultWithChange = rows.map(row => {
 
       const dateStr = row.Date;
 
       const currentNetOI =
 
         (row["Future Index Long"] || 0) - (row["Future Index Short"] || 0);
 
 
 
       const change =
 
         dateStr === "2025-04-07"
 
           ? currentNetOI - manualPrevNetOI
 
           : currentNetOI - prevNetOI;
 
 
 
       prevNetOI = currentNetOI;
 
 
 
       return {
 
         Date: dateStr,
 
         DII_Index_Futures: change,
 
         NIFTY_Value: niftyMap[dateStr] ?? null,
 
       };
 
     });
 
 
 
     res.json(resultWithChange);
 
   } catch (err) {
 
     console.error("Error in /api/DII_Index_Fut/data:", err);
 
     res.status(500).json({ error: "Internal Server Error" });
 
   }
 
 });
 
 
 
 // Fetch DII Option Index OI & Nifty value
 
 app.get("/api/OIDII_Index_Opt/data", async (req, res) => {
 
   try {
 
     const nseColl = db.collection("nse");
 
     const niftyColl = db.collection("Nifty");
 
 
 
     const rows = await nseColl
 
       .find({ "Client Type": "DII" }, {
 
         projection: {
 
           Date: 1,
 
           "Option Index Call Long": 1,
 
           "Option Index Call Short": 1,
 
           "Option Index Put Long": 1,
 
           "Option Index Put Short": 1,
 
           _id: 0
 
         }
 
       })
 
       .sort({ Date: 1 })
 
       .toArray();
 
 
 
     const niftyRows = await niftyColl
 
       .find({}, { projection: { Date: 1, Close: 1, _id: 0 } })
 
       .toArray();
 
 
 
     const niftyMap: Record<string, number> = {};
 
 
     niftyRows.forEach(nifty => {
 
       const norm = dayjs(nifty.Date, "DD-MMM-YYYY").format("YYYY-MM-DD");
 
       niftyMap[norm] = Number(nifty.Close);
 
     });
 
 
 
     const result = rows.map(row => ({
 
       Date: row.Date,
 
       DII_Call_OI:
 
         (row["Option Index Call Long"] || 0) - (row["Option Index Call Short"] || 0),
 
       DII_Put_OI:
 
         (row["Option Index Put Long"] || 0) - (row["Option Index Put Short"] || 0),
 
       NIFTY_Value: niftyMap[row.Date] ?? null,
 
     }));
 
 
 
     res.json(result);
 
   } catch (err) {
 
     console.error("Error in /api/OIDII_Index_Opt/data:", err);
 
     res.status(500).json({ error: "Internal Server Error" });
 
   }
 
 });
 
 
 
 // Fetch DII Futures OI & Nifty value
 
 app.get("/api/OIDII_Index_Fut/data", async (req, res) => {
 
   try {
 
     const nseColl = db.collection("nse");
 
     const niftyColl = db.collection("Nifty");
 
 
 
     const rows = await nseColl
 
       .find({ "Client Type": "DII" }, {
 
         projection: {
 
           Date: 1,
 
           "Future Index Long": 1,
 
           "Future Index Short": 1,
 
           _id: 0
 
         }
 
       })
 
       .sort({ Date: 1 })
 
       .toArray();
 
 
 
     const niftyRows = await niftyColl
 
       .find({}, { projection: { Date: 1, Close: 1, _id: 0 } })
 
       .toArray();
 
 
 
     const niftyMap: Record<string, number> = {};
 
 
     niftyRows.forEach(nifty => {
 
       const norm = dayjs(nifty.Date, "DD-MMM-YYYY").format("YYYY-MM-DD");
 
       niftyMap[norm] = Number(nifty.Close);
 
     });
 
 
 
     const result = rows.map(row => ({
 
       Date: row.Date,
 
       DII_Futures_OI:
 
         (row["Future Index Long"] || 0) - (row["Future Index Short"] || 0),
 
       NIFTY_Value: niftyMap[row.Date] ?? null,
 
     }));
 
 
 
     res.json(result);
 
   } catch (err) {
 
     console.error("Error in /api/OIDII_Index_Fut/data:", err);
 
     res.status(500).json({ error: "Internal Server Error" });
 
   }
 
 });
 
 
}
