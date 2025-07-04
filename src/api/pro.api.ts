import { Express } from "express";
import { Db } from "mongodb";
import dayjs from "dayjs";

export default function ProRoutes(app: Express, db: Db) {
  // Fetch Pro Option Index change & Nifty value

  app.get("/api/Pro_Index_Opt/data", async (req, res) => {
    try {
      const nseColl = db.collection("nse");

      const niftyColl = db.collection("Nifty");

      const rows = await nseColl

        .find(
          { "Client Type": "Pro" },
          {
            projection: {
              Date: 1,

              "Option Index Call Long": 1,

              "Option Index Call Short": 1,

              "Option Index Put Long": 1,

              "Option Index Put Short": 1,

              _id: 0,
            },
          }
        )

        .sort({ Date: 1 })

        .toArray();

      const niftyRows = await niftyColl

        .find({}, { projection: { Date: 1, Close: 1, _id: 0 } })

        .toArray();

      const niftyMap: Record<string, number> = {};

      niftyRows.forEach((nifty) => {
        const norm = dayjs(nifty.Date, "DD-MMM-YYYY").format("YYYY-MM-DD");

        niftyMap[norm] = Number(nifty.Close);
      });

      const result = rows.map((row) => ({
        Date: row.Date,

        Pro_Call_Change:
          (row["Option Index Call Long"] || 0) -
          (row["Option Index Call Short"] || 0),

        Pro_Put_Change:
          (row["Option Index Put Long"] || 0) -
          (row["Option Index Put Short"] || 0),

        NIFTY_Value: niftyMap[row.Date] ?? null,
      }));

      res.json(result);
    } catch (err) {
      console.error("Error in /api/Pro_Index_Opt/data:", err);

      res.status(500).json({ error: "Internal Server Error" });
    }
  });

  // Fetch Pro Index Futures change & Nifty value (with manualPrevNetOI)

  app.get("/api/Pro_Index_Fut/data", async (req, res) => {
    try {
      const nseColl = db.collection("nse");

      const niftyColl = db.collection("Nifty");

      const rows = await nseColl

        .find(
          { "Client Type": "Pro" },
          {
            projection: {
              Date: 1,

              "Future Index Long": 1,

              "Future Index Short": 1,

              _id: 0,
            },
          }
        )

        .sort({ Date: 1 })

        .toArray();

      const niftyRows = await niftyColl

        .find({}, { projection: { Date: 1, Close: 1, _id: 0 } })

        .toArray();

      const niftyMap: Record<string, number> = {};

      niftyRows.forEach((nifty) => {
        const norm = dayjs(nifty.Date, "DD-MMM-YYYY").format("YYYY-MM-DD");

        niftyMap[norm] = Number(nifty.Close);
      });

      const manualPrevNetOI = -29643;

      let prevNetOI = manualPrevNetOI;

      const resultWithChange = rows.map((row) => {
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

          Pro_Index_Futures: change,

          NIFTY_Value: niftyMap[dateStr] ?? null,
        };
      });

      res.json(resultWithChange);
    } catch (err) {
      console.error("Error in /api/Pro_Index_Fut/data:", err);

      res.status(500).json({ error: "Internal Server Error" });
    }
  });

  // Fetch Pro Option Index OI & Nifty value

  app.get("/api/OIPro_Index_Opt/data", async (req, res) => {
    try {
      const nseColl = db.collection("nse");

      const niftyColl = db.collection("Nifty");

      const rows = await nseColl

        .find(
          { "Client Type": "Pro" },
          {
            projection: {
              Date: 1,

              "Option Index Call Long": 1,

              "Option Index Call Short": 1,

              "Option Index Put Long": 1,

              "Option Index Put Short": 1,

              _id: 0,
            },
          }
        )

        .sort({ Date: 1 })

        .toArray();

      const niftyRows = await niftyColl

        .find({}, { projection: { Date: 1, Close: 1, _id: 0 } })

        .toArray();

      const niftyMap: Record<string, number> = {};

      niftyRows.forEach((nifty) => {
        const norm = dayjs(nifty.Date, "DD-MMM-YYYY").format("YYYY-MM-DD");

        niftyMap[norm] = Number(nifty.Close);
      });

      const result = rows.map((row) => ({
        Date: row.Date,

        Pro_Call_OI:
          (row["Option Index Call Long"] || 0) -
          (row["Option Index Call Short"] || 0),

        Pro_Put_OI:
          (row["Option Index Put Long"] || 0) -
          (row["Option Index Put Short"] || 0),

        NIFTY_Value: niftyMap[row.Date] ?? null,
      }));

      res.json(result);
    } catch (err) {
      console.error("Error in /api/OIPro_Index_Opt/data:", err);

      res.status(500).json({ error: "Internal Server Error" });
    }
  });

  // Fetch Pro Futures OI & Nifty value

  app.get("/api/OIPro_Index_Fut/data", async (req, res) => {
    try {
      const nseColl = db.collection("nse");

      const niftyColl = db.collection("Nifty");

      const rows = await nseColl

        .find(
          { "Client Type": "Pro" },
          {
            projection: {
              Date: 1,

              "Future Index Long": 1,

              "Future Index Short": 1,

              _id: 0,
            },
          }
        )

        .sort({ Date: 1 })

        .toArray();

      const niftyRows = await niftyColl

        .find({}, { projection: { Date: 1, Close: 1, _id: 0 } })

        .toArray();

      const niftyMap: Record<string, number> = {};

      niftyRows.forEach((nifty) => {
        const norm = dayjs(nifty.Date, "DD-MMM-YYYY").format("YYYY-MM-DD");

        niftyMap[norm] = Number(nifty.Close);
      });

      const result = rows.map((row) => ({
        Date: row.Date,

        Pro_Futures_OI:
          (row["Future Index Long"] || 0) - (row["Future Index Short"] || 0),

        NIFTY_Value: niftyMap[row.Date] ?? null,
      }));

      res.json(result);
    } catch (err) {
      console.error("Error in /api/OIPro_Index_Fut/data:", err);

      res.status(500).json({ error: "Internal Server Error" });
    }
  });
}
