import { Express } from "express";
import { Db } from "mongodb";
import dayjs from "dayjs";

export default function FIIRoutes(app: Express, db: Db) {

// Fetch FII Option Index Call/Put change & Nifty value

app.get("/api/data", async (req, res) => {

  try {

    const nseColl = db.collection("nse");

    const niftyColl = db.collection("Nifty");



    // Fetch all FII rows from NSE

    const nseRows = await nseColl

      .find({ "Client Type": "FII" }, {

        projection: {

          Date: 1,

          "Option Index Call Long": 1,

          "Option Index Call Short": 1,

          "Option Index Put Long": 1,

          "Option Index Put Short": 1,

          _id: 0

        }

      })

      .toArray();



    // Fetch all Nifty rows

    const niftyRows = await niftyColl

      .find({}, { projection: { Date: 1, Close: 1, _id: 0 } })

      .toArray();



    // Build a map from normalized Nifty date → Close

    const niftyMap: Record<string, number> = {};


    niftyRows.forEach(nifty => {

      const norm = dayjs(nifty.Date, "DD-MMM-YYYY").format("YYYY-MM-DD");

      niftyMap[norm] = Number(nifty.Close);

    });



    // Compute FII Call/Put changes and merge Nifty values

    const result = nseRows.map(row => ({

      Date: row.Date,

      FII_Call_Change:

        (row["Option Index Call Long"] || 0) - (row["Option Index Call Short"] || 0),

      FII_Put_Change:

        (row["Option Index Put Long"] || 0) - (row["Option Index Put Short"] || 0),

      NIFTY_Value: niftyMap[row.Date] ?? null,

    }));



    // Sort by Date ascending

    result.sort((a, b) => (a.Date < b.Date ? -1 : a.Date > b.Date ? 1 : 0));



    res.json(result);

  } catch (err) {

    console.error("Error in /api/data:", err);

    res.status(500).json({ error: "Internal Server Error" });

  }

});



// Fetch FII Index Futures change & Nifty value (with manualPrevNetOI for 2025-04-07)

app.get("/api/FII_Index_Fut/data", async (req, res) => {

  try {

    const nseColl = db.collection("nse");

    const niftyColl = db.collection("Nifty");



    // Fetch all FII rows from NSE

    const rows = await nseColl

      .find({ "Client Type": "FII" }, {

        projection: {

          Date: 1,

          "Future Index Long": 1,

          "Future Index Short": 1,

          _id: 0

        }

      })

      .sort({ Date: 1 })

      .toArray();



    // Fetch all Nifty rows for mapping

    const niftyRows = await niftyColl

      .find({}, { projection: { Date: 1, Close: 1, _id: 0 } })

      .toArray();



    const niftyMap: Record<string, number> = {};


    niftyRows.forEach(nifty => {

      const norm = dayjs(nifty.Date, "DD-MMM-YYYY").format("YYYY-MM-DD");

      niftyMap[norm] = Number(nifty.Close);

    });



    const manualPrevNetOI = -86592;

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

        FII_Index_Futures: change,

        NIFTY_Value: niftyMap[dateStr] ?? null,

      };

    });



    res.json(resultWithChange);

  } catch (err) {

    console.error("Error in /api/FII_Index_Fut/data:", err);

    res.status(500).json({ error: "Internal Server Error" });

  }

});



// Fetch FII Option Index OI & Nifty value

app.get("/api/OIFII_Index_Opt/data", async (req, res) => {

  try {

    const nseColl = db.collection("nse");

    const niftyColl = db.collection("Nifty");



    const rows = await nseColl

      .find({ "Client Type": "FII" }, {

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

      FII_Call_OI:

        (row["Option Index Call Long"] || 0) - (row["Option Index Call Short"] || 0),

      FII_Put_OI:

        (row["Option Index Put Long"] || 0) - (row["Option Index Put Short"] || 0),

      NIFTY_Value: niftyMap[row.Date] ?? null,

    }));



    res.json(result);

  } catch (err) {

    console.error("Error in /api/OIFII_Index_Opt/data:", err);

    res.status(500).json({ error: "Internal Server Error" });

  }

});



// Fetch FII Futures OI & Nifty value

app.get("/api/OIFII_Index_Fut/data", async (req, res) => {

  try {

    const nseColl = db.collection("nse");

    const niftyColl = db.collection("Nifty");



    const rows = await nseColl

      .find({ "Client Type": "FII" }, {

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

      FII_Futures_OI:

        (row["Future Index Long"] || 0) - (row["Future Index Short"] || 0),

      NIFTY_Value: niftyMap[row.Date] ?? null,

    }));



    res.json(result);

  } catch (err) {

    console.error("Error in /api/OIFII_Index_Fut/data:", err);

    res.status(500).json({ error: "Internal Server Error" });

  }

});
 
 
}
