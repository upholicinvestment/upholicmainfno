import { Express } from "express";
import { Db } from "mongodb";
import dayjs from "dayjs";

export default function cash_dataRoutes(app: Express, db: Db) {
 // Fetch FII/DII cash data

app.get("/api/fii-dii-data", async (req, res) => {

  try {

    const collection = db.collection("cash_data");

    const rows = await collection

      .find({}, {

        projection: {

          Date: 1,

          "FII Net Purchase/Sales": 1,

          "DII Net Purchase/Sales": 1,

          _id: 0

        }

      })

      .sort({ Date: 1 })

      .toArray();



    const formattedData = rows.map(row => {

      const [yy, mm, dd] = row.Date.split("-");

      return {

        date: row.Date,

        month: mm,

        year: parseInt(dd, 10),

        FII: parseFloat(row["FII Net Purchase/Sales"]),

        DII: parseFloat(row["DII Net Purchase/Sales"]),

      };

    });



    res.json(formattedData);

  } catch (err) {

    console.error("Error in /api/fii-dii-data:", err);

    res.status(500).json({ error: "Internal Server Error" });

  }

});
}
