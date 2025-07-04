import { Express,Request, Response } from "express";
import { Db } from "mongodb";
import dayjs from "dayjs";

export default function AnalysisRoutes(app: Express, db: Db) {
 // Fetch FII/DII cash data
function normalizeDate(dateString: string, source: 'nse' | 'cash' | 'nifty'): string | null {
  if (!dateString) return null;

  try {
    // Initialize with default values
    let day = '';
    let month = '';
    let year = '';

    if (source === "nse") {
      // "YYYY-MM-DD" → "DD-MM-YYYY"
      const parts = dateString.split("-");
      if (parts.length !== 3) return null;
      [year, month, day] = parts;
    } else if (source === "cash") {
      // "17-Mar-25" → "DD-MM-YYYY"
      const parts = dateString.trim().split("-");
      if (parts.length !== 3) return null;

      const [d, mStr, yShort] = parts;
      const months: Record<string, string> = {
        Jan: "01", Feb: "02", Mar: "03", Apr: "04", May: "05", Jun: "06",
        Jul: "07", Aug: "08", Sep: "09", Oct: "10", Nov: "11", Dec: "12"
      };

      day = d.padStart(2, "0");
      month = months[mStr] || ''; // Provide fallback for invalid month
      year = yShort.length === 2 ? `20${yShort}` : yShort;
    } else if (source === "nifty") {
      // "02-APR-2025" → "DD-MM-YYYY"
      const parts = dateString.trim().split("-");
      if (parts.length !== 3) return null;

      const [d, mStr, y] = parts;
      const months: Record<string, string> = {
        JAN: "01", FEB: "02", MAR: "03", APR: "04", MAY: "05", JUN: "06",
        JUL: "07", AUG: "08", SEP: "09", OCT: "10", NOV: "11", DEC: "12"
      };

      day = d.padStart(2, "0");
      month = months[mStr.toUpperCase()] || ''; // Provide fallback for invalid month
      year = y;
    }

    // Validate all components were set
    if (!day || !month || !year) {
      return null;
    }

    return `${day}-${month}-${year}`;
  } catch (err) {
    console.error(`Failed to normalize ${source} date:`, dateString);
    return null;
  }
}

app.get("/api/net-oi", async (req, res) => {

  try {

    const nseColl = db.collection("nse");

    const niftyColl = db.collection("Nifty");

    const cashColl = db.collection("cash_data_nse");



    // Fetch all FII & Client rows

    const nseData = await nseColl

      .find({ "Client Type": { $in: ["FII", "Client"] } })

      .toArray();



    // Fetch all Nifty rows

    const niftyData = await niftyColl.find({}).toArray();



    // Fetch all cash data for FII/FPI * and DII **

    const cashData = await cashColl

      .find({ CATEGORY: { $in: ["FII/FPI *", "DII **"] } })

      .sort({ DATE: 1 }) // optional

      .toArray();



    // const netOIMap = {};


    const netOIMap: Record<
      string,
      {
        date: string;
        FII_Index_Futures: number;
        FII_Index_Options: number;
        Client_Index_Futures: number;
        Client_Index_Options: number;
        Nifty_Close: number;
        FII_Cash_Net: number;
        DII_Cash_Net: number;
      }
    > = {};




    // Process NSE data (FII & Client)

    nseData.forEach(row => {

      // const date = normalizeDate(row["Date"], "nse");

      const raw = normalizeDate(row["Date"], "nse");
      if (!raw) return; // or `continue` if you’re in a `for`‌loop
      const date = raw;


      const clientType = row["Client Type"];



      if (!netOIMap[date]) {

        netOIMap[date] = {

          date,

          FII_Index_Futures: 0,

          FII_Index_Options: 0,

          Client_Index_Futures: 0,

          Client_Index_Options: 0,

          Nifty_Close: 0,

          FII_Cash_Net: 0,

          DII_Cash_Net: 0

        };

      }



      const indexFutures =

        (row["Future Index Long"] || 0) - (row["Future Index Short"] || 0);

      const indexOptions =

        ((row["Option Index Call Long"] || 0) - (row["Option Index Call Short"] || 0)) -

        ((row["Option Index Put Long"] || 0) - (row["Option Index Put Short"] || 0));



      if (clientType === "FII") {

        netOIMap[date].FII_Index_Futures = indexFutures;

        netOIMap[date].FII_Index_Options = indexOptions;

      } else if (clientType === "Client") {

        netOIMap[date].Client_Index_Futures = indexFutures;

        netOIMap[date].Client_Index_Options = indexOptions;

      }

    });



    // Process Nifty data

    niftyData.forEach(row => {
      const raw = normalizeDate(row["Date"], "nifty");
      if (!raw) return;
      const date = raw;
      if (netOIMap[date]) {
        netOIMap[date].Nifty_Close = Number(row["Close"] || 0);
      }
    });



    cashData.forEach(row => {
      const raw = normalizeDate(row["DATE"], "cash");
      if (!raw) return;
      const date = raw;
      if (netOIMap[date]) {
        if (row["CATEGORY"] === "FII/FPI *") {
          netOIMap[date].FII_Cash_Net = Number(row["NET VALUE"] || 0);
        } else if (row["CATEGORY"] === "DII **") {
          netOIMap[date].DII_Cash_Net = Number(row["NET VALUE"] || 0);
        }
      }
    });


    // Convert map to sorted array by actual date

    const mergedData = Object.values(netOIMap).sort((a, b) => {

      return (

        // dayjs(a.date, "DD-MM-YYYY").toDate() - dayjs(b.date, "DD-MM-YYYY").toDate()

        dayjs(a.date, "DD-MM-YYYY").toDate().getTime() - dayjs(b.date, "DD-MM-YYYY").toDate().getTime()

      );

    });



    res.json(mergedData);

  } catch (err) {

    console.error("Error in /api/net-oi:", err);

    res.status(500).send("Internal Server Error");

  }

});

}
