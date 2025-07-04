import { Express,Request, Response } from "express";
import { Db } from "mongodb";
import dayjs from "dayjs";

export default function summaryRoutes(app: Express, db: Db) {
 // Fetch FII/DII cash data

app.get("/summary", (req: Request, res: Response): void => {
  const selectedDate = req.query.date;

  if (!selectedDate) {
    res.status(400).json({ error: 'Missing "date" query parameter' });
    return;
  }

  const nseColl = db.collection("nse");

  // Fetch all rows for the selected date (excluding "TOTAL")
  nseColl
    .find({ Date: selectedDate, "Client Type": { $ne: "TOTAL" } })
    .toArray()
    .then(rows => {
      if (rows.length === 0) {
        res.json({ available: false });
        return;
      }

      const currentNetOI: Record<
        string,
        { "Index Futures": number; "Stock Futures": number; "Index Options": number }
      > = {};

      rows.forEach(row => {
        const participant = row["Client Type"];
        if (!currentNetOI[participant]) {
          currentNetOI[participant] = {
            "Index Futures": 0,
            "Stock Futures": 0,
            "Index Options": 0,
          };
        }
        currentNetOI[participant]["Index Futures"] =
          (Number(row["Future Index Long"]) || 0) - (Number(row["Future Index Short"]) || 0);

        currentNetOI[participant]["Stock Futures"] =
          (Number(row["Future Stock Long"]) || 0) - (Number(row["Future Stock Short"]) || 0);

        currentNetOI[participant]["Index Options"] =
          ((Number(row["Option Index Call Long"]) || 0) - (Number(row["Option Index Call Short"]) || 0)) -
          ((Number(row["Option Index Put Long"]) || 0) - (Number(row["Option Index Put Short"]) || 0));
      });

      let prevNetOI: Record<
        string,
        { "Index Futures": number; "Stock Futures": number; "Index Options": number }
      > = {};

      let previousDate: string | undefined;

      const manualPrevData = {
        "FII": { "Index Futures": -86592, "Stock Futures": 1529000, "Index Options": -73818 },
        "Pro": { "Index Futures": -29643, "Stock Futures": 400000, "Index Options": -66112 },
        "Client": { "Index Futures": 40514, "Stock Futures": 1882000, "Index Options": 213000 },
        "DII": { "Index Futures": 75721, "Stock Futures": -3810000, "Index Options": -73116 }
      };

      if (selectedDate === "2025-04-07") {
        prevNetOI = manualPrevData;
        finishResponse();
      } else {
        nseColl
          .find({ Date: { $lt: selectedDate }, "Client Type": { $ne: "TOTAL" } }, { projection: { Date: 1, _id: 0 } })
          .sort({ Date: -1 })
          .limit(1)
          .toArray()
          .then(prevDocArr => {
            previousDate = prevDocArr[0]?.Date || null;
            if (previousDate) {
              nseColl
                .find({ Date: previousDate, "Client Type": { $ne: "TOTAL" } })
                .toArray()
                .then(prevRows => {
                  prevRows.forEach(row => {
                    const participant = row["Client Type"];
                    if (!prevNetOI[participant]) {
                      prevNetOI[participant] = {
                        "Index Futures": 0,
                        "Stock Futures": 0,
                        "Index Options": 0,
                      };
                    }
                    prevNetOI[participant]["Index Futures"] =
                      (Number(row["Future Index Long"]) || 0) - (Number(row["Future Index Short"]) || 0);

                    prevNetOI[participant]["Stock Futures"] =
                      (Number(row["Future Stock Long"]) || 0) - (Number(row["Future Stock Short"]) || 0);

                    prevNetOI[participant]["Index Options"] =
                      ((Number(row["Option Index Call Long"]) || 0) - (Number(row["Option Index Call Short"]) || 0)) -
                      ((Number(row["Option Index Put Long"]) || 0) - (Number(row["Option Index Put Short"]) || 0));
                  });
                  finishResponse();
                })
                .catch(err => {
                  console.error("Error fetching previous rows:", err);
                  res.status(500).json({ error: "Internal Server Error" });
                });
            } else {
              // No previous date found, treat as zeros
              finishResponse();
            }
          })
          .catch(err => {
            console.error("Error fetching previous date:", err);
            res.status(500).json({ error: "Internal Server Error" });
          });
      }

      // Final response builder function
      function finishResponse() {
        const response = Object.entries(currentNetOI).map(([participant, segments]) => {
          const rows = Object.entries(segments).map(([segment, netOI]) => {
            const prev = prevNetOI[participant]?.[
              segment as "Index Futures" | "Stock Futures" | "Index Options"
            ] || 0;
            const change = netOI - prev;
            return { segment, netOI, change };
          });
          return { participant, rows };
        });
        res.json({ available: true, data: response });
      }
    })
    .catch(err => {
      console.error("Error in /summary:", err);
      res.status(500).json({ error: "Internal Server Error" });
    });
});

app.get("/available-dates", async (req, res) => {

  try {

    const nseColl = db.collection("nse");

    const dates = await nseColl.distinct("Date", { "Client Type": { $ne: "TOTAL" } });

    dates.sort(); // Sort lexicographically (works for "YYYY-MM-DD")

    res.json(dates);

  } catch (err) {

    console.error("Error in /available-dates:", err);

    res.status(500).json({ error: "Internal Server Error" });

  }

});
}
