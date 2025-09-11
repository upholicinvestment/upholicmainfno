// src/routes/stocks.routes.ts
import { Express, Request, Response } from 'express';
import { Db } from 'mongodb';

export function Stocks(app: Express, db: Db) {
  app.get("/api/stocks", async (_req: Request, res: Response) => {
    try {
      const securityIds = [
        3506, 5258, 21808, 1964, 10999, 4306, 20374, 910, 317, 10604
      ];

      const stocks = await db
        .collection("nse_fno_stock")
        .aggregate([
          {
            $match: {
              security_id: { $in: securityIds },
              type: "Full Data",
            },
          },
          { $sort: { received_at: -1 } },
          {
            $group: {
              _id: "$security_id",
              doc: { $first: "$$ROOT" },
            },
          },
          { $replaceRoot: { newRoot: "$doc" } },
          {
            $project: {
              _id: 0,
              security_id: 1,
              LTP: { $toString: "$LTP" },
              volume: 1,
              open: { $toString: "$open" },
              close: { $toString: "$close" },
              received_at: 1,
            },
          },
        ])
        .toArray();

      res.json(stocks);
    } catch (err) {
      console.error("Error fetching stocks:", err);
      res.status(500).json({ error: "Failed to fetch stocks" });
    }
  });
}







    // app.get("/api/stocks", async (_req, res) => {
    //   try {
    //     const securityIds = [
    //       53454, 53435, 53260, 53321, 53461, 53359, 53302, 53224, 53405, 53343,
    //       53379, 53251, 53469, 53375, 53311, 53314, 53429, 53252, 53460, 53383,
    //       53354, 53450, 53466, 53317, 53301, 53480, 53226, 53432, 53352, 53433,
    //       53241, 53300, 53327, 53258, 53253, 53471, 53398, 53441, 53425, 53369,
    //       53341, 53250, 53395, 53324, 53316, 53318, 53279, 53245, 53280, 53452,
    //     ]; // Only BEL for now

    //     const stocks = await db
    //       .collection("nse_fno_stock")
    //       .aggregate([
    //         {
    //           $match: {
    //             security_id: { $in: securityIds },
    //             type: "Full Data", // Only include Full Data entries
    //           },
    //         },
    //         { $sort: { received_at: -1 } }, // Sort by most recent first
    //         {
    //           $group: {
    //             _id: "$security_id",
    //             doc: { $first: "$$ROOT" }, // Get the most recent document for each security
    //           },
    //         },
    //         { $replaceRoot: { newRoot: "$doc" } },
    //         {
    //           $project: {
    //             _id: 0,
    //             security_id: 1,
    //             LTP: { $toString: "$LTP" },
    //             volume: 1,
    //             open: { $toString: "$open" },
    //             close: { $toString: "$close" },
    //             received_at: 1,
    //           },
    //         },
    //       ])
    //       .toArray();

    //     res.json(stocks);
    //   } catch (err) {
    //     console.error("Error fetching stocks:", err);
    //     res.status(500).json({ error: "Failed to fetch stocks" });
    //   }
    // });
