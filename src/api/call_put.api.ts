import { Express } from 'express';
import { Db } from 'mongodb';

export default function registerNiftyRoutes(app: Express, db: Db) {
  app.get('/api/nifty/atm-strikes-timeline', async (req, res) => {
    try {
      const intervalParam = req.query.interval as string || '3';
      const interval = parseInt(intervalParam, 10); // in minutes
  
      const collection = db.collection('all_nse_fno');
  
      // Step 1: Try to fetch today’s data
      const now = new Date();
      const todayStart = new Date(now.setUTCHours(0, 0, 0, 0));
      const todayEnd = new Date(todayStart);
      todayEnd.setUTCDate(todayEnd.getUTCDate() + 1);
  
      let dataStart = todayStart;
      let dataEnd = todayEnd;
  
      let docs = await collection.find({
        security_id: 56785,
        timestamp: { $gte: todayStart, $lt: todayEnd }
      }, { projection: { _id: 0, LTP: 1, timestamp: 1 } })
        .sort({ timestamp: 1 })
        .toArray();
  
      // Step 2: If today is empty, fallback to latest available data date
      if (docs.length === 0) {
        const lastEntry = await collection.find({ security_id: 56785 })
          .sort({ timestamp: -1 })
          .limit(1)
          .toArray();
  
        if (!lastEntry.length) {
          res.status(404).json({ error: 'No NIFTY data found in collection' });
          return;
        }
  
        const lastDate = new Date(lastEntry[0].timestamp);
        lastDate.setUTCHours(0, 0, 0, 0);
        const lastEnd = new Date(lastDate);
        lastEnd.setUTCDate(lastDate.getUTCDate() + 1);
  
        dataStart = lastDate;
        dataEnd = lastEnd;
  
        docs = await collection.find({
          security_id: 56785,
          timestamp: { $gte: lastDate, $lt: lastEnd }
        }, { projection: { _id: 0, LTP: 1, timestamp: 1 } })
          .sort({ timestamp: 1 })
          .toArray();
      }
  
      const results = [];
      const intervalMap: Record<string, boolean> = {};
  
      for (const doc of docs) {
        const ts = new Date(doc.timestamp);
        const ltp = parseFloat(doc.LTP);
        if (isNaN(ltp)) continue;
  
        // Round to nearest interval
        const rounded = new Date(Math.floor(ts.getTime() / (interval * 60 * 1000)) * (interval * 60 * 1000));
        const roundedISO = rounded.toISOString();
  
        if (!intervalMap[roundedISO]) {
          intervalMap[roundedISO] = true;
  
          const atmStrike = Math.round(ltp / 50) * 50;
  
          const windowStart = new Date(rounded);
          windowStart.setMinutes(windowStart.getMinutes() - interval);
          const windowEnd = new Date(rounded);
          windowEnd.setMinutes(windowEnd.getMinutes() + interval);
  
          const [optionData, optionData2] = await Promise.all([
            collection.findOne({
              strike_price: atmStrike,
              trading_symbol: { $regex: '^NIFTY-Jun2025' },
              option_type: "CE",
              timestamp: { $gte: windowStart, $lt: windowEnd }
            }, { sort: { timestamp: -1 } }),
            collection.findOne({
              strike_price: atmStrike,
              trading_symbol: { $regex: '^NIFTY-Jun2025' },
              option_type: "PE",
              timestamp: { $gte: windowStart, $lt: windowEnd }
            }, { sort: { timestamp: -1 } }),
          ]);
  
          results.push({
            atmStrike,
            niftyLTP: ltp,
            timestamp: roundedISO,
            callOI: optionData?.OI ?? null,
            putOI: optionData2?.OI ?? null,
            callTimestamp: optionData?.timestamp ?? null,
            putTimestamp: optionData2?.timestamp ?? null
          });
        }
      }
  
      res.json({ atmStrikes: results });
  
    } catch (error) {
      console.error('Error fetching ATM strikes timeline:', error);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  app.get('/api/nifty/near5', async (_req, res) => {
    try {
      const collection = db.collection('all_nse_fno');
  
      // --- Step 1: Determine today’s date range ---
      const now = new Date();
      const todayStart = new Date(now.setUTCHours(0, 0, 0, 0));
      const todayEnd = new Date(todayStart);
      todayEnd.setUTCDate(todayEnd.getUTCDate() + 1);
  
      // --- Step 2: Try to fetch today’s latest NIFTY LTP ---
      let latestNifty = await collection.find({
        security_id: 56785,
        timestamp: { $gte: todayStart, $lt: todayEnd }
      })
        .sort({ timestamp: -1 })
        .limit(1)
        .toArray();
  
      // --- Step 3: If today's data missing, fallback to latest available date in DB ---
      let dataStart = todayStart;
      let dataEnd = todayEnd;
  
      if (!latestNifty.length || !latestNifty[0].LTP) {
        const lastAvailable = await collection.find({ security_id: 56785 })
          .sort({ timestamp: -1 })
          .limit(1)
          .toArray();
  
        if (!lastAvailable.length || !lastAvailable[0].timestamp) {
          res.status(404).json({ error: 'No historical Nifty LTP found in database' });
          return;
        }
  
        const lastDate = new Date(lastAvailable[0].timestamp);
        const lastStart = new Date(Date.UTC(lastDate.getUTCFullYear(), lastDate.getUTCMonth(), lastDate.getUTCDate()));
        const lastEnd = new Date(lastStart);
        lastEnd.setUTCDate(lastEnd.getUTCDate() + 1);
  
        dataStart = lastStart;
        dataEnd = lastEnd;
  
        latestNifty = await collection.find({
          security_id: 56785,
          timestamp: { $gte: dataStart, $lt: dataEnd }
        }).sort({ timestamp: -1 }).limit(1).toArray();
  
        if (!latestNifty.length || !latestNifty[0].LTP) {
          res.status(404).json({ error: 'Failed to fetch LTP even for last available date' });
          return;
        }
      }
  
      // --- Step 4: Determine ATM strike and nearby strikes ---
      const niftyLTP = parseFloat(latestNifty[0].LTP);
      const atmStrike = Math.round(niftyLTP / 50) * 50;
      // const strikePrices = Array.from({ length: 5 }, (_, i) => atmStrike - 100 + i * 50);
      const strikePrices = Array.from({ length: 11 }, (_, i) => atmStrike - 250 + i * 50);
  
      // --- Step 5: Get CE and PE OI data for each strike ---
      const results = await Promise.all(
        strikePrices.map(async (strike) => {
          const CE_docs = await collection.find({
            strike_price: strike,
            option_type: "CE",
            trading_symbol: { $regex: '^NIFTY-Jun2025' },
            timestamp: { $gte: dataStart, $lt: dataEnd }
          }).toArray();
  
          const PE_docs = await collection.find({
            strike_price: strike,
            option_type: "PE",
            trading_symbol: { $regex: '^NIFTY-Jun2025' },
            timestamp: { $gte: dataStart, $lt: dataEnd }
          }).toArray();
  
          const groupedByTimestamp: { [key: string]: any } = {};
  
          CE_docs.forEach(doc => {
            const ts = new Date(doc.timestamp).toISOString();
            if (!groupedByTimestamp[ts]) groupedByTimestamp[ts] = { strike_price: strike };
            groupedByTimestamp[ts].callOI = doc.OI || 0;
            groupedByTimestamp[ts].callTimestamp = doc.timestamp;
          });
  
          PE_docs.forEach(doc => {
            const ts = new Date(doc.timestamp).toISOString();
            if (!groupedByTimestamp[ts]) groupedByTimestamp[ts] = { strike_price: strike };
            groupedByTimestamp[ts].putOI = doc.OI || 0;
            groupedByTimestamp[ts].putTimestamp = doc.timestamp;
          });
  
          return Object.values(groupedByTimestamp);
        })
      );
  
      const flattened = results.flat();
  
      // --- Step 6: Fetch Nifty LTP timeline for the same date ---
      const niftyDocs = await collection
        .find({
          security_id: 56785,
          timestamp: { $gte: dataStart, $lt: dataEnd }
        }, { projection: { _id: 0, LTP: 1, timestamp: 1 } })
        .sort({ timestamp: 1 })
        .toArray();
  
      const nifty = niftyDocs.map(doc => ({
        value: doc.LTP,
        timestamp: doc.timestamp,
      }));
  
      // --- Step 7: Include dateUsed for frontend reference ---
      const dateUsed = dataStart.toISOString().split('T')[0];
  
      res.json({ atmStrike, overall: flattened, nifty, dateUsed });
  
    } catch (error) {
      console.error('Error fetching NEAR5:', error);
      res.status(500).json({ error: 'Internal server error' });
    }
  });


  app.get('/api/nifty/overall', async (_req, res) => {
    try {
      const collection = db.collection('all_nse_fno');
  
      // Step 1: Get today's UTC date range
      const now = new Date();
      const todayStart = new Date(now.setUTCHours(0, 0, 0, 0));
      const todayEnd = new Date(todayStart);
      todayEnd.setUTCDate(todayEnd.getUTCDate() + 1);
  
      // Step 2: Try fetching latest NIFTY from today
      let latestNifty = await collection.find({
        security_id: 56785,
        timestamp: { $gte: todayStart, $lt: todayEnd }
      }).sort({ timestamp: -1 }).limit(1).toArray();
  
      let dataStart = todayStart;
      let dataEnd = todayEnd;
  
      // Step 3: If no LTP today, find most recent available date
      if (!latestNifty.length || !latestNifty[0].LTP) {
        const lastAvailable = await collection.find({ security_id: 56785 })
          .sort({ timestamp: -1 })
          .limit(1)
          .toArray();
  
        if (!lastAvailable.length || !lastAvailable[0].timestamp) {
          res.status(404).json({ error: 'No NIFTY LTP data available in database' });
          return;
        }
  
        const lastDate = new Date(lastAvailable[0].timestamp);
        const lastStart = new Date(Date.UTC(lastDate.getUTCFullYear(), lastDate.getUTCMonth(), lastDate.getUTCDate()));
        const lastEnd = new Date(lastStart);
        lastEnd.setUTCDate(lastEnd.getUTCDate() + 1);
  
        dataStart = lastStart;
        dataEnd = lastEnd;
  
        latestNifty = await collection.find({
          security_id: 56785,
          timestamp: { $gte: lastStart, $lt: lastEnd }
        }).sort({ timestamp: -1 }).limit(1).toArray();
  
        if (!latestNifty.length || !latestNifty[0].LTP) {
          res.status(404).json({ error: 'Failed to fetch LTP for most recent available date' });
          return;
        }
      }
  
      // Step 4: Calculate ATM and 21 strikes (±500 range)
      const niftyLTP = parseFloat(latestNifty[0].LTP);
      const atmStrike = Math.round(niftyLTP / 50) * 50;
      const strikePrices = Array.from({ length: 21 }, (_, i) => atmStrike - 500 + i * 50);
  
      // Step 5: For each strike, group OI by timestamp
      const results = await Promise.all(
        strikePrices.map(async (strike) => {
          const CE_docs = await collection.find({
            strike_price: strike,
            option_type: "CE",
            trading_symbol: { $regex: '^NIFTY-Jun2025' },
            timestamp: { $gte: dataStart, $lt: dataEnd }
          }).toArray();
  
          const PE_docs = await collection.find({
            strike_price: strike,
            option_type: "PE",
            trading_symbol: { $regex: '^NIFTY-Jun2025' },
            timestamp: { $gte: dataStart, $lt: dataEnd }
          }).toArray();
  
          const groupedByTimestamp: { [key: string]: any } = {};
  
          CE_docs.forEach(doc => {
            const ts = new Date(doc.timestamp).toISOString();
            if (!groupedByTimestamp[ts]) groupedByTimestamp[ts] = { strike_price: strike };
            groupedByTimestamp[ts].callOI = doc.OI || 0;
            groupedByTimestamp[ts].callTimestamp = doc.timestamp;
          });
  
          PE_docs.forEach(doc => {
            const ts = new Date(doc.timestamp).toISOString();
            if (!groupedByTimestamp[ts]) groupedByTimestamp[ts] = { strike_price: strike };
            groupedByTimestamp[ts].putOI = doc.OI || 0;
            groupedByTimestamp[ts].putTimestamp = doc.timestamp;
          });
  
          return Object.values(groupedByTimestamp);
        })
      );
  
      const flattened = results.flat();
  
      // Step 6: Fetch NIFTY LTP timeline
      const niftyDocs = await collection.find({
        security_id: 56785,
        timestamp: { $gte: dataStart, $lt: dataEnd }
      }, {
        projection: { _id: 0, LTP: 1, timestamp: 1 }
      }).sort({ timestamp: 1 }).toArray();
  
      const nifty = niftyDocs.map(doc => ({
        value: doc.LTP,
        timestamp: doc.timestamp
      }));
  
      const dateUsed = dataStart.toISOString().split('T')[0];
  
      res.json({ atmStrike, overall: flattened, nifty, dateUsed });
  
    } catch (error) {
      console.error('Error fetching OVERALL:', error);
      res.status(500).json({ error: 'Internal server error' });
    }
  });
}
