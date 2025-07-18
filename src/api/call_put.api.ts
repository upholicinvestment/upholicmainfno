import { Express } from 'express';
import { Db } from 'mongodb';

export default function registerNiftyRoutes(app: Express, db: Db) {

  // -----------------------
  // Helper: Fetch CE & PE OI grouped by timestamp with carry-forward
  // -----------------------
  async function getOIForStrike(collection: any, strike: number, dataStart: Date, dataEnd: Date) {
    const docs = await collection.find({
      strike_price: strike,
      expiry_flag: "W",
      trading_symbol: { $regex: '^NIFTY-Jul2025' },
      option_type: { $in: ["CE", "PE"] },
      timestamp: { $gte: dataStart, $lt: dataEnd }
    }).sort({ timestamp: 1 }).toArray();

    console.log(`Fetched ${docs.length} CE/PE records for strike ${strike}`);

    const groupedByTimestamp: { [key: string]: any } = {};
    let lastCallOI: number | null = null;
    let lastPutOI: number | null = null;

    for (const doc of docs) {
      const ts = new Date(doc.timestamp).toISOString();
      if (!groupedByTimestamp[ts]) groupedByTimestamp[ts] = { strike_price: strike };

      if (doc.option_type === "CE") {
        lastCallOI = doc.OI || lastCallOI || 0;
        groupedByTimestamp[ts].callOI = lastCallOI;
        groupedByTimestamp[ts].callTimestamp = doc.timestamp;
      } else if (doc.option_type === "PE") {
        lastPutOI = doc.OI || lastPutOI || 0;
        groupedByTimestamp[ts].putOI = lastPutOI;
        groupedByTimestamp[ts].putTimestamp = doc.timestamp;
      }
    }

    console.log(`Processed ${Object.keys(groupedByTimestamp).length} timestamps for strike ${strike}`);
    return Object.values(groupedByTimestamp);
  }

  // -----------------------
  // /api/nifty/atm-strikes-timeline
  // -----------------------
  app.get('/api/nifty/atm-strikes-timeline', async (req, res) => {
    try {
      console.log('\n=== Processing ATM Strikes Timeline Request ===');
      const intervalParam = req.query.interval as string || '3';
      const interval = parseInt(intervalParam, 10);
      const collection = db.collection('all_nse_fno');

      const now = new Date();
      const todayStart = new Date(now.setUTCHours(0, 0, 0, 0));
      const todayEnd = new Date(todayStart);
      todayEnd.setUTCDate(todayEnd.getUTCDate() + 1);

      console.log(`Fetching NIFTY data for today (${todayStart.toISOString()} to ${todayEnd.toISOString()})`);

      let docs = await collection.find({
        security_id: 53216,
        timestamp: { $gte: todayStart, $lt: todayEnd }
      }, { projection: { _id: 0, LTP: 1, timestamp: 1 } })
        .sort({ timestamp: 1 })
        .toArray();

      if (!docs.length) {
        console.log('No data for today, falling back to last available date...');
        const lastEntry = await collection.find({ security_id: 53216 })
          .sort({ timestamp: -1 }).limit(1).toArray();

        if (!lastEntry.length) {
          res.status(404).json({ error: 'No NIFTY data found' });
          return;
        }

        const lastDate = new Date(lastEntry[0].timestamp);
        lastDate.setUTCHours(0, 0, 0, 0);
        const lastEnd = new Date(lastDate);
        lastEnd.setUTCDate(lastEnd.getUTCDate() + 1);

        docs = await collection.find({
          security_id: 53216,
          timestamp: { $gte: lastDate, $lt: lastEnd }
        }, { projection: { _id: 0, LTP: 1, timestamp: 1 } })
          .sort({ timestamp: 1 })
          .toArray();
      }

      if (!docs.length) {
        res.status(404).json({ error: 'No NIFTY data found for any date' });
        return;
      }

      // FIXED ATM STRIKE from the first LTP
      const firstLTP = parseFloat(docs[0].LTP);
      const fixedATM = Math.round(firstLTP / 50) * 50;
      console.log(`Fixed ATM Strike for session: ${fixedATM} (based on first LTP = ${firstLTP})`);

      const results = [];
      const intervalMap: Record<string, boolean> = {};

      for (const doc of docs) {
        const ts = new Date(doc.timestamp);
        const ltp = parseFloat(doc.LTP);
        if (isNaN(ltp)) continue;

        const rounded = new Date(Math.floor(ts.getTime() / (interval * 60 * 1000)) * (interval * 60 * 1000));
        const roundedISO = rounded.toISOString();

        if (!intervalMap[roundedISO]) {
          intervalMap[roundedISO] = true;

          const [optionDataCE, optionDataPE] = await Promise.all([
            collection.findOne({
              strike_price: fixedATM,
              expiry_flag: "W",
              option_type: "CE",
              trading_symbol: { $regex: '^NIFTY-Jul2025' },
              timestamp: { $lte: rounded }
            }, { sort: { timestamp: -1 } }),
            collection.findOne({
              strike_price: fixedATM,
              expiry_flag: "W",
              option_type: "PE",
              trading_symbol: { $regex: '^NIFTY-Jul2025' },
              timestamp: { $lte: rounded }
            }, { sort: { timestamp: -1 } }),
          ]);

          const resultItem = {
            atmStrike: fixedATM,
            niftyLTP: ltp,
            timestamp: roundedISO,
            callOI: optionDataCE?.OI ?? null,
            putOI: optionDataPE?.OI ?? null,
            callTimestamp: optionDataCE?.timestamp ?? null,
            putTimestamp: optionDataPE?.timestamp ?? null
          };

          console.log(`Interval ${roundedISO}: CE OI = ${resultItem.callOI}, PE OI = ${resultItem.putOI}`);
          results.push(resultItem);
        }
      }

      console.log(`Returning ${results.length} data points`);
      res.json({ atmStrikes: results });
    } catch (error) {
      console.error('Error fetching ATM strikes timeline:', error);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  // -----------------------
  // /api/nifty/near5
  // -----------------------
  app.get('/api/nifty/near5', async (req, res) => {
    try {
      console.log('\n=== Processing NEAR5 Request ===');
      const collection = db.collection('all_nse_fno');
      const now = new Date();
      const todayStart = new Date(now.setUTCHours(0, 0, 0, 0));
      const todayEnd = new Date(todayStart);
      todayEnd.setUTCDate(todayEnd.getUTCDate() + 1);

      let latestNifty = await collection.find({
        security_id: 53216,
        timestamp: { $gte: todayStart, $lt: todayEnd }
      }).sort({ timestamp: -1 }).limit(1).toArray();

      let dataStart = todayStart;
      let dataEnd = todayEnd;

      if (!latestNifty.length || !latestNifty[0].LTP) {
        console.log('No data for today, falling back to last available date...');
        const lastAvailable = await collection.find({ security_id: 53216 })
          .sort({ timestamp: -1 }).limit(1).toArray();
        if (!lastAvailable.length) {
          res.status(404).json({ error: 'No historical Nifty LTP found' });
          return;
        }
        const lastDate = new Date(lastAvailable[0].timestamp);
        const lastStart = new Date(Date.UTC(lastDate.getUTCFullYear(), lastDate.getUTCMonth(), lastDate.getUTCDate()));
        const lastEnd = new Date(lastStart);
        lastEnd.setUTCDate(lastEnd.getUTCDate() + 1);

        dataStart = lastStart;
        dataEnd = lastEnd;

        latestNifty = await collection.find({
          security_id: 53216,
          timestamp: { $gte: dataStart, $lt: dataEnd }
        }).sort({ timestamp: -1 }).limit(1).toArray();
      }

      const niftyLTP = parseFloat(latestNifty[0].LTP);
      const fixedATM = Math.round(niftyLTP / 50) * 50;  // NEAR5 still uses current ATM

      const strikePrices = Array.from({ length: 11 }, (_, i) => fixedATM - 250 + i * 50);
      console.log(`NIFTY LTP: ${niftyLTP}, ATM Strike: ${fixedATM}`);

      const results = (await Promise.all(
        strikePrices.map(strike => getOIForStrike(collection, strike, dataStart, dataEnd))
      )).flat();

      const niftyDocs = await collection.find({
        security_id: 53216,
        timestamp: { $gte: dataStart, $lt: dataEnd }
      }, { projection: { _id: 0, LTP: 1, timestamp: 1 } })
        .sort({ timestamp: 1 }).toArray();

      const nifty = niftyDocs.map(doc => ({ value: doc.LTP, timestamp: doc.timestamp }));
      const dateUsed = dataStart.toISOString().split('T')[0];

      res.json({ atmStrike: fixedATM, overall: results, nifty, dateUsed });
    } catch (error) {
      console.error('Error fetching NEAR5:', error);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  // -----------------------
  // /api/nifty/overall
  // -----------------------
  app.get('/api/nifty/overall', async (req, res) => {
    try {
      console.log('\n=== Processing OVERALL Request ===');
      const collection = db.collection('all_nse_fno');
      const now = new Date();
      const todayStart = new Date(now.setUTCHours(0, 0, 0, 0));
      const todayEnd = new Date(todayStart);
      todayEnd.setUTCDate(todayEnd.getUTCDate() + 1);

      let latestNifty = await collection.find({
        security_id: 53216,
        timestamp: { $gte: todayStart, $lt: todayEnd }
      }).sort({ timestamp: -1 }).limit(1).toArray();

      let dataStart = todayStart;
      let dataEnd = todayEnd;

      if (!latestNifty.length || !latestNifty[0].LTP) {
        console.log('No data for today, falling back to last available date...');
        const lastAvailable = await collection.find({ security_id: 53216 })
          .sort({ timestamp: -1 }).limit(1).toArray();
        if (!lastAvailable.length) {
          res.status(404).json({ error: 'No NIFTY LTP data available' });
          return;
        }
        const lastDate = new Date(lastAvailable[0].timestamp);
        const lastStart = new Date(Date.UTC(lastDate.getUTCFullYear(), lastDate.getUTCMonth(), lastDate.getUTCDate()));
        const lastEnd = new Date(lastStart);
        lastEnd.setUTCDate(lastEnd.getUTCDate() + 1);

        dataStart = lastStart;
        dataEnd = lastEnd;

        latestNifty = await collection.find({
          security_id: 53216,
          timestamp: { $gte: dataStart, $lt: dataEnd }
        }).sort({ timestamp: -1 }).limit(1).toArray();
      }

      const niftyLTP = parseFloat(latestNifty[0].LTP);
      const fixedATM = Math.round(niftyLTP / 50) * 50;

      const strikePrices = Array.from({ length: 21 }, (_, i) => fixedATM - 500 + i * 50);
      console.log(`NIFTY LTP: ${niftyLTP}, ATM Strike: ${fixedATM}`);

      const results = (await Promise.all(
        strikePrices.map(strike => getOIForStrike(collection, strike, dataStart, dataEnd))
      )).flat();

      const niftyDocs = await collection.find({
        security_id: 53216,
        timestamp: { $gte: dataStart, $lt: dataEnd }
      }, { projection: { _id: 0, LTP: 1, timestamp: 1 } })
        .sort({ timestamp: 1 }).toArray();

      const nifty = niftyDocs.map(doc => ({ value: doc.LTP, timestamp: doc.timestamp }));
      const dateUsed = dataStart.toISOString().split('T')[0];

      res.json({ atmStrike: fixedATM, overall: results, nifty, dateUsed });
    } catch (error) {
      console.error('Error fetching OVERALL:', error);
      res.status(500).json({ error: 'Internal server error' });
    }
  });
}
