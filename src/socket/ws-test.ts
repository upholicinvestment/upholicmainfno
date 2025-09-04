import path from "path";
import dotenv from "dotenv";
import WebSocket from "ws";
import axios from "axios";
import { MongoClient } from "mongodb";

dotenv.config({ path: path.resolve(__dirname, "../../.env") });

// MongoDB setup
const MONGO_URI = process.env.MONGO_URI || "mongodb://localhost:27017";
const mongoClient = new MongoClient(MONGO_URI);
const db = mongoClient.db("Upholic");
const collection = db.collection("new_all_nse_data");

const EXCHANGE_SEGMENT = "NSE_FNO";
const securityIds = [
    '35052', '35053', '35054', '35055', '35107', '35108', '35109', '35110', 
    '35111', '35112', '35113', '35114', '35179', '35183', '35189', '35190', 
    '37102', '37116', '38340', '38341', '42509', '42510', '42511', '42512', 
    '43562', '43563', '46963', '46964', '46965', '46966', '46967', '46968', 
    '46969', '46976', '46977', '46978', '46979', '46986', '46987', '46988', 
    '46989', '46990', '46991', '47002', '51299', '51300', '51305', '51306', 
    '51307', '51308', '52540', '52541', '52546', '52547', '53216', '55525', 
    '55526', '55527', '55528', '55529', '55530', '55531', '55534', '55535', 
    '55538', '55539', '55540', '55541', '55542', '55543', '55544', '55545', 
    '55546', '55547', '55548', '55549', '55550', '55551', '55552', '55553', 
    '55554', '55555', '55556', '55557', '55558', '55559', '55560', '55561', 
    '55562', '55563', '55564', '55565', '55566', '55567', '55568', '55569', 
    '55570', '55571', '55574', '55575', '55576', '55577', '55578', '55579', 
    '55580', '55581', '55582', '55583', '55584', '55585', '55586', '55587', 
    '55588', '55589', '55590', '55591', '55592', '55595', '55596', '55597', 
    '55598', '55599', '55600', '55601', '55602', '55603', '55604', '55605', 
    '55606', '55607', '55608', '55609', '55610', '55615', '55616', '55617', 
    '55618', '55619', '55620', '55621', '55622', '55623', '55624', '55625', 
    '55626', '55627', '55628', '55629', '55630', '55631', '55632', '55633', 
    '55634', '55635', '55636', '55637', '55638', '55639', '55640', '55641', 
    '55642', '55643', '55644', '55645', '55646', '55647', '55648', '55649', 
    '55650', '55651', '55652', '55653', '55654', '55655', '55656', '55657', 
    '55658', '55659', '55660', '55661', '55662', '55663', '55667', '55669', 
    '55670', '55671', '55672', '55673', '55674', '55675', '55676', '55677', 
    '55678', '55679', '55680', '55681', '55682', '55683', '55684', '55685', 
    '55686', '55687', '55688', '55689', '55690', '55691', '55692', '55693', 
    '55694', '55695', '55696', '55699', '55700', '55701', '55702', '55703', 
    '55704', '55705', '55706', '55711', '55720', '55721', '55722', '55723', 
    '55724', '55725', '55726', '55727', '55728', '55729', '55730', '55731', 
    '55732', '55733', '55734', '55735', '55736', '55737', '55738', '55739', 
    '55740', '55743', '55744', '55745', '55746', '55747', '55748', '55749', 
    '55750', '55751', '55752', '55753', '55754', '55755', '55756', '55757', 
    '55758', '55759', '55760', '55761', '55762', '55763', '55764', '55765', 
    '55766', '55767', '55770', '55771', '55772', '55773', '55774', '55775', 
    '55776', '55777', '55778', '55779', '55780', '55781', '55782', '55783', 
    '55784', '55785', '55786', '55787', '55788', '55789', '55790', '55791', 
    '55792', '55793', '55794', '55795', '55796', '55797', '55798', '55799', 
    '55800', '55801', '55802', '55803', '55804', '55805', '55806', '55807', 
    '55808', '55809', '55810', '55811', '55812', '55815', '55816', '55817', 
    '55820', '55821', '55824', '55825', '55826', '55829', '55830', '55831', 
    '55832', '55833', '55834', '55835', '55836', '55837', '55838', '55839', 
    '55840', '55841', '55843', '55844', '55846', '55847', '55848', '55849', 
    '55850', '55851', '55852', '55853', '55854', '55855', '55856', '55857', 
    '55858', '55859', '55860', '55861', '55862', '55863', '55864', '55865', 
    '55866', '55867', '55868', '55869', '55870', '55871', '55872', '55873', 
    '55875', '55876', '55878', '55879', '55880', '55881', '55882', '55883', 
    '55884', '55885', '55886', '55887', '55892', '55893', '55896', '55897', 
    '55898', '55899', '55900', '55901', '55902', '55903', '55904', '55905', 
    '55906', '55907', '55908', '55909', '55912', '55913', '55914', '55915', 
    '55916', '55917', '55918', '55919', '55920', '55921', '55922', '55923', 
    '55926', '55927', '55928', '55929', '55930', '55931', '55932', '55933', 
    '55934', '55935', '55936', '55937', '55938', '55939', '55940', '55941', 
    '55942', '55943', '55944', '55945', '55946', '55947', '55948', '55949', 
    '55950', '55951', '55952', '55953', '55956', '55957', '55958', '55959', 
    '55960', '55961', '55962', '55963', '55964', '55965', '55966', '55967', 
    '55968', '55969', '55970', '55971', '55972', '55973', '55974', '55975', 
    '55976', '55977', '55986', '55987', '55991', '55992', '55994', '55995', 
    '55996', '55997', '55998', '55999', '56000', '56001', '56002', '56003', 
    '56004', '56005', '56006', '56007', '56008', '56009', '56010', '56011', 
    '56012', '56013', '56014', '56015', '56016', '60067', '60068', '60069', 
    '60070', '60075', '60076', '60077', '60078', '60085', '60086', '60087', 
    '60088'
];

const MAX_WS_BATCH = 100;
const MAX_REST_BATCH = 1000;
const REFRESH_INTERVAL = 10000; // 10 seconds

// Convert UTC to IST (UTC+5:30)
function getISTTimestamp() {
    const now = new Date();
    now.setHours(now.getHours() + 5);
    now.setMinutes(now.getMinutes() + 30);
    return now;
}

const url = `wss://api-feed.dhan.co?version=2&token=${process.env.DHAN_API_KEY}&clientId=${process.env.DHAN_CLIENT_ID}&authType=2`;
// console.log("Connecting to:", url);

const ws = new WebSocket(url);

// Connect to MongoDB when starting the application
async function connectToDatabase() {
    try {
        await mongoClient.connect();
        // console.log("✅ Connected to MongoDB!");
    } catch (err) {
        console.error("❌ MongoDB connection error:", err);
    }
}

connectToDatabase();

ws.on("open", () => {
    // console.log("✅ Connected! Subscribing to LTP feed...");
    subscribeTicker(securityIds);
    startQuotePolling();
});

ws.on("message", (msg: Buffer) => {
    parseBinaryPacket(msg);
});

ws.on("error", (err) => console.error("❌ WebSocket Error:", err));
ws.on("close", (code, reason) =>
    console.log(`❌ WebSocket closed. Code: ${code}, Reason: ${reason.toString()}`)
);

// --------------------
// WebSocket Subscription
// --------------------
function subscribeTicker(ids: string[]) {
    for (let i = 0; i < ids.length; i += MAX_WS_BATCH) {
        const batch = ids.slice(i, i + MAX_WS_BATCH);
        const payload = {
            RequestCode: 2, // Ticker feed
            InstrumentCount: batch.length,
            InstrumentList: batch.map((id) => ({
                ExchangeSegment: EXCHANGE_SEGMENT,
                SecurityId: id,
            })),
        };
        ws.send(JSON.stringify(payload));
        // console.log("📡 Subscribed WebSocket LTP batch:", batch);
    }
}

// --------------------
// REST API Quote Polling (Full Market Depth Data)
// --------------------
async function startQuotePolling() {
    setInterval(async () => {
        try {
            for (let i = 0; i < securityIds.length; i += MAX_REST_BATCH) {
                const batch = securityIds.slice(i, i + MAX_REST_BATCH);
                const payload = { [EXCHANGE_SEGMENT]: batch.map(Number) };

                const response: any = await axios.post(
                    "https://api.dhan.co/v2/marketfeed/quote",
                    payload,
                    {
                        headers: {
                            "Content-Type": "application/json",
                            "access-token": process.env.DHAN_API_KEY || "",
                            "client-id": process.env.DHAN_CLIENT_ID || "",
                        },
                    }
                );

                const marketData = response?.data?.data?.[EXCHANGE_SEGMENT] || {};
                // console.log(
                //     JSON.stringify(
                //         { data: { [EXCHANGE_SEGMENT]: marketData }, status: "success" },
                //         null,
                //         2
                //     )
                // );

                // Store the data in MongoDB
                try {
                    if (Object.keys(marketData).length > 0) {
                        const timestamp = getISTTimestamp();
                        
                        // Convert the market data to individual documents
                        const documents = Object.entries(marketData).map(([security_id, data]: [string, any]) => ({
                            security_id,
                            average_price: data.average_price,
                            buy_quantity: data.buy_quantity,
                            depth: {
                                buy: data.depth?.buy || [],
                                sell: data.depth?.sell || []
                            },
                            exchange: data.exchange,
                            last_price: data.last_price,
                            last_quantity: data.last_quantity,
                            last_trade_time: data.last_trade_time,
                            lower_circuit_limit: data.lower_circuit_limit,
                            net_change: data.net_change,
                            ohlc: {
                                open: data.ohlc?.open,
                                close: data.ohlc?.close,
                                high: data.ohlc?.high,
                                low: data.ohlc?.low
                            },
                            oi: data.oi,
                            oi_day_high: data.oi_day_high,
                            oi_day_low: data.oi_day_low,
                            sell_quantity: data.sell_quantity,
                            timestamp,
                            upper_circuit_limit: data.upper_circuit_limit,
                            volume: data.volume
                        }));

                        await collection.insertMany(documents);
                        // console.log(`💾 Saved ${documents.length} documents for batch ${i}-${i + batch.length}`);
                    }
                } catch (dbError) {
                    console.error("❌ MongoDB save error:", dbError);
                }
            }
        } catch (error: any) {
            console.error("❌ Quote API Error:", error?.response?.data || error.message);
        }
    }, REFRESH_INTERVAL);
}

// --------------------
// Parse Binary WS Packets
// --------------------
async function parseBinaryPacket(buffer: Buffer) {
    const feedResponseCode = buffer.readUInt8(0);
    const securityId = buffer.readInt32LE(4).toString();

    switch (feedResponseCode) {
        case 2: {
            const ltp = buffer.readFloatLE(8);
            // console.log(`💹 [WS LTP] SecID=${securityId} LTP=${ltp}`);
            
            // Store LTP data in MongoDB
            try {
                await collection.updateOne(
                    { security_id: securityId },
                    {
                        $set: {
                            last_price: ltp,
                            timestamp: getISTTimestamp()
                        }
                    },
                    { upsert: true }
                );
            } catch (dbError) {
                console.error("❌ MongoDB LTP save error:", dbError);
            }
            break;
        }
        case 6: {
            const prevClose = buffer.readFloatLE(8);
            // console.log(`🔄 [PrevClose] SecID=${securityId} PrevClose=${prevClose}`);
            
            // Store previous close data in MongoDB
            try {
                await collection.updateOne(
                    { security_id: securityId },
                    {
                        $set: {
                            "ohlc.close": prevClose,
                            timestamp: getISTTimestamp()
                        }
                    },
                    { upsert: true }
                );
            } catch (dbError) {
                console.error("❌ MongoDB PrevClose save error:", dbError);
            }
            break;
        }
        default:
            // console.log(`⚠ Unknown WS Packet Code=${feedResponseCode}`);
    }
}

// Handle graceful shutdown
process.on('SIGINT', async () => {
    try {
        await mongoClient.close();
        // console.log('MongoDB connection closed');
        process.exit(0);
    } catch (err) {
        console.error('Error closing MongoDB connection:', err);
        process.exit(1);
    }
});