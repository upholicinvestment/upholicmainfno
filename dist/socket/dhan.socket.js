"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.DhanSocket = void 0;
const ws_1 = __importDefault(require("ws"));
const ltp_service_1 = require("../services/ltp.service");
const dotenv_1 = __importDefault(require("dotenv"));
dotenv_1.default.config();
const MAX_BATCH_SIZE = 100;
const RECONNECT_INTERVAL = 5000;
const PING_INTERVAL = 30000;
const MAX_RECONNECT_ATTEMPTS = 5;
class DhanSocket {
    token;
    clientId;
    ws = null;
    securityIds = [];
    isConnected = false;
    reconnectTimeout = null;
    reconnectAttempts = 0;
    pingInterval = null;
    wsUrl;
    constructor(token, clientId) {
        this.token = token;
        this.clientId = clientId;
        this.wsUrl = `wss://api-feed.dhan.co?version=2&token=${this.token}&clientId=${this.clientId}&authType=2`;
        // console.log("WS URL:", this.wsUrl);
    }
    async connect(securityIds) {
        this.securityIds = securityIds;
        this.cleanup();
        // console.log(`🔗 Connecting to Dhan WebSocket: ${this.wsUrl}`);
        this.ws = new ws_1.default(this.wsUrl);
        this.setupEventHandlers();
    }
    setupEventHandlers() {
        if (!this.ws)
            return;
        this.ws.on("open", () => {
            // console.log("✅ Connected to Dhan WebSocket");
            this.isConnected = true;
            this.reconnectAttempts = 0;
            this.setupPing();
            this.subscribeBatch(2, this.securityIds);
        });
        this.ws.on("message", (message) => this.handleMessage(message));
        this.ws.on("close", (code, reason) => {
            console.error(`❌ WebSocket closed. Code: ${code}, Reason: ${reason}`);
            this.handleDisconnect();
        });
        this.ws.on("error", (err) => {
            console.error("❌ WebSocket error:", err);
            this.handleDisconnect();
        });
        this.ws.on("pong", () => console.debug("🏓 Pong received"));
    }
    handleMessage(message) {
        try {
            if (Buffer.isBuffer(message)) {
                const buf = message;
                const feedCode = buf.readUInt8(0);
                const secId = buf.readInt32LE(4);
                if (feedCode === 2) {
                    const ltp = buf.readFloatLE(8);
                    // console.log(`💹 [Ticker] SecID=${secId} LTP=${ltp}`);
                    (0, ltp_service_1.saveLTP)({ securityId: secId, LTP: ltp });
                }
            }
        }
        catch (err) {
            console.error("Failed to parse incoming message:", err);
        }
    }
    subscribeBatch(requestCode, ids) {
        if (!this.ws || !this.isConnected)
            return;
        for (let i = 0; i < ids.length; i += MAX_BATCH_SIZE) {
            const batch = ids.slice(i, i + MAX_BATCH_SIZE);
            const payload = {
                RequestCode: requestCode,
                InstrumentCount: batch.length,
                InstrumentList: batch.map((id) => ({
                    ExchangeSegment: "NSE_FNO",
                    SecurityId: id.toString(),
                })),
            };
            this.ws.send(JSON.stringify(payload));
            // console.log(`📡 Sent subscription batch:`, batch);
        }
    }
    setupPing() {
        this.pingInterval = setInterval(() => {
            if (this.ws && this.isConnected) {
                this.ws.ping();
            }
        }, PING_INTERVAL);
    }
    handleDisconnect() {
        this.isConnected = false;
        this.cleanup();
        if (this.reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
            this.reconnectAttempts++;
            // console.log(`♻ Reconnecting attempt ${this.reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS}...`);
            this.reconnectTimeout = setTimeout(() => this.connect(this.securityIds), RECONNECT_INTERVAL);
        }
        else {
            console.error("❌ Max reconnection attempts reached.");
        }
    }
    cleanup() {
        if (this.pingInterval)
            clearInterval(this.pingInterval);
        if (this.reconnectTimeout)
            clearTimeout(this.reconnectTimeout);
        if (this.ws)
            this.ws.removeAllListeners();
    }
    disconnect() {
        if (this.ws) {
            this.ws.close();
        }
        this.cleanup();
    }
}
exports.DhanSocket = DhanSocket;
exports.default = DhanSocket;
