"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.devGetLastOtp = exports.resetPassword = exports.forgotPassword = exports.login = exports.finalizeSignup = exports.registerIntent = exports.setDatabase = void 0;
const jsonwebtoken_1 = __importDefault(require("jsonwebtoken"));
const bcryptjs_1 = __importDefault(require("bcryptjs"));
const mongodb_1 = require("mongodb");
// ===== Database wiring =====
let db;
const setDatabase = (database) => {
    db = database;
};
exports.setDatabase = setDatabase;
// ===== JWT =====
const generateToken = (userId) => {
    const secret = process.env.JWT_SECRET;
    if (!secret)
        throw new Error("JWT_SECRET is not defined in .env");
    return jsonwebtoken_1.default.sign({ id: userId }, secret, { expiresIn: "30d" });
};
// ===== Env / Utils =====
const IS_DEV = process.env.NODE_ENV !== "production";
const FORCE_LOG_OTP = process.env.SMS_DEBUG_FORCE_LOG === "true";
// Normalize Indian numbers to 91XXXXXXXXXX
function normalizePhone(raw) {
    if (!raw)
        return raw;
    let p = raw.replace(/\D/g, "");
    if (p.startsWith("91") && p.length === 12)
        return p;
    if (p.length === 10)
        return `91${p}`;
    return p;
}
function addMonths(d, n) {
    const x = new Date(d);
    x.setMonth(x.getMonth() + n);
    return x;
}
function addYears(d, n) {
    const x = new Date(d);
    x.setFullYear(x.getFullYear() + n);
    return x;
}
function nextExpiry(currentEndsAt, interval, now = new Date()) {
    const base = currentEndsAt && currentEndsAt > now ? currentEndsAt : now;
    return interval === "yearly" ? addYears(base, 1) : addMonths(base, 1);
}
// ===== SMS (SMSGatewayHub) =====
// Uses Node 18+ global fetch (no node-fetch needed)
async function sendOtpSMS(phone, otp) {
    const API_KEY = process.env.SMS_API_KEY;
    const SENDER_ID = process.env.SMS_SENDER_ID || "UPOHTC"; // exactly 6 chars
    const ROUTE_ID = process.env.SMS_ROUTE_ID || "1"; // you use 1
    const number = normalizePhone(phone);
    const text = `Dear Customer, your OTP for free call demo is ${otp}. Please use this to complete your registration. Do not share this OTP with anyone. - UpholicTech`;
    if (!API_KEY) {
        console.warn("[SMS] SMS_API_KEY missing. Skipping provider call.");
        // console.log(`[DEV SMS] ${number}: ${text}`);
        return;
    }
    if (IS_DEV || FORCE_LOG_OTP) {
        // console.log(`[DEV SMS] ${number}: ${text}`);
    }
    try {
        const qs = new URLSearchParams({
            APIKey: API_KEY,
            senderid: SENDER_ID,
            channel: "2",
            DCS: "0",
            flashsms: "0",
            number,
            text,
            route: ROUTE_ID,
        });
        const url = `https://www.smsgatewayhub.com/api/mt/SendSMS?${qs.toString()}`;
        const resp = await fetch(url, { method: "GET" });
        const body = await resp.text();
        // console.log("[SMS Response]", resp.status, body);
        if (!resp.ok) {
            console.error("[SMS ERROR] Non-200 status returned by provider.");
        }
    }
    catch (e) {
        console.error("[SMS ERROR] Exception:", e);
    }
}
// Optional email channel (stub)
async function sendOtpEmail(email, otp) {
    const text = `Your password reset OTP is ${otp}. It expires in 10 minutes.`;
    if (IS_DEV || FORCE_LOG_OTP) {
        // console.log(`[DEV EMAIL] ${email}: ${text}`);
    }
    // TODO: integrate email provider if you want email delivery
}
// ======================================================================
// =============== SIGNUP INTENT FLOW (no user created yet) =============
// ======================================================================
/**
 * POST /api/auth/register-intent
 * body: {
 *  name, email, password, phone,
 *  initialProductId?, initialVariantId?, brokerConfig?,
 *  billingInterval?  // "monthly" | "yearly" (only for bundle & journaling_solo)
 * }
 * returns: { signupIntentId }
 */
const registerIntent = async (req, res) => {
    try {
        const { name, email, password, phone, initialProductId, initialVariantId, brokerConfig, billingInterval, // new
         } = req.body;
        if (!name || !email || !password || !phone) {
            res.status(400).json({ message: "All fields are required" });
            return;
        }
        // Ensure user does not already exist
        const existingUser = await db.collection("users").findOne({
            $or: [{ email }, { phone }],
        });
        if (existingUser) {
            res
                .status(400)
                .json({ message: "User already exists with this email or phone" });
            return;
        }
        // Validate product/variant if provided (optional)
        let productId = null;
        let variantId = null;
        let normalizedInterval = "monthly";
        if (initialProductId) {
            const product = await db.collection("products").findOne({
                _id: new mongodb_1.ObjectId(initialProductId),
                isActive: true,
            });
            if (!product) {
                res
                    .status(400)
                    .json({ message: "Selected product not found or inactive" });
                return;
            }
            productId = product._id;
            const productKey = product.key;
            if (product.hasVariants) {
                // ALGO → enforce monthly-only (via variant)
                if (!initialVariantId) {
                    res
                        .status(400)
                        .json({ message: "Please select a plan for the chosen product." });
                    return;
                }
                const variant = await db.collection("product_variants").findOne({
                    _id: new mongodb_1.ObjectId(initialVariantId),
                    productId,
                    isActive: true,
                });
                if (!variant) {
                    res
                        .status(400)
                        .json({ message: "Selected plan is invalid or inactive." });
                    return;
                }
                variantId = variant._id;
                normalizedInterval = "monthly"; // variants are monthly only
            }
            else {
                // No variants → allow yearly only for bundle/journaling_solo
                if (productKey === "essentials_bundle" ||
                    productKey === "journaling_solo") {
                    normalizedInterval =
                        billingInterval === "yearly" ? "yearly" : "monthly";
                }
                else {
                    normalizedInterval = "monthly";
                }
            }
        }
        const passwordHash = await bcryptjs_1.default.hash(password, 10);
        // Create signup intent (no user yet)
        const intent = await db.collection("signup_intents").insertOne({
            name,
            email,
            phone,
            passwordHash,
            productId,
            variantId,
            brokerConfig: brokerConfig || null,
            billingInterval: normalizedInterval, // store it
            status: "created", // created | completed | cancelled | expired
            createdAt: new Date(),
            updatedAt: new Date(),
        });
        res.status(201).json({ signupIntentId: intent.insertedId });
    }
    catch (err) {
        console.error("registerIntent error:", err);
        res.status(500).json({ message: "Server error" });
    }
};
exports.registerIntent = registerIntent;
/**
 * POST /api/auth/finalize-signup
 * body: { signupIntentId }
 * Creates user (and user_products if product exists) for FREE/NO-PAYMENT flows.
 * For paid flows, the user is created in /payments/verify after signature check.
 */
const finalizeSignup = async (req, res) => {
    try {
        const { signupIntentId } = req.body;
        if (!signupIntentId) {
            res.status(400).json({ message: "signupIntentId is required" });
            return;
        }
        const intent = await db.collection("signup_intents").findOne({
            _id: new mongodb_1.ObjectId(signupIntentId),
        });
        if (!intent) {
            res.status(404).json({ message: "Signup intent not found" });
            return;
        }
        if (intent.status !== "created") {
            res
                .status(400)
                .json({ message: "Signup intent is not in a finalizable state" });
            return;
        }
        // Double-check duplicates
        const dup = await db.collection("users").findOne({
            $or: [{ email: intent.email }, { phone: intent.phone }],
        });
        if (dup) {
            res.status(400).json({ message: "User already exists" });
            return;
        }
        // Create user
        const now = new Date();
        const userIns = await db.collection("users").insertOne({
            name: intent.name,
            email: intent.email,
            phone: intent.phone,
            password: intent.passwordHash,
            role: "customer",
            createdAt: now,
            updatedAt: now,
        });
        // If product selected, attach as active (free/no-payment case) WITH EXPIRY
        if (intent.productId) {
            const product = await db
                .collection("products")
                .findOne({ _id: intent.productId });
            const productKey = product?.key;
            let interval = "monthly";
            if (productKey === "essentials_bundle" ||
                productKey === "journaling_solo") {
                interval =
                    (intent.billingInterval || "monthly");
            }
            else if (productKey === "algo_simulator") {
                interval = "monthly";
            }
            const variantId = intent.variantId || null;
            const newEndsAt = nextExpiry(null, interval, now);
            await db.collection("user_products").insertOne({
                userId: userIns.insertedId,
                productId: intent.productId,
                variantId,
                status: "active",
                startedAt: now,
                endsAt: newEndsAt,
                meta: { source: "signup_free", interval },
            });
            // Broker config if ALGO and present
            if (product?.key === "algo_simulator" &&
                intent.variantId &&
                intent.brokerConfig) {
                await db.collection("broker_configs").insertOne({
                    userId: userIns.insertedId,
                    productId: intent.productId,
                    variantId: intent.variantId,
                    brokerName: intent.brokerConfig?.brokerName,
                    createdAt: now,
                    updatedAt: now,
                    ...(intent.brokerConfig || {}),
                });
            }
        }
        // Mark intent complete
        await db.collection("signup_intents").updateOne({ _id: new mongodb_1.ObjectId(signupIntentId) }, {
            $set: {
                status: "completed",
                userId: userIns.insertedId,
                updatedAt: new Date(),
            },
        });
        const token = generateToken(userIns.insertedId.toString());
        res.json({
            token,
            user: {
                id: userIns.insertedId,
                name: intent.name,
                email: intent.email,
                phone: intent.phone,
            },
        });
    }
    catch (err) {
        console.error("finalizeSignup error:", err);
        res.status(500).json({ message: "Server error" });
    }
};
exports.finalizeSignup = finalizeSignup;
// ======================================================================
// ============================== LOGIN =================================
// ======================================================================
const login = async (req, res) => {
    try {
        const { email, password } = req.body;
        const user = await db.collection("users").findOne({ email });
        if (!user || !(await bcryptjs_1.default.compare(password, user.password))) {
            res.status(400).json({ message: "Invalid credentials" });
            return;
        }
        const token = generateToken(user._id.toString());
        // console.log(
        //   "[auth.login] JWT issued for user",
        //   (user as any)._id.toString(),
        //   token
        // );
        res.status(200).json({
            token,
            user: {
                id: user._id,
                name: user.name,
                email: user.email,
                phone: user.phone,
            },
        });
    }
    catch (err) {
        console.error("Login error:", err);
        res.status(500).json({ message: "Server error" });
    }
};
exports.login = login;
// ======================================================================
// ========================= FORGOT / RESET OTP =========================
// ======================================================================
/**
 * POST /api/auth/forgot-password
 * body: { emailOrPhone: string }
 * returns: { resetId, message }
 */
const forgotPassword = async (req, res) => {
    try {
        const { emailOrPhone } = req.body;
        if (!emailOrPhone) {
            res.status(400).json({ message: "emailOrPhone is required" });
            return;
        }
        const user = await db.collection("users").findOne({
            $or: [{ email: emailOrPhone }, { phone: emailOrPhone }],
        });
        // Always 200 to avoid user enumeration
        if (!user) {
            res
                .status(200)
                .json({ message: "If the account exists, an OTP has been sent." });
            return;
        }
        // Create a 6-digit OTP
        const otp = Math.floor(100000 + Math.random() * 900000).toString();
        const otpHash = await bcryptjs_1.default.hash(otp, 10);
        // Invalidate older resets for this user
        await db.collection("password_resets").updateMany({ userId: user._id, used: { $ne: true } }, { $set: { used: true, invalidatedAt: new Date() } });
        // Store reset request. In dev, also store otpPlain to simplify testing.
        const resetInsert = await db.collection("password_resets").insertOne({
            userId: user._id,
            otpHash,
            otpPlain: IS_DEV ? otp : undefined, // DEV ONLY, never in prod
            channel: user.phone ? "sms" : "email",
            attempts: 0,
            used: false,
            createdAt: new Date(),
            expiresAt: new Date(Date.now() + 10 * 60 * 1000), // 10 minutes
        });
        if (user.phone) {
            await sendOtpSMS(user.phone, otp);
        }
        else {
            await sendOtpEmail(user.email, otp);
        }
        res.status(200).json({
            resetId: resetInsert.insertedId,
            message: "If the account exists, an OTP has been sent.",
        });
    }
    catch (err) {
        console.error("forgotPassword error:", err);
        res.status(500).json({ message: "Server error" });
    }
};
exports.forgotPassword = forgotPassword;
/**
 * POST /api/auth/reset-password
 * body: { resetId: string, otp: string, newPassword: string }
 */
const resetPassword = async (req, res) => {
    try {
        const { resetId, otp, newPassword } = req.body;
        if (!resetId || !otp || !newPassword) {
            res
                .status(400)
                .json({ message: "resetId, otp and newPassword are required" });
            return;
        }
        const resetDoc = await db
            .collection("password_resets")
            .findOne({ _id: new mongodb_1.ObjectId(resetId) });
        if (!resetDoc) {
            res.status(400).json({ message: "Invalid or expired reset request" });
            return;
        }
        if (resetDoc.used) {
            res.status(400).json({ message: "This reset request is already used" });
            return;
        }
        if (new Date(resetDoc.expiresAt).getTime() < Date.now()) {
            res.status(400).json({ message: "OTP expired" });
            return;
        }
        // Limit attempts
        if (resetDoc.attempts >= 5) {
            res.status(400).json({ message: "Too many attempts, request a new OTP" });
            return;
        }
        const isMatch = await bcryptjs_1.default.compare(otp, resetDoc.otpHash);
        if (!isMatch) {
            await db.collection("password_resets").updateOne({ _id: resetDoc._id }, { $inc: { attempts: 1 }, $set: { lastAttemptAt: new Date() } });
            res.status(400).json({ message: "Invalid OTP" });
            return;
        }
        // Update user password
        const hashed = await bcryptjs_1.default.hash(newPassword, 10);
        await db.collection("users").updateOne({ _id: resetDoc.userId }, { $set: { password: hashed, updatedAt: new Date() } });
        // Mark reset as used
        await db.collection("password_resets").updateOne({ _id: resetDoc._id }, { $set: { used: true, usedAt: new Date() } });
        res
            .status(200)
            .json({ message: "Password updated successfully. You can now log in." });
    }
    catch (err) {
        console.error("resetPassword error:", err);
        res.status(500).json({ message: "Server error" });
    }
};
exports.resetPassword = resetPassword;
// ===== DEV ONLY: fetch last OTP for a user (so you can test without SMS) =====
const devGetLastOtp = async (req, res) => {
    if (process.env.NODE_ENV === "production") {
        res.status(403).json({ message: "Not available in production" });
        return;
    }
    const emailOrPhone = req.query.emailOrPhone || "";
    if (!emailOrPhone) {
        res.status(400).json({ message: "emailOrPhone is required" });
        return;
    }
    const user = await db.collection("users").findOne({ $or: [{ email: emailOrPhone }, { phone: emailOrPhone }] }, { projection: { _id: 1 } });
    if (!user) {
        res.status(404).json({ message: "User not found" });
        return;
    }
    const pr = await db
        .collection("password_resets")
        .find({ userId: user._id })
        .sort({ createdAt: -1 })
        .limit(1)
        .toArray();
    if (!pr.length) {
        res.status(404).json({ message: "No reset record found" });
        return;
    }
    const last = pr[0];
    res.json({
        resetId: last._id,
        otpPlain: last.otpPlain || "(not stored)",
        expiresAt: last.expiresAt,
        used: !!last.used,
        attempts: last.attempts || 0,
    });
};
exports.devGetLastOtp = devGetLastOtp;
