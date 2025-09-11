"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.sendOtp = exports.otpStore = void 0;
const https_1 = __importDefault(require("https"));
const OTP_CONFIG = {
    EXPIRY_SECONDS: 300,
    MAX_ATTEMPTS: 3,
    LENGTH: 6,
    COUNTRY_CODE: '91',
};
const SMS_CONFIG = {
    API_KEY: process.env.SMS_API_KEY || '',
    SENDER_ID: process.env.SMS_SENDER_ID || '',
    BASE_URL: 'https://www.smsgatewayhub.com/api/mt/SendSMS',
};
const generateOtp = () => Math.floor(100000 + Math.random() * 900000);
const cleanPhone = (phone) => phone.replace(/\D/g, '');
// 🧠 In-memory OTP store
exports.otpStore = {};
const sendSms = async (phone, message) => {
    const url = `${SMS_CONFIG.BASE_URL}?APIKey=${SMS_CONFIG.API_KEY}&senderid=${SMS_CONFIG.SENDER_ID}&channel=2&DCS=0&flashsms=0&number=${OTP_CONFIG.COUNTRY_CODE}${phone}&text=${encodeURIComponent(message)}&route=1`;
    return new Promise((resolve) => {
        https_1.default.get(url, (res) => {
            let responseData = '';
            res.on('data', (chunk) => (responseData += chunk));
            res.on('end', () => {
                // console.log('📩 SMS API Response:', responseData); // Debug log
                const lower = responseData.toLowerCase();
                if (lower.includes('invalid') ||
                    lower.includes('error') ||
                    lower.includes('fail') ||
                    lower.includes('not valid')) {
                    resolve(false);
                }
                else {
                    resolve(true);
                }
            });
        }).on('error', (err) => {
            console.error('❌ HTTPS error:', err);
            resolve(false);
        });
    });
};
const sendOtp = async (req, res) => {
    try {
        const phone = cleanPhone(req.body.phone || '');
        if (phone.length !== 10) {
            res.status(400).json({ success: false, message: 'Invalid phone number. Use 10 digits only.' });
            return;
        }
        const now = Math.floor(Date.now() / 1000);
        const existingOtp = exports.otpStore[phone];
        if (existingOtp) {
            const timeLeft = existingOtp.expiresAt - now;
            if (timeLeft > OTP_CONFIG.EXPIRY_SECONDS - 60) {
                res.status(429).json({ success: false, message: 'Please wait before requesting a new OTP.' });
                return;
            }
        }
        const otp = generateOtp();
        exports.otpStore[phone] = {
            otp,
            expiresAt: now + OTP_CONFIG.EXPIRY_SECONDS,
            attempts: 0,
            verified: false,
        };
        const message = `Dear Customer, your OTP for free call demo is ${otp}. Please use this to complete your registration. Do not share this OTP with anyone. - UpholicTech`;
        const smsSent = await sendSms(phone, message);
        if (!smsSent) {
            res.status(200).json({
                success: true,
                message: 'OTP may be delivered, but SMS API returned a warning. Please verify manually.',
                expiresIn: OTP_CONFIG.EXPIRY_SECONDS,
                debug: true,
            });
            return;
        }
        res.status(200).json({
            success: true,
            message: 'OTP sent successfully.',
            expiresIn: OTP_CONFIG.EXPIRY_SECONDS,
        });
    }
    catch (error) {
        res.status(500).json({ success: false, message: 'Internal server error', error: error.message });
    }
};
exports.sendOtp = sendOtp;
