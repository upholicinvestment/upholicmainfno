// src/controllers/otp_send.controller.ts
import https from "https";
import { RequestHandler } from "express";

const OTP_CONFIG = {
  EXPIRY_SECONDS: 300,
  MAX_ATTEMPTS: 3,
  LENGTH: 6,
  COUNTRY_CODE: "91",
};

const SMS_CONFIG = {
  API_KEY: process.env.SMS_API_KEY || "",
  SENDER_ID: process.env.SMS_SENDER_ID || "UPOHTC", // exactly 6 chars
  BASE_URL: "https://www.smsgatewayhub.com/api/mt/SendSMS",
  DLT_TEMPLATE_ID:
    process.env.SMS_DLT_TEMPLATE_ID_SIGNUP || "1707175767028337759",
  DLT_ENTITY_ID: process.env.SMS_DLT_ENTITY_ID || "1701174893649845477",
  ROUTE_ID: process.env.SMS_ROUTE_ID || "1",
  DEBUG: String(process.env.SMS_DEBUG_FORCE_LOG || "").toLowerCase() === "true",
};

const generateOtp = (): number => Math.floor(100000 + Math.random() * 900000);
const cleanPhone = (phone: string): string => phone.replace(/\D/g, "");

// 🧠 In-memory OTP store (for signup)
export const otpStore: Record<
  string,
  { otp: number; expiresAt: number; attempts: number; verified: boolean }
> = {};

const sendSms = async (phone: string, message: string): Promise<boolean> => {
  const number = `${OTP_CONFIG.COUNTRY_CODE}${phone}`;

  const qs = new URLSearchParams({
    APIKey: SMS_CONFIG.API_KEY,
    senderid: SMS_CONFIG.SENDER_ID,
    channel: "2",
    DCS: "0",
    flashsms: "0",
    number,
    text: message,
    route: SMS_CONFIG.ROUTE_ID,

    // DLT params (add common aliases too)
    dlttemplateid: SMS_CONFIG.DLT_TEMPLATE_ID,
    TemplateId: SMS_CONFIG.DLT_TEMPLATE_ID,
    EntityId: SMS_CONFIG.DLT_ENTITY_ID,
  });

  const url = `${SMS_CONFIG.BASE_URL}?${qs.toString()}`;
  if (SMS_CONFIG.DEBUG) {
    const masked = SMS_CONFIG.API_KEY.replace(/.(?=.{3})/g, "•");
    console.log(
      "[SMS DEBUG][Signup]",
      url.replace(SMS_CONFIG.API_KEY, masked),
      "\nText:",
      message
    );
  }

  return new Promise((resolve) => {
    https
      .get(url, (res) => {
        let responseData = "";
        res.on("data", (chunk) => (responseData += chunk));
        res.on("end", () => {
          const lower = responseData.toLowerCase();
          if (
            res.statusCode !== 200 ||
            /invalid|error|fail|rejected|mismatch|not valid/i.test(lower)
          ) {
            console.error("[SMS ERROR][Signup]", res.statusCode, responseData);
            resolve(false);
          } else {
            resolve(true);
          }
        });
      })
      .on("error", (err) => {
        console.error("❌ HTTPS error:", err);
        resolve(false);
      });
  });
};

export const sendOtp: RequestHandler = async (req, res): Promise<void> => {
  try {
    const phone = cleanPhone(req.body.phone || "");
    if (phone.length !== 10) {
      res
        .status(400)
        .json({
          success: false,
          message: "Invalid phone number. Use 10 digits only.",
        });
      return;
    }

    const now = Math.floor(Date.now() / 1000);
    const existingOtp = otpStore[phone];

    if (existingOtp) {
      const timeLeft = existingOtp.expiresAt - now;
      if (timeLeft > OTP_CONFIG.EXPIRY_SECONDS - 60) {
        res
          .status(429)
          .json({
            success: false,
            message: "Please wait before requesting a new OTP.",
          });
        return;
      }
    }

    const otp = generateOtp();
    otpStore[phone] = {
      otp,
      expiresAt: now + OTP_CONFIG.EXPIRY_SECONDS,
      attempts: 0,
      verified: false,
    };

    // ✅ EXACT Signup template
    const message = `Dear Customer, your OTP for Signup is ${otp}. Please use this to complete your registration. Do not share this OTP with anyone. - UpholicTech`;
    const smsSent = await sendSms(phone, message);

    if (!smsSent) {
      res.status(200).json({
        success: true,
        message:
          "OTP may be delivered, but the SMS provider returned a warning. Please verify manually.",
        expiresIn: OTP_CONFIG.EXPIRY_SECONDS,
        debug: true,
      });
      return;
    }

    res.status(200).json({
      success: true,
      message: "OTP sent successfully.",
      expiresIn: OTP_CONFIG.EXPIRY_SECONDS,
      dltTemplateId: SMS_CONFIG.DLT_TEMPLATE_ID,
    });
  } catch (error: any) {
    res
      .status(500)
      .json({
        success: false,
        message: "Internal server error",
        error: error.message,
      });
  }
};
