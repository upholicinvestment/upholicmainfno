import nodemailer from "nodemailer";
import type SMTPTransport from "nodemailer/lib/smtp-transport";

type Transporter = nodemailer.Transporter<SMTPTransport.SentMessageInfo>;
let transporterPromise: Promise<Transporter> | null = null;

function resolveSMTPConfig(): SMTPTransport.Options {
  const hasSES =
    !!process.env.SES_SMTP_HOST &&
    !!process.env.SES_USERNAME &&
    !!process.env.SES_SMTP_PASS;

  if (hasSES) {
    const host = process.env.SES_SMTP_HOST!;
    const port = Number(process.env.SES_SMTP_PORT || 465);
    const secure =
      port === 465 ||
      String(process.env.SMTP_SECURE || "true").toLowerCase() === "true";

    return {
      host,
      port,
      secure,
      auth: {
        user: process.env.SES_USERNAME!,
        pass: process.env.SES_SMTP_PASS!,
      },
      tls: { minVersion: "TLSv1.2" },
    };
  }

  const host = process.env.SMTP_HOST || "smtp.gmail.com";
  const port = Number(process.env.SMTP_PORT || 465);
  const secure =
    port === 465 ||
    String(process.env.SMTP_SECURE || "true").toLowerCase() === "true";
  const user =
    process.env.SMTP_USER || process.env.MAIL_USER || process.env.EMAIL_USER;
  const pass =
    process.env.SMTP_PASS || process.env.MAIL_PASS || process.env.EMAIL_PASS;

  return {
    host,
    port,
    secure,
    auth: user && pass ? { user, pass } : undefined,
    tls: { minVersion: "TLSv1.2" },
  };
}

async function createTransporter(): Promise<Transporter> {
  const opts = resolveSMTPConfig();
  const transporter = nodemailer.createTransport(opts);

  try {
    await transporter.verify();
    console.log(`[mail] SMTP verified: ${opts.host}:${opts.port}`);
  } catch (e: any) {
    console.warn("[mail] SMTP verify failed:", e?.message || e);
  }
  return transporter;
}

export async function getTransporter(): Promise<Transporter> {
  if (!transporterPromise) transporterPromise = createTransporter();
  return transporterPromise;
}

export type MailOptions = nodemailer.SendMailOptions & { forceFrom?: string };

export async function sendMail(options: MailOptions) {
  const transporter = await getTransporter();
  const fromDefault =
    options.forceFrom ||
    process.env.MAIL_FROM ||
    process.env.SMTP_USER ||
    process.env.SES_USERNAME ||
    "billing@upholic.in";

  const final: nodemailer.SendMailOptions = {
    ...options,
    from: options.from || fromDefault,
  };
  return transporter.sendMail(final);
}

export default sendMail;
