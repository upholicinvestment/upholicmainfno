// server/src/utils/invoice.ts
import path from "path";
import fs from "fs";
import axios from "axios";
import PDFDocument from "pdfkit";
import express, { type Express, type Request, type Response } from "express";
import { sendMail } from "./mailer";

// ====== types ======
export type InvoiceItem = {
  name: string;
  qty: number;
  rate: number;
  gst: number;
  inclusive?: boolean;
};

export type Party = {
  name?: string;
  address?: string;
  email?: string;
  phone?: string;
  gstin?: string;
};

export type InvoicePayload = {
  invoiceNo: string;
  invoiceDate?: string;
  dueDate?: string;
  billTo: Party;
  shipTo?: Party | null;
  items: InvoiceItem[];

  logo?: string; // absolute path | URL | data-URI
  fromName?: string;
  fromAddress?: string;
  fromEmail?: string;
  fromPhone?: string;
  fromWebsite?: string;
  fromGSTIN?: string;

  signature?: string; // absolute path | URL | data-URI
  signName?: string;
  signTitle?: string;

  gstInclusive?: boolean;

  subject?: string;
  text?: string;
  html?: string;
};

// ===== Brand (UpholicTech) =====
const BRAND = {
  fromName: "UpholicTech",
  fromAddress: "FA-05/9, Vikas Business Centre, Thane(W), 400601",
  email: "billing@upholictech.com",
  phone: "022-44511316",
  gstin: "GST27AADCU5626L1ZY",
  website: "www.upholictech.com",
  colorDark: "#0B1224",
  color: "#1E2A78",
  colorAccent: "#4C6FFF",
  text: "#111111",
  textMuted: "#5B6472",
  line: "#E5E7EB",
};

const DEBUG_ASSETS =
  String(process.env.DEBUG_INVOICE_ASSETS || "").toLowerCase() === "true";
const DEFAULT_LOGO_FILE =
  process.env.INVOICE_LOGO_FILE_NAME?.trim() || "Upholictech.png";

// ===== Fonts (Montserrat optional) =====
const FONTS = {
  regular: path.join(__dirname, "fonts", "Montserrat-Regular.ttf"),
  medium: path.join(__dirname, "fonts", "Montserrat-Medium.ttf"),
  bold: path.join(__dirname, "fonts", "Montserrat-Bold.ttf"),
};
const fileExists = (p: string) => {
  try {
    fs.accessSync(p, fs.constants.R_OK);
    return true;
  } catch {
    return false;
  }
};
const HAVE_MONTSERRAT =
  fileExists(FONTS.regular) &&
  fileExists(FONTS.medium) &&
  fileExists(FONTS.bold);

// ===== Safe Jimp loader (ESM/CJS compatible) =====
let _JIMP: any = null;
async function getJimp() {
  if (_JIMP) return _JIMP;
  // @ts-ignore - shim in src/types/jimp.d.ts
  const mod = await import("jimp");
  _JIMP = (mod as any).default || (mod as any).Jimp || mod;
  return _JIMP;
}
async function jimpRead(buf: Buffer) {
  const J = await getJimp();
  if (typeof J.read === "function") return J.read(buf);
  return new J(buf);
}

// ===== utils: image loading & cleanup =====
function fromDataUrl(uri?: string | null) {
  const m = /^data:image\/png;base64,(.+)$/i.exec(uri || "");
  if (!m) return null;
  try {
    return Buffer.from(m[1], "base64");
  } catch {
    return null;
  }
}
function localCandidates(rel: string) {
  return [path.join(process.cwd(), rel), path.resolve(rel)].filter(
    (v, i, a) => !!v && a.indexOf(v) === i
  );
}
async function loadImageBuffer(maybePathOrUrl?: string | null) {
  if (!maybePathOrUrl) return null;

  const dataBuf = fromDataUrl(maybePathOrUrl);
  if (dataBuf) return dataBuf;

  if (path.isAbsolute(maybePathOrUrl)) {
    if (fileExists(maybePathOrUrl)) return fs.readFileSync(maybePathOrUrl);
    return null;
  }
  if (/^https?:\/\//i.test(maybePathOrUrl)) {
    try {
      const res = await axios.get<ArrayBuffer>(maybePathOrUrl, {
        responseType: "arraybuffer",
      });
      return Buffer.from(res.data);
    } catch {
      return null;
    }
  }
  for (const pth of localCandidates(maybePathOrUrl)) {
    if (fileExists(pth)) return fs.readFileSync(pth);
  }
  return null;
}

async function removeSolidBackgroundToTransparent(buf: Buffer) {
  try {
    const img = await jimpRead(buf);
    const { width: W, height: H, data } = img.bitmap;
    const idxOf = (x: number, y: number) => (W * y + x) * 4;

    const c = [
      [0, 0],
      [W - 1, 0],
      [0, H - 1],
      [W - 1, H - 1],
    ].map(([x, y]) => {
      const i = idxOf(x, y);
      return { r: data[i], g: data[i + 1], b: data[i + 2] };
    });
    const avg = {
      r: (c[0].r + c[1].r + c[2].r + c[3].r) / 4,
      g: (c[0].g + c[1].g + c[2].g + c[3].g) / 4,
      b: (c[0].b + c[1].b + c[2].b + c[3].b) / 4,
    };
    const nearBg = (r: number, g: number, b: number) => {
      const dr = r - avg.r,
        dg = g - avg.g,
        db = b - avg.b;
      const dist = Math.sqrt(dr * dr + dg * dg + db * db);
      return (
        dist < 60 ||
        (Math.max(r, g, b) - Math.min(r, g, b) < 28 && (r + g + b) / 3 > 220)
      );
    };

    const q: [number, number][] = [];
    const seen = new Uint8Array(W * H);
    const pushIf = (x: number, y: number) => {
      if (x < 0 || y < 0 || x >= W || y >= H) return;
      const p = W * y + x;
      if (seen[p]) return;
      const i = idxOf(x, y);
      const r = data[i],
        g = data[i + 1],
        b = data[i + 2],
        a = data[i + 3];
      if (a > 0 && nearBg(r, g, b)) {
        seen[p] = 1;
        q.push([x, y]);
      }
    };

    for (let x = 0; x < W; x++) {
      pushIf(x, 0);
      pushIf(x, H - 1);
    }
    for (let y = 0; y < H; y++) {
      pushIf(0, y);
      pushIf(W - 1, y);
    }
    while (q.length) {
      const [x, y] = q.pop()!;
      const i = idxOf(x, y);
      data[i + 3] = 0;
      if (x > 0) pushIf(x - 1, y);
      if (x + 1 < W) pushIf(x + 1, y);
      if (y > 0) pushIf(x, y - 1);
      if (y + 1 < H) pushIf(x, y + 1);
    }

    img.blur(1);
    return (await img.getBufferAsync("image/png")) as Buffer;
  } catch {
    return buf;
  }
}

async function removeGlobalWhiteToAlpha(buf: Buffer, tol = 20, minLuma = 240) {
  try {
    const img = await jimpRead(buf);
    const { data } = img.bitmap;

    for (let i = 0; i < data.length; i += 4) {
      const r = data[i],
        g = data[i + 1],
        b = data[i + 2];
      const luma = (r + g + b) / 3;
      const neutral = Math.max(r, g, b) - Math.min(r, g, b) <= tol;
      if (neutral && luma >= minLuma) data[i + 3] = 0;
    }

    img.blur(1);
    return (await img.getBufferAsync("image/png")) as Buffer;
  } catch {
    return buf;
  }
}

async function removeSignatureBackground(buf: Buffer) {
  const step1 = await removeSolidBackgroundToTransparent(buf);
  const step2 = await removeGlobalWhiteToAlpha(step1, 22, 238);
  return step2;
}
function resolveSignaturePath(explicitPath?: string) {
  if (explicitPath) return explicitPath;
  const candidates = [
    "assets/Sign.png",
    "assets/signature.png",
    "assets/sign.png",
  ];
  for (const p of candidates) {
    const abs = path.join(process.cwd(), p);
    if (fileExists(abs)) return abs;
  }
  return undefined;
}

// ---------- Helpers for logo resolution
function findCaseInsensitive(dir: string, wanted: string) {
  try {
    const files = fs.readdirSync(dir);
    const match = files.find((f) => f.toLowerCase() === wanted.toLowerCase());
    return match ? path.join(dir, match) : null;
  } catch {
    return null;
  }
}

// ---------- Robust logo resolver (env override + many common locations)
function resolveLogoPath(explicitPath?: string) {
  // 1) Direct data-URI / URL
  if (explicitPath && /^data:image\/|^https?:\/\//i.test(explicitPath)) {
    if (DEBUG_ASSETS) console.log("[invoice] Using payload logo (data/url).");
    return explicitPath;
  }

  // 2) Env overrides
  const envPath = process.env.INVOICE_LOGO_PATH?.trim();
  if (envPath) {
    if (DEBUG_ASSETS) console.log("[invoice] Using INVOICE_LOGO_PATH:", envPath);
    return envPath;
  }
  const envUrl = process.env.INVOICE_LOGO_URL?.trim();
  if (envUrl) {
    if (DEBUG_ASSETS) console.log("[invoice] Using INVOICE_LOGO_URL:", envUrl);
    return envUrl;
  }

  // 3) Try absolute/relative locations (and case-insensitive checks)
  const tryDirs = [
    path.join(__dirname, "../../../assets"),
    path.join(__dirname, "../../assets"),
    path.join(__dirname, "../assets"),
    path.join(process.cwd(), "assets"),
    path.join(process.cwd(), "public"),
    path.join(process.cwd(), "static"),
    path.join(process.cwd(), "src", "assets"), // your screenshot location
    path.join(process.cwd(), "..", "src", "assets"),
    path.join(process.cwd(), "..", "..", "src", "assets"),
  ];

  for (const dir of tryDirs) {
    const p = path.join(dir, DEFAULT_LOGO_FILE);
    if (fileExists(p)) {
      if (DEBUG_ASSETS) console.log("[invoice] logo found at:", p);
      return p;
    }
    // case-insensitive fallback
    const ci = findCaseInsensitive(dir, DEFAULT_LOGO_FILE);
    if (ci && fileExists(ci)) {
      if (DEBUG_ASSETS) console.log("[invoice] logo found (CI) at:", ci);
      return ci;
    }
    if (DEBUG_ASSETS) console.log("[invoice] not found in dir:", dir);
  }

  if (DEBUG_ASSETS) {
    console.warn(
      "[invoice] No logo found. Set INVOICE_LOGO_PATH or place",
      DEFAULT_LOGO_FILE,
      "under src/assets or assets/"
    );
    console.warn("[invoice] cwd:", process.cwd(), " __dirname:", __dirname);
  }
  return undefined; // render without logo
}

const MONEY = new Intl.NumberFormat("en-IN", {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});
const money = (n: number) => MONEY.format(Number(n || 0));

function hairline(
  doc: any,
  x1: number,
  y: number,
  x2: number,
  color = BRAND.line
) {
  doc
    .save()
    .moveTo(x1, y)
    .lineTo(x2, y)
    .lineWidth(0.6)
    .strokeColor(color)
    .stroke()
    .restore();
}
function label(doc: any, text: string, x: number, y: number) {
  if (!text) return;
  doc.font("M").fontSize(10).fillColor(BRAND.textMuted).text(text, x, y);
}
function value(
  doc: any,
  text: string,
  x: number,
  y: number,
  width: number,
  opts: {
    bold?: boolean;
    size?: number;
    align?: "left" | "center" | "right";
  } = {}
) {
  if (text === undefined || text === null) return;
  doc
    .font(opts.bold ? "B" : "R")
    .fontSize(opts.size || 12)
    .fillColor(BRAND.text)
    .text(String(text), x, y, { width, align: opts.align || "left" });
}

function numberToWordsIndian(nInput: number) {
  let n = Math.round(Number(nInput || 0));
  if (n === 0) return "Zero only";
  const ones = [
    "",
    "one",
    "two",
    "three",
    "four",
    "five",
    "six",
    "seven",
    "eight",
    "nine",
    "ten",
    "eleven",
    "twelve",
    "thirteen",
    "fourteen",
    "fifteen",
    "sixteen",
    "seventeen",
    "eighteen",
    "nineteen",
  ];
  const tens = [
    "",
    "",
    "twenty",
    "thirty",
    "forty",
    "fifty",
    "sixty",
    "seventy",
    "eighty",
    "ninety",
  ];
  const two = (num: number) =>
    num < 20
      ? ones[num]
      : tens[Math.floor(num / 10)] + (num % 10 ? " " + ones[num % 10] : "");
  const three = (num: number) =>
    num < 100
      ? two(num)
      : ones[Math.floor(num / 100)] +
        " hundred" +
        (num % 100 ? " and " + two(num % 100) : "");
  const parts: string[] = [];
  const crore = Math.floor(n / 10000000);
  n %= 10000000;
  const lakh = Math.floor(n / 100000);
  n %= 100000;
  const thousand = Math.floor(n / 1000);
  n %= 1000;
  const rest = n;
  if (crore) parts.push(two(crore) + " crore");
  if (lakh) parts.push(two(lakh) + " lakh");
  if (thousand) parts.push(two(thousand) + " thousand");
  if (rest) parts.push(three(rest));
  const s = parts.join(" ");
  return s.charAt(0).toUpperCase() + s.slice(1) + " only";
}

function drawHeader(
  doc: any,
  W: number,
  heroH: number,
  P: number,
  _name: string | undefined,
  logoBuf: Buffer | null,
  meta: { no?: string; date?: string; due?: string }
) {
  const HEADER_BG = "#0C1E52";
  const BAND_H = Math.min(heroH, 110);
  doc.save().rect(0, 0, W, BAND_H).fill(HEADER_BG).restore();

  // brand block + meta space (kept from your layout)
  const avail = W - 2 * P;
  const gap = 16;
  let brandW = Math.max(200, Math.min(300, Math.floor(avail * 0.45)));
  let metaW = avail - brandW - gap;
  if (metaW < 230) {
    metaW = 230;
    brandW = avail - metaW - gap;
  }
  if (brandW < 200) {
    brandW = 200;
    metaW = avail - brandW - gap;
  }

  // Logo placement
  // If the logo is wide, fit to height; if tall, fit to width.
  const logoMaxH = Math.floor(BAND_H * 0.8);
  const logoMaxW = Math.floor(brandW * 0.9);
  const logoX = P;
  const logoY = Math.max(8, BAND_H - logoMaxH - 6);

  if (logoBuf) {
    try {
      doc.image(logoBuf, logoX, logoY, { fit: [logoMaxW, logoMaxH] });
    } catch (e) {
      if (DEBUG_ASSETS) console.warn("[invoice] header image error:", e);
    }
  } else if (DEBUG_ASSETS) {
    console.warn("[invoice] header rendering WITHOUT logo.");
  }

  // Right-side meta (Invoice No.)
  const innerX = P + brandW + gap + 120;
  const innerW = W - 2 * P - brandW - gap - 50;

  const labelText = "Invoice No.";
  const tinyGap = 4;
  const lift = 32;
  const baseY = Math.max(16, Math.min(heroH, 110) / 2 - 6 - lift);

  doc.font("M").fontSize(10).fillColor("#E9EDFF");
  const labelW = doc.widthOfString(labelText);
  doc.text(labelText, innerX, baseY);

  doc
    .font("B")
    .fontSize(12)
    .fillColor("#FFFFFF")
    .text(
      meta?.no ? String(meta.no) : "—",
      innerX + labelW + tinyGap,
      baseY - 2,
      {
        width: innerW - (labelW + tinyGap),
      }
    );
}

export async function buildInvoicePdfBuffer(
  payload: Partial<InvoicePayload>
): Promise<Buffer> {
  const data: InvoicePayload = {
    invoiceNo: payload.invoiceNo || "INV-XXXX",
    invoiceDate: payload.invoiceDate || new Date().toISOString().slice(0, 10),
    dueDate: payload.dueDate || "—",
    billTo: payload.billTo || { name: "Customer" },
    shipTo: payload.shipTo || null,
    items:
      payload.items && payload.items.length
        ? payload.items
        : [
            {
              name: "Option Scalper PRO (Monthly)",
              qty: 1,
              rate: 14999,
              gst: 18,
              inclusive: true,
            },
          ],
    logo: resolveLogoPath(payload.logo), // robust resolver
    fromName: payload.fromName || BRAND.fromName,
    fromAddress: payload.fromAddress || BRAND.fromAddress,
    fromEmail: payload.fromEmail || BRAND.email,
    fromPhone: payload.fromPhone || BRAND.phone,
    fromWebsite: payload.fromWebsite || BRAND.website,
    fromGSTIN: payload.fromGSTIN || BRAND.gstin,
    signature: (payload.signature ||
      resolveSignaturePath(payload.signature)) as any,
    gstInclusive: !!payload.gstInclusive,
  } as InvoicePayload;

  const rawLogo = await loadImageBuffer(data.logo).catch(() => null);
  if (DEBUG_ASSETS)
    console.log("[invoice] logo load:", data.logo, "=>", !!rawLogo);
  // If you're still not seeing the image, temporarily set `const logoBuf = rawLogo;`
  const logoBuf = rawLogo
    ? await removeSolidBackgroundToTransparent(rawLogo)
    : null;

  let signatureBuf: Buffer | null = null;
  let sigW = 0,
    sigH = 0;
  if (data.signature) {
    try {
      const rawSig = await loadImageBuffer(data.signature);
      if (rawSig) {
        signatureBuf = await removeSignatureBackground(rawSig);
        const metaImg = await jimpRead(signatureBuf);
        sigW = metaImg.bitmap.width;
        sigH = metaImg.bitmap.height;
      }
    } catch {}
  }

  return await new Promise<Buffer>((resolve, reject) => {
    const doc = new PDFDocument({
      size: "A4",
      margin: 0,
      info: {
        Title: `${data.invoiceNo} — Invoice`,
        Author: data.fromName || "",
      },
    });

    const chunks: Buffer[] = [];
    doc.on("data", (c) => chunks.push(c as Buffer));
    doc.on("end", () => resolve(Buffer.concat(chunks)));
    doc.on("error", reject);

    if (HAVE_MONTSERRAT) {
      doc.registerFont("R", FONTS.regular);
      doc.registerFont("M", FONTS.medium);
      doc.registerFont("B", FONTS.bold);
    } else {
      doc.registerFont("R", "Helvetica");
      doc.registerFont("M", "Helvetica");
      doc.registerFont("B", "Helvetica-Bold");
    }

    const W = doc.page.width;
    const H = doc.page.height;
    const P = 54;

    const heroH = 140;
    drawHeader(doc, W, heroH, P, data.fromName, logoBuf, {
      no: data.invoiceNo,
      date: data.invoiceDate,
      due: data.dueDate || "—",
    });

    let y = heroH + 24;
    hairline(doc, P, y, W - P);
    y += 12;

    // LEFT: From
    label(doc, "From", P, y);
    value(doc, data.fromName || "", P, y + 14, 280, { bold: true });
    if (data.fromAddress) value(doc, data.fromAddress, P, y + 30, 280);
    if (data.fromGSTIN) value(doc, `GSTIN: ${data.fromGSTIN}`, P, y + 48, 280);
    if (data.fromEmail) value(doc, `Email: ${data.fromEmail}`, P, y + 68, 280);
    if (data.fromPhone) value(doc, `Phone: ${data.fromPhone}`, P, y + 84, 280);
    if (data.fromWebsite) value(doc, `${data.fromWebsite}`, P, y + 100, 280);

    // MID: Bill To
    const col2X = P + 300;
    label(doc, "Bill To", col2X, y);
    value(doc, data.billTo?.name || "—", col2X, y + 14, 260, { bold: true });
    if (data.billTo?.address)
      value(doc, data.billTo.address, col2X, y + 30, 260);
    if (data.billTo?.gstin)
      value(doc, `GSTIN: ${data.billTo.gstin}`, col2X, y + 48, 260);
    if (data.billTo?.email)
      value(doc, `Email: ${data.billTo.email}`, col2X, y + 68, 260);
    if (data.billTo?.phone)
      value(doc, `Phone: ${data.billTo.phone}`, col2X, y + 84, 260);

    // RIGHT: Ship To (optional)
    const col3X = W - P - 260;
    if (data.shipTo && (data.shipTo.name || data.shipTo.address)) {
      label(doc, "Ship To", col3X, y);
      value(doc, data.shipTo.name || "—", col3X, y + 14, 260, { bold: true });
      if (data.shipTo.address)
        value(doc, data.shipTo.address, col3X, y + 30, 260);
    }

    y += 130;
    hairline(doc, P, y, W - P);
    y += 14;

    // PRECOMPUTE LINES
    const round2 = (n: number) => Math.round((Number(n) || 0) * 100) / 100;
    const lineCalcs = data.items.map((it) => {
      const qty = Number(it.qty) || 0;
      const rate = Number(it.rate) || 0;
      const gstPct = Number(it.gst) || 0;
      const inclusive = !!it.inclusive || !!data.gstInclusive;

      const lineTotal = round2(qty * rate);
      let base: number, tax: number;
      if (inclusive) {
        base = round2(lineTotal / (1 + gstPct / 100));
        tax = round2(lineTotal - base);
      } else {
        base = round2(lineTotal);
        tax = round2(base * (gstPct / 100));
      }
      const cgst = round2(tax / 2);
      const sgst = round2(tax - cgst);
      return { qty, rate, gstPct, inclusive, base, tax, cgst, sgst, lineTotal };
    });

    const sumBase = round2(lineCalcs.reduce((a, b) => a + b.base, 0));
    const cgstTotal = round2(lineCalcs.reduce((a, b) => a + b.cgst, 0));
    const sgstTotal = round2(lineCalcs.reduce((a, b) => a + b.sgst, 0));
    const sumTax = round2(cgstTotal + sgstTotal);
    const grandTotal = round2(lineCalcs.reduce((a, b) => a + b.lineTotal, 0));

    // TABLE
    const tableX = P;
    const tableW = W - 2 * P;
    let tableY = y;

    const srW = 48;
    const totalW = 110;
    const descW = tableW - (srW + totalW);

    const COLS = { sr: srW, desc: Math.max(180, descW), total: totalW };
    const colX = {
      sr: tableX,
      desc: tableX + COLS.sr,
      total() {
        return this.desc + COLS.desc;
      },
    };

    const stroke = (x: number, y: number, w: number, h: number) =>
      doc
        .save()
        .rect(x, y, w, h)
        .lineWidth(0.8)
        .strokeColor("#000")
        .stroke()
        .restore();

    const cell = (
      x: number,
      y: number,
      w: number,
      h: number,
      text?: string,
      opts: any = {}
    ) => {
      stroke(x, y, w, h);
      doc
        .font(opts.bold ? "B" : "R")
        .fontSize(opts.size || 10)
        .fillColor("#000")
        .text(text ?? "", x + 4, y + 6, {
          width: w - 8,
          align: opts.align || "left",
          lineBreak: opts.noWrap ? false : true,
        });
    };

    const headerH = 24;
    cell(colX.sr, tableY, COLS.sr, headerH, "Sr. No.", {
      bold: true,
      align: "center",
      noWrap: true,
    });
    cell(colX.desc, tableY, COLS.desc, headerH, "Service Description", {
      bold: true,
      align: "center",
    });
    cell(colX.total(), tableY, COLS.total, headerH, "Total", {
      bold: true,
      align: "center",
      noWrap: true,
    });
    tableY += headerH;

    const rowH = 24;
    let sr = 1;
    for (let i = 0; i < data.items.length; i++) {
      const it = data.items[i];
      const calc = lineCalcs[i];
      cell(colX.sr, tableY, COLS.sr, rowH, String(sr++), {
        align: "center",
        noWrap: true,
      });
      cell(colX.desc, tableY, COLS.desc, rowH, String(it.name || ""), {
        size: 10,
      });
      cell(colX.total(), tableY, COLS.total, rowH, money(calc.base), {
        align: "right",
        noWrap: true,
      });
      tableY += rowH;
    }

    stroke(colX.sr, tableY, COLS.sr + COLS.desc, rowH);
    doc
      .font("B")
      .fontSize(11)
      .fillColor("#000")
      .text("Total", colX.sr + 6, tableY + 6, {
        width: COLS.sr + COLS.desc - 12,
      });
    cell(colX.total(), tableY, COLS.total, rowH, money(sumBase), {
      bold: true,
      align: "right",
      noWrap: true,
    });
    tableY += rowH;

    const leftBlockW = COLS.sr + COLS.desc;
    const summaryRow = (labelText: string, valueNum: number, bold = false) => {
      const h = 24;
      cell(colX.sr, tableY, leftBlockW, h, labelText, { bold });
      cell(colX.total(), tableY, COLS.total, h, money(valueNum), {
        bold,
        align: "right",
        noWrap: true,
      });
      tableY += h;
    };
    summaryRow("Total Amount before Tax", sumBase, false);
    summaryRow("Add: CGST (9%)", cgstTotal, false);
    summaryRow("Add: SGST (9%)", sgstTotal, false);
    summaryRow("Total Tax Amount (18%)", sumTax, true);
    summaryRow("Total Amount after Tax", grandTotal, true);

    const words = "Amount in words: " + numberToWordsIndian(grandTotal);
    const wordsHeight = Math.max(
      24,
      doc.heightOfString(words, { width: leftBlockW - 8 }) + 12
    );
    cell(colX.sr, tableY, leftBlockW, wordsHeight, words, { bold: false });
    cell(colX.total(), tableY, COLS.total, wordsHeight, money(grandTotal), {
      bold: true,
      align: "right",
      noWrap: true,
    });
    tableY += wordsHeight;

    // SIGNATURE (bottom-right)
    const FOOTER_PAD = 32;
    const SIG_BLOCK_W = 360;
    const SIG_BLOCK_H = 120;
    const sigX = W - P - SIG_BLOCK_W;
    const sigY = H - P - FOOTER_PAD - SIG_BLOCK_H;

    const maxW = 80,
      maxH = 60,
      topGap = 2;
    let renderW = 0,
      renderH = 0;
    if (signatureBuf && sigW && sigH) {
      const scale = Math.min(maxW / sigW, maxH / sigH, 1);
      renderW = Math.round(sigW * scale);
      renderH = Math.round(sigH * scale);
      const imgX = sigX + SIG_BLOCK_W - renderW;
      const imgY = sigY + topGap;
      try {
        doc.image(signatureBuf, imgX, imgY, {
          width: renderW,
          height: renderH,
        });
      } catch {}
    }

    const tinyGap = 1;
    const textY = sigY + topGap + (renderH || maxH) + tinyGap;

    doc.font("B").fontSize(11).fillColor(BRAND.text);
    const nameLineHeight = doc.currentLineHeight();
    doc.text(String(data.signName || ""), sigX, textY, {
      width: SIG_BLOCK_W,
      align: "right",
    });

    doc.font("R").fontSize(10).fillColor(BRAND.textMuted);
    const titleY = textY + nameLineHeight + 2;
    doc.text(String(data.signTitle || ""), sigX, titleY, {
      width: SIG_BLOCK_W,
      align: "right",
    });

    const thankYouY = Math.min(tableY + 24, sigY - 40);
    doc
      .font("M")
      .fontSize(12)
      .fillColor(BRAND.color)
      .text("Thank you for your business!", P, thankYouY, {
        align: "center",
        width: W - 2 * P,
      });
    doc
      .font("R")
      .fontSize(10)
      .fillColor(BRAND.textMuted)
      .text("We look forward to serving you again.", P, thankYouY + 20, {
        align: "center",
        width: W - 2 * P,
      });

    // page number (right)
    const pageNo = ((doc as any).page && (doc as any).page.number) || 1;
    doc
      .font("R")
      .fontSize(9)
      .fillColor("#9E9E9E")
      .text(String(pageNo), W - P - 10, H - 28, {
        width: 10,
        align: "right",
      });

    // electronic document notice (centered footer)
    doc
      .font("R")
      .fontSize(9)
      .fillColor("#6B7280")
      .text(
        "This is an electronically generated document and does not require a signature.",
        P,
        H - 42,
        { width: W - 2 * P, align: "center" }
      );

    doc.end();
  });
}

export async function sendInvoiceEmail(to: string, payload: InvoicePayload) {
  const pdf = await buildInvoicePdfBuffer(payload);
  const invoiceNo = payload.invoiceNo || "invoice";

  const from =
    process.env.MAIL_FROM ||
    process.env.SMTP_USER ||
    process.env.SES_USERNAME ||
    BRAND.email;

  const subject = payload.subject || `Invoice ${invoiceNo}`;
  const text =
    payload.text ||
    `Dear ${
      payload.billTo?.name || "Customer"
    },\n\nPlease find attached your invoice ${invoiceNo}.\n\nRegards,\n${
      payload.fromName || BRAND.fromName
    }`;
  const html =
    payload.html ||
    `<p>Dear ${
      payload.billTo?.name || "Customer"
    },</p><p>Please find attached your invoice <b>${invoiceNo}</b>.</p><p>Regards,<br>${
      payload.fromName || BRAND.fromName
    }</p>`;

  return sendMail({
    from,
    to,
    subject,
    text,
    html,
    attachments: [
      {
        filename: `${invoiceNo}.pdf`,
        content: pdf,
        contentType: "application/pdf",
      },
    ],
  });
}

// ============ Optional helpers / routes ============

// 1) Serve assets directory as static (so you can use INVOICE_LOGO_URL)
export function registerInvoiceAssets(app: Express) {
  // Prefer src/assets if it exists, else assets/
  const srcAssets = path.join(process.cwd(), "src", "assets");
  const rootAssets = path.join(process.cwd(), "assets");
  const chosen = fileExists(srcAssets) ? srcAssets : rootAssets;
  if (fileExists(chosen)) {
    app.use("/static", express.static(chosen));
    if (DEBUG_ASSETS) console.log("[invoice] static assets mounted at /static ->", chosen);
  } else if (DEBUG_ASSETS) {
    console.warn("[invoice] no local assets folder found to mount as static.");
  }
}

// 2) JSON endpoint to debug logo resolution
export function registerInvoiceDebug(app: Express, basePath = "/api/invoice") {
  app.get(`${basePath}/logo-debug`, (_req: Request, res: Response) => {
    const resolved = resolveLogoPath();
    const exists =
      !!resolved &&
      (!/^https?:\/\//i.test(resolved) ? fileExists(resolved) : true);

    res.json({
      resolved,
      exists,
      cwd: process.cwd(),
      __dirname,
      defaultLogoFile: DEFAULT_LOGO_FILE,
      hint:
        "Set INVOICE_LOGO_PATH to an absolute file path or INVOICE_LOGO_URL to an http(s) URL. You can also POST with { logo: '...absolute path or URL...' }.",
    });
  });
}

// 3) Preview / build / send routes
export function registerInvoiceRoutes(app: Express, basePath = "/api/invoice") {
  app.get(
    `${basePath}/preview`,
    async (_req: Request, res: Response): Promise<void> => {
      try {
        const pdf = await buildInvoicePdfBuffer({});
        res.setHeader("Content-Type", "application/pdf");
        res.setHeader("Content-Disposition", "inline; filename=preview.pdf");
        res.send(pdf);
      } catch (e) {
        res.status(500).json({ error: "failed_to_generate_invoice" });
      }
    }
  );

  app.post(
    `${basePath}`,
    async (req: Request, res: Response): Promise<void> => {
      try {
        const pdf = await buildInvoicePdfBuffer(req.body || {});
        res.setHeader("Content-Type", "application/pdf");
        res.setHeader(
          "Content-Disposition",
          `attachment; filename="${req.body?.invoiceNo || "invoice"}.pdf"`
        );
        res.send(pdf);
      } catch (e) {
        res.status(500).json({ error: "failed_to_generate_invoice" });
      }
    }
  );

  app.post(
    `${basePath}/send`,
    async (req: Request, res: Response): Promise<void> => {
      try {
        const to = req.body?.to || req.body?.billTo?.email;
        if (!to) {
          res.status(400).json({ error: "missing_to" });
          return;
        }
        const payload: InvoicePayload = {
          ...(req.body || {}),
          invoiceNo: req.body?.invoiceNo || "invoice",
          billTo: req.body?.billTo || {},
        };
        const info = await sendInvoiceEmail(to, payload);
        res.json({ ok: true, messageId: (info as any)?.messageId, to });
      } catch (e: any) {
        res.status(500).json({
          ok: false,
          error: "failed_to_send_email",
          detail: e?.message || String(e),
        });
      }
    }
  );
}
