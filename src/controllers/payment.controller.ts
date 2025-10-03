import { Request, Response, RequestHandler } from "express";
import crypto from "crypto";
import { Db, ObjectId } from "mongodb";
import jwt from "jsonwebtoken";
import { razorpay } from "../services/razorpay.service";
import { sendInvoiceEmail, type InvoicePayload } from "../utils/invoice";

let db: Db;
export const setPaymentDatabase = (database: Db) => {
  db = database;
};

const BUNDLE_SKU_KEY = "essentials_bundle";
const ALGO_SKU_KEY = "algo_simulator";
const JOURNALING_SOLO_SKU_KEY = "journaling_solo";

type Interval = "monthly" | "yearly";

/** === RENEWAL CONFIG === */
const RENEW_WINDOW_DAYS = Number(process.env.RENEW_WINDOW_DAYS || 7);

const generateToken = (userId: string) => {
  const secret = process.env.JWT_SECRET;
  if (!secret) throw new Error("JWT_SECRET is not defined in .env");
  return jwt.sign({ id: userId }, secret, { expiresIn: "30d" });
};

async function getBundleComponentsSet(): Promise<Set<string>> {
  const bundle = await db.collection("products").findOne({ key: BUNDLE_SKU_KEY, isActive: true });
  const comps = Array.isArray((bundle as any)?.components)
    ? ((bundle as any).components as string[])
    : ["technical_scanner", "fundamental_scanner", "fno_khazana", "journaling", "fii_dii_data"];
  return new Set(comps);
}

/* ---------------- helpers ---------------- */
const startOfDay = (d: Date) => {
  const x = new Date(d);
  x.setHours(0, 0, 0, 0);
  return x;
};
const daysUntil = (dateLike?: Date | string | null) => {
  if (!dateLike) return Infinity;
  const today = startOfDay(new Date());
  const exp = startOfDay(new Date(dateLike));
  return Math.ceil((exp.getTime() - today.getTime()) / 86400000);
};

const getUserIdFromReqOrBearer = (req: Request): ObjectId | null => {
  const raw =
    (req as any).user?.id ||
    (req as any).user?._id ||
    (req as any).userId ||
    null;

  if (raw) {
    try {
      return new ObjectId(raw);
    } catch {}
  }

  const auth = req.headers.authorization;
  if (!auth) return null;
  const [scheme, token] = auth.split(" ");
  if (scheme?.toLowerCase() !== "bearer" || !token) return null;

  try {
    const secret = process.env.JWT_SECRET!;
    const decoded: any = jwt.verify(token, secret);
    return decoded?.id ? new ObjectId(decoded.id) : null;
  } catch {
    return null;
  }
};

const resolvePriceForProduct = async (
  product: any,
  interval: Interval,
  variant?: any
) => {
  const productKey = product.key as string;

  if (productKey === BUNDLE_SKU_KEY) {
    const pm = Number(product.priceMonthly);
    const py = Number(product.priceYearly);
    const envPM = Number(process.env.BUNDLE_MONTHLY_PRICE || 4999);
    const envPY = Number(process.env.BUNDLE_YEARLY_PRICE || envPM * 10);

    const priceRupees =
      interval === "yearly"
        ? Number.isFinite(py) && py > 0 ? py : envPY
        : Number.isFinite(pm) && pm > 0 ? pm : envPM;

    return {
      amountPaise: Math.round(priceRupees * 100),
      displayName:
        "Trader Essentials Bundle (5-in-1)" +
        (interval === "yearly" ? " – Yearly" : " – Monthly"),
    };
  }

  if (productKey === ALGO_SKU_KEY) {
    if (!variant) {
      const err: any = new Error("variantId required for this product");
      err.status = 400;
      throw err;
    }
    const priceMonthly = (variant as any).priceMonthly;
    if (!priceMonthly || typeof priceMonthly !== "number") {
      return { amountPaise: 0, displayName: `${product.name} - ${variant.name}` };
    }
    return {
      amountPaise: Math.round(priceMonthly * 100),
      displayName: `${product.name} - ${variant.name} (Monthly)`,
    };
  }

  if (productKey === JOURNALING_SOLO_SKU_KEY) {
    const pm = Number(product.priceMonthly);
    const py = Number(product.priceYearly);
    const envPM = Number(process.env.JOURNALING_SOLO_MONTHLY_PRICE || 299);
    const envPY = Number(process.env.JOURNALING_SOLO_YEARLY_PRICE || envPM * 10);

    const priceRupees =
      interval === "yearly"
        ? Number.isFinite(py) && py > 0 ? py : envPY
        : Number.isFinite(pm) && pm > 0 ? pm : envPM;

    return {
      amountPaise: Math.round(priceRupees * 100),
      displayName: product.name + (interval === "yearly" ? " (Yearly)" : " (Monthly)"),
    };
  }

  const err: any = new Error("This product is not purchasable");
  err.status = 400;
  throw err;
};

// ---- date helpers (expiry) ----
function addMonths(date: Date, months: number): Date {
  const d = new Date(date);
  d.setMonth(d.getMonth() + months);
  return d;
}
function addYears(date: Date, years: number): Date {
  const d = new Date(date);
  d.setFullYear(d.getFullYear() + years);
  return d;
}
function computeEndsAt(interval: Interval, from: Date): Date {
  return interval === "yearly" ? addYears(from, 1) : addMonths(from, 1);
}

// ---------- DUPLICATE / UPGRADE HELPERS ----------
function endsActiveExpr(now: Date) {
  return { $or: [{ endsAt: null }, { endsAt: { $gt: now } }] };
}
function variantFilterExpr(variantId: ObjectId | null) {
  return variantId === null
    ? { $or: [{ variantId: null }, { variantId: { $exists: false } }] }
    : { variantId };
}

async function findActiveEntitlement(
  userId: ObjectId,
  productId: ObjectId,
  variantId: ObjectId | null
) {
  const now = new Date();
  return db.collection("user_products").findOne({
    userId,
    productId,
    status: "active",
    $and: [variantFilterExpr(variantId), endsActiveExpr(now)],
  });
}

async function findActiveBundleComponentEntitlements(userId: ObjectId) {
  const now = new Date();
  const componentKeys = Array.from(await getBundleComponentsSet());
  const components = await db
    .collection("products")
    .find({ key: { $in: componentKeys }, isActive: true })
    .project({ _id: 1 })
    .toArray();
  const componentIds = components.map((p: any) => p._id as ObjectId);

  return db.collection("user_products").find({
    userId,
    productId: { $in: componentIds },
    status: "active",
    $and: [
      variantFilterExpr(null),
      endsActiveExpr(now),
      { "meta.source": "payment_bundle" },
    ],
  }).toArray();
}

async function cancelActiveJournalingSolo(userId: ObjectId, reason: string) {
  const now = new Date();
  const journalingSolo = await db
    .collection("products")
    .findOne({ key: JOURNALING_SOLO_SKU_KEY, isActive: true });
  if (!journalingSolo) return;

  await db.collection("user_products").updateMany(
    {
      userId,
      productId: (journalingSolo as any)._id,
      status: "active",
      $or: [{ endsAt: null }, { endsAt: { $gt: now } }],
    },
    {
      $set: {
        status: "cancelled",
        endsAt: now,
        "meta.cancelledBy": reason,
        "meta.cancelledAt": now,
      },
    }
  );
}

const intervalToSet = (interval: Interval, isUpgrade: boolean) =>
  isUpgrade ? ("yearly" as Interval) : interval;

// -------- invoice helpers ----------
type CounterDoc = { _id: string; seq: number };

async function nextInvoiceSequence(): Promise<number> {
  const coll = db.collection<CounterDoc>("counters");
  const options: any = {
    upsert: true,
    returnDocument: "after",
    returnOriginal: false,
  };
  const result: any = await coll.findOneAndUpdate(
    { _id: "invoice_seq" },
    { $inc: { seq: 1 } as any },
    options
  );
  const doc: CounterDoc | null = result?.value ?? result ?? null;
  return typeof doc?.seq === "number" ? doc.seq : 1;
}

async function generateInvoiceNo(): Promise<string> {
  const now = new Date();
  const y = now.getFullYear();
  const m = String(now.getMonth() + 1).padStart(2, "0");
  const d = String(now.getDate()).padStart(2, "0");
  const seq = await nextInvoiceSequence();
  return `UPH-${y}${m}${d}-${String(seq).padStart(4, "0")}`;
}

function productDisplayName(
  product: any,
  interval?: Interval,
  variant?: any,
  isBundle?: boolean
) {
  if (isBundle) {
    return `Trader Essentials Bundle (5-in-1)${
      interval === "yearly" ? " – Yearly" : " – Monthly"
    }`;
  }
  if (product?.key === ALGO_SKU_KEY && variant) {
    return `${product.name} - ${variant.name}${
      interval ? ` (${interval === "yearly" ? "Yearly" : "Monthly"})` : ""
    }`;
  }
  if (product?.key === JOURNALING_SOLO_SKU_KEY && interval) {
    return `${product.name} (${interval === "yearly" ? "Yearly" : "Monthly"})`;
  }
  return product?.name || "Product";
}

/* ===================== controller: createOrder (UPDATED for renew) ===================== */
export const createOrder: RequestHandler = async (req, res): Promise<void> => {
  try {
    const {
      signupIntentId,
      productId,
      variantId,
      billingInterval,
      brokerConfig,
      renew, // <—— NEW
    } = (req.body ?? {}) as {
      signupIntentId?: string;
      productId?: string;
      variantId?: string;
      billingInterval?: Interval;
      brokerConfig?: Record<string, any>;
      renew?: boolean;
    };

    // ----- Guest flow (register-intent) -----
    if (signupIntentId) {
      let signupObjectId: ObjectId;
      try {
        signupObjectId = new ObjectId(signupIntentId);
      } catch {
        res.status(400).json({ message: "Invalid signupIntentId" });
        return;
      }

      const intent = await db.collection("signup_intents").findOne({ _id: signupObjectId });
      if (!intent) {
        res.status(404).json({ message: "Signup intent not found" });
        return;
      }
      if ((intent as any).status !== "created") {
        res.status(400).json({ message: "Signup intent not in a payable state" });
        return;
      }
      if (!(intent as any).productId) {
        res.status(400).json({ message: "No product selected for payment" });
        return;
      }

      const product = await db.collection("products")
        .findOne({ _id: (intent as any).productId, isActive: true });
      if (!product) {
        res.status(400).json({ message: "Invalid product" });
        return;
      }

      const interval: Interval = ((intent as any).billingInterval as Interval) || "monthly";
      const productKey = (product as any).key as string;

      let variantDoc: any = null;
      if (productKey === ALGO_SKU_KEY) {
        if (!(intent as any).variantId) {
          res.status(400).json({ message: "variantId required for this product" });
          return;
        }
        variantDoc = await db.collection("product_variants").findOne({
          _id: (intent as any).variantId,
          productId: (product as any)._id,
          isActive: true,
        });
        if (!variantDoc) {
          res.status(400).json({ message: "Invalid or inactive variant" });
          return;
        }
      }

      const { amountPaise, displayName } = await resolvePriceForProduct(product, interval, variantDoc);

      if (amountPaise === 0) {
        res.status(204).send();
        return;
      }

      const order = await razorpay.orders.create({
        amount: amountPaise,
        currency: process.env.CURRENCY || "INR",
        receipt: `rcpt_${Date.now()}`,
        notes: { signupIntentId },
      });

      const amountRupeesGuest = Number(order.amount) / 100;
      const paymentIntent = await db.collection("payment_intents").insertOne({
        signupIntentId: signupObjectId,
        orderId: order.id,
        amount: amountRupeesGuest,
        currency: order.currency,
        status: "created",
        createdAt: new Date(),
        updatedAt: new Date(),
      });

      res.json({
        orderId: order.id,
        amount: order.amount,
        currency: order.currency,
        key: process.env.RAZORPAY_KEY_ID,
        name: "UpholicTech",
        description: displayName,
        intentId: paymentIntent.insertedId,
        user: {
          name: (intent as any).name,
          email: (intent as any).email,
          contact: (intent as any).phone,
        },
      });
      return;
    }

    // ----- Logged-in direct purchase / renewal -----
    const authUserId = getUserIdFromReqOrBearer(req);
    if (!authUserId) {
      res.status(401).json({ message: "Authentication required" });
      return;
    }
    if (!productId) {
      res.status(400).json({ message: "productId is required" });
      return;
    }

    let productObjectId: ObjectId;
    try {
      productObjectId = new ObjectId(productId);
    } catch {
      res.status(400).json({ message: "Invalid productId" });
      return;
    }

    const product = await db.collection("products").findOne({ _id: productObjectId, isActive: true });
    if (!product) {
      res.status(400).json({ message: "Invalid product" });
      return;
    }

    const productKey = (product as any).key as string;
    const interval: Interval =
      (billingInterval as Interval) ||
      (productKey === ALGO_SKU_KEY ? "monthly" : "monthly");

    let variantDoc: any = null;
    let isUpgrade = false;

    // === Duplicate / Upgrade / Renew checks ===
    if (productKey === ALGO_SKU_KEY) {
      if (!variantId) {
        res.status(400).json({ message: "variantId required for this product" });
        return;
      }
      let variantObjectId: ObjectId;
      try {
        variantObjectId = new ObjectId(variantId);
      } catch {
        res.status(400).json({ message: "Invalid variantId" });
        return;
      }

      variantDoc = await db.collection("product_variants").findOne({
        _id: variantObjectId,
        productId: (product as any)._id,
        isActive: true,
      });
      if (!variantDoc) {
        res.status(400).json({ message: "Invalid or inactive variant" });
        return;
      }

      const activeAlgo = await findActiveEntitlement(authUserId, (product as any)._id, variantObjectId);
      if (activeAlgo) {
        if (renew) {
          const d = daysUntil((activeAlgo as any).endsAt || null);
          if (!(Number.isFinite(d) && d >= 0 && d <= RENEW_WINDOW_DAYS)) {
            res.status(409).json({ message: `Renewal allowed only within ${RENEW_WINDOW_DAYS} days of expiry` });
            return;
          }
        } else {
          res.status(409).json({ message: "You already have an active ALGO Simulator plan for this variant" });
          return;
        }
      }
    } else if (productKey === BUNDLE_SKU_KEY) {
      const activeBundleComps = await findActiveBundleComponentEntitlements(authUserId);
      if (activeBundleComps.length > 0) {
        const anyMonthly = activeBundleComps.some((r: any) => r?.meta?.interval === "monthly");
        if (renew) {
          const soonest = Math.min(
            ...activeBundleComps.map((r: any) => daysUntil(r?.endsAt || null))
          );
          if (Number.isFinite(soonest) && soonest >= 0 && soonest <= RENEW_WINDOW_DAYS) {
            // allow renewal
          } else if (interval === "yearly" && anyMonthly) {
            isUpgrade = true; // allow monthly -> yearly upgrade
          } else {
            res.status(409).json({ message: `Bundle is already active; renewal only within ${RENEW_WINDOW_DAYS} days of expiry` });
            return;
          }
        } else if (interval === "yearly" && anyMonthly) {
          isUpgrade = true;
        } else {
          res.status(409).json({ message: "You already have active Bundle access" });
          return;
        }
      }
    } else if (productKey === JOURNALING_SOLO_SKU_KEY) {
      const journalingProd = await db.collection("products").findOne({ key: "journaling", isActive: true });
      if (journalingProd) {
        const now = new Date();
        const hasBundleJ = await db.collection("user_products").findOne({
          userId: authUserId,
          productId: (journalingProd as any)._id,
          status: "active",
          $and: [
            variantFilterExpr(null),
            endsActiveExpr(now),
            { "meta.source": "payment_bundle" },
          ],
        });
        if (hasBundleJ) {
          res.status(409).json({
            message: "Your Bundle already includes Journaling. No need to buy Journaling (Solo).",
          });
          return;
        }
      }

      const activeSolo = await findActiveEntitlement(authUserId, (product as any)._id, null);
      if (activeSolo) {
        const existingInterval = (activeSolo as any)?.meta?.interval || "monthly";
        if (existingInterval === "yearly") {
          res.status(409).json({ message: "You already have an active Journaling (Solo) – Yearly" });
          return;
        }
        if (interval === "yearly" && existingInterval === "monthly") {
          isUpgrade = true;
        } else if (renew) {
          const d = daysUntil((activeSolo as any).endsAt || null);
          if (!(Number.isFinite(d) && d >= 0 && d <= RENEW_WINDOW_DAYS)) {
            res.status(409).json({ message: `Renewal allowed only within ${RENEW_WINDOW_DAYS} days of expiry` });
            return;
          }
        } else {
          res.status(409).json({ message: "You already have an active Journaling (Solo)" });
          return;
        }
      }
    } else {
      res.status(400).json({ message: "This product is not purchasable" });
      return;
    }

    const { amountPaise, displayName } = await resolvePriceForProduct(product, interval, variantDoc);

    if (amountPaise === 0) {
      res.status(204).send();
      return;
    }

    const u = await db.collection("users").findOne({ _id: authUserId });

    const order = await razorpay.orders.create({
      amount: amountPaise,
      currency: process.env.CURRENCY || "INR",
      receipt: `rcpt_${Date.now()}`,
      notes: {
        purchase: "direct",
        productId,
        variantId: variantId || "",
        userId: authUserId.toString(),
        interval,
        ...(isUpgrade ? { upgradeTo: "yearly" } : {}),
        ...(renew ? { renew: "1" } : {}),
      },
    });

    const amountRupeesDirect = Number(order.amount) / 100;
    const paymentIntent = await db.collection("payment_intents").insertOne({
      orderId: order.id,
      amount: amountRupeesDirect,
      currency: order.currency,
      status: "created",
      createdAt: new Date(),
      updatedAt: new Date(),
      purchase: {
        userId: authUserId,
        productId: productObjectId,
        variantId: variantId ? new ObjectId(variantId) : null,
        interval,
        brokerConfig: brokerConfig || null,
        ...(isUpgrade ? { upgradeTo: "yearly" } : {}),
        ...(renew ? { renew: true } : {}), // <—— remember renewal intent
      },
    });

    res.json({
      orderId: order.id,
      amount: order.amount,
      currency: order.currency,
      key: process.env.RAZORPAY_KEY_ID,
      name: "UpholicTech",
      description: displayName,
      intentId: paymentIntent.insertedId,
      user: {
        name: (u as any)?.name,
        email: (u as any)?.email,
        contact: (u as any)?.phone,
      },
    });
    return;
  } catch (err: any) {
    const status = err?.status || 500;
    console.error("createOrder error:", {
      name: err?.name,
      message: err?.message,
      stack: err?.stack?.split("\n").slice(0, 2).join("\n"),
    });
    res.status(status).json({ message: err?.message || "Failed to create order" });
    return;
  }
};

/* ===================== controller: verifyPayment (UPDATED to extend from current endsAt on renew) ===================== */
export const verifyPayment: RequestHandler = async (req, res): Promise<void> => {
  const now = new Date();

  const {
    razorpay_order_id,
    razorpay_payment_id,
    razorpay_signature,
    intentId,
  } = (req.body ?? {}) as {
    razorpay_order_id?: string;
    razorpay_payment_id?: string;
    razorpay_signature?: string;
    intentId?: string;
  };

  if (!razorpay_order_id || !razorpay_payment_id || !razorpay_signature || !intentId) {
    res.status(400).json({ message: "Missing payment verification fields" });
    return;
  }

  const keySecret = process.env.RAZORPAY_KEY_SECRET;
  if (!keySecret || !keySecret.trim()) {
    res.status(500).json({ message: "Server misconfiguration: RAZORPAY_KEY_SECRET not set" });
    return;
  }

  let intentObjectId: ObjectId;
  try {
    intentObjectId = new ObjectId(intentId);
  } catch {
    res.status(400).json({ message: "Invalid intentId" });
    return;
  }

  try {
    const pIntent = await db.collection("payment_intents").findOne({ _id: intentObjectId });
    if (!pIntent) {
      res.status(404).json({ message: "Payment intent not found" });
      return;
    }

    if ((pIntent as any).orderId && (pIntent as any).orderId !== razorpay_order_id) {
      await db.collection("payment_intents").updateOne(
        { _id: intentObjectId },
        {
          $set: {
            status: "failed",
            updatedAt: now,
            razorpay_order_id,
            razorpay_payment_id,
            razorpay_signature,
            failureReason: "order_id_mismatch",
          },
        }
      );
      res.status(400).json({ message: "Order ID mismatch for this intent" });
      return;
    }

    const expected = crypto
      .createHmac("sha256", keySecret)
      .update(`${razorpay_order_id}|${razorpay_payment_id}`)
      .digest("hex");

    if (expected !== razorpay_signature) {
      await db.collection("payment_intents").updateOne(
        { _id: intentObjectId },
        {
          $set: {
            status: "failed",
            updatedAt: now,
            razorpay_order_id,
            razorpay_payment_id,
            razorpay_signature,
            failureReason: "invalid_signature",
          },
        }
      );
      res.status(400).json({ message: "Invalid signature" });
      return;
    }

    const paidAmountRupees = Number((pIntent as any).amount) || 0;

    // ===== FLOW A: Guest – finalize signup (unchanged) =====
    if ((pIntent as any).signupIntentId) {
      // ... unchanged guest flow from your original code ...
      // (keeping your existing logic exactly as-is)
      // BEGIN original block
      const signupId: ObjectId | undefined = (pIntent as any).signupIntentId;
      if (!signupId || !(signupId instanceof ObjectId)) {
        res.status(400).json({ message: "Corrupt payment intent: missing signupIntentId" });
        return;
      }

      const sIntent = await db.collection("signup_intents").findOne({ _id: signupId });
      if (!sIntent) {
        res.status(404).json({ message: "Signup intent not found" });
        return;
      }
      if ((sIntent as any).status !== "created") {
        res.status(400).json({ message: "Signup intent already finalized" });
        return;
      }

      const existing = await db.collection("users").findOne({
        $or: [{ email: (sIntent as any).email }, { phone: (sIntent as any).phone }],
      });

      let userId: ObjectId;
      if (existing) {
        userId = (existing as any)._id;
      } else {
        const ins = await db.collection("users").insertOne({
          name: (sIntent as any).name,
          email: (sIntent as any).email,
          phone: (sIntent as any).phone,
          password: (sIntent as any).passwordHash ?? (sIntent as any).password ?? null,
          role: "customer",
          createdAt: now,
          updatedAt: now,
        });
        userId = ins.insertedId;
      }

      const sProduct = await db.collection("products").findOne({ _id: (sIntent as any).productId });
      if (!sProduct) {
        res.status(400).json({ message: "Invalid product on signup intent" });
        return;
      }
      const sProductKey: string | undefined = (sProduct as any)?.key;
      if (!sProductKey) {
        res.status(400).json({ message: "This product is not purchasable" });
        return;
      }

      const interval: Interval = (((sIntent as any).billingInterval as Interval) ?? "monthly");
      const newEndsAt = computeEndsAt(interval, now);

      if (sProductKey === BUNDLE_SKU_KEY) {
        const componentKeys = Array.from(await getBundleComponentsSet());
        const bundleProducts = await db.collection("products")
          .find({ key: { $in: componentKeys }, isActive: true }).toArray();

        for (const bp of bundleProducts) {
          await db.collection("user_products").updateOne(
            { userId, productId: (bp as any)._id, variantId: null },
            {
              $setOnInsert: { startedAt: now },
              $set: {
                status: "active",
                endsAt: newEndsAt,
                lastPaymentAt: now,
                "meta.source": "payment_bundle",
                "meta.interval": interval,
                paymentMeta: {
                  provider: "razorpay",
                  orderId: razorpay_order_id,
                  paymentId: razorpay_payment_id,
                  amount: paidAmountRupees,
                  currency: (pIntent as any).currency,
                },
              },
            },
            { upsert: true }
          );
        }
        await cancelActiveJournalingSolo(userId, "bundle_purchase");
      } else if (sProductKey === ALGO_SKU_KEY) {
        const ends = computeEndsAt("monthly", now);
        await db.collection("user_products").updateOne(
          {
            userId,
            productId: (sIntent as any).productId,
            variantId: (sIntent as any).variantId || null,
          },
          {
            $setOnInsert: { startedAt: now },
            $set: {
              status: "active",
              endsAt: ends,
              lastPaymentAt: now,
              "meta.source": "payment",
              "meta.interval": "monthly",
              paymentMeta: {
                provider: "razorpay",
                orderId: razorpay_order_id,
                paymentId: razorpay_payment_id,
                amount: paidAmountRupees,
                currency: (pIntent as any).currency,
              },
            },
          },
          { upsert: true }
        );

        if ((sIntent as any).variantId && (sIntent as any).brokerConfig) {
          await db.collection("broker_configs").insertOne({
            userId,
            productId: (sIntent as any).productId,
            variantId: (sIntent as any).variantId,
            brokerName: (sIntent as any).brokerConfig?.brokerName,
            createdAt: now,
            updatedAt: now,
            ...((sIntent as any).brokerConfig || {}),
          });
        }
      } else if (sProductKey === JOURNALING_SOLO_SKU_KEY) {
        await db.collection("user_products").updateOne(
          { userId, productId: (sIntent as any).productId, variantId: null },
          {
            $setOnInsert: { startedAt: now },
            $set: {
              status: "active",
              endsAt: newEndsAt,
              lastPaymentAt: now,
              "meta.source": "payment",
              "meta.interval": interval,
              paymentMeta: {
                provider: "razorpay",
                orderId: razorpay_order_id,
                paymentId: razorpay_payment_id,
                amount: paidAmountRupees,
                currency: (pIntent as any).currency,
              },
            },
          },
          { upsert: true }
        );
      } else {
        res.status(400).json({ message: "This product is not purchasable" });
        return;
      }

      await db.collection("signup_intents").updateOne(
        { _id: signupId },
        { $set: { status: "completed", userId, updatedAt: now } }
      );
      await db.collection("payment_intents").updateOne(
        { _id: intentObjectId },
        {
          $set: {
            status: "paid",
            updatedAt: now,
            razorpay_order_id,
            razorpay_payment_id,
            razorpay_signature,
          },
        }
      );

      // ---- build + send invoice email (GUEST) ----
      try {
        const invoiceNo = await generateInvoiceNo();
        const variantForName =
          (sIntent as any).variantId
            ? await db.collection("product_variants").findOne({ _id: (sIntent as any).variantId })
            : undefined;
        const productName = productDisplayName(
          sProduct,
          interval,
          variantForName,
          sProductKey === BUNDLE_SKU_KEY
        );

        const payload: InvoicePayload = {
          invoiceNo,
          invoiceDate: now.toISOString().slice(0, 10),
          billTo: {
            name: (sIntent as any).name,
            email: (sIntent as any).email,
            phone: (sIntent as any).phone,
          },
          items: [
            {
              name: productName,
              qty: 1,
              rate: paidAmountRupees,
              gst: 18,
              inclusive: true,
            },
          ],
          gstInclusive: true,
        };
        const to = (sIntent as any).email;
        if (to) {
          await sendInvoiceEmail(to, payload);
        }
        await db.collection("invoices").insertOne({
          invoiceNo,
          userId,
          intentId,
          orderId: razorpay_order_id,
          paymentId: razorpay_payment_id,
          amount: paidAmountRupees,
          currency: (pIntent as any).currency,
          payload,
          createdAt: now,
        });
      } catch (e) {
        console.warn("[invoice] email failed (guest):", (e as any)?.message || e);
      }

      const token = generateToken(userId.toString());
      const u = await db.collection("users").findOne({ _id: userId });
      res.json({
        success: true,
        token,
        user: {
          id: userId,
          name: (u as any)?.name,
          email: (u as any)?.email,
          phone: (u as any)?.phone,
        },
      });
      return;
      // END original block
    }

    // ===== FLOW B: Direct purchase / renewal =====
    const purchase = (pIntent as any).purchase;
    if (!purchase) {
      res.status(400).json({ message: "Invalid payment intent" });
      return;
    }

    const userId: ObjectId = purchase.userId;
    const prodId: ObjectId = purchase.productId;
    const varId: ObjectId | null = purchase.variantId || null;
    const interval: Interval = purchase.interval || "monthly";
    const brokerConfig = purchase.brokerConfig || null;
    const isUpgrade = purchase.upgradeTo === "yearly";
    const isRenew = !!purchase.renew; // <—— NEW

    const product = await db.collection("products").findOne({ _id: prodId });
    if (!product) {
      res.status(400).json({ message: "Invalid product" });
      return;
    }

    const pKey: string | undefined = (product as any)?.key;
    if (!pKey) {
      res.status(400).json({ message: "This product is not purchasable" });
      return;
    }

    if (pKey === BUNDLE_SKU_KEY) {
      const componentKeys = Array.from(await getBundleComponentsSet());
      const bundleProducts = await db.collection("products")
        .find({ key: { $in: componentKeys }, isActive: true }).toArray();

      for (const bp of bundleProducts) {
        // extend from existing endsAt if renewal and still in future, else from now
        const existing = await db.collection("user_products").findOne({
          userId,
          productId: (bp as any)._id,
          variantId: null,
        });

        const currentEnds =
          existing?.endsAt ? new Date(existing.endsAt) : null;

        const base =
          isRenew && currentEnds && currentEnds.getTime() > now.getTime()
            ? currentEnds
            : now;

        const endsAt = computeEndsAt(intervalToSet(interval, isUpgrade), base);

        await db.collection("user_products").updateOne(
          { userId, productId: (bp as any)._id, variantId: null },
          {
            $setOnInsert: { startedAt: now },
            $set: {
              status: "active",
              endsAt,
              lastPaymentAt: now,
              "meta.source": "payment_bundle",
              "meta.interval": intervalToSet(interval, isUpgrade),
              paymentMeta: {
                provider: "razorpay",
                orderId: razorpay_order_id,
                paymentId: razorpay_payment_id,
                amount: paidAmountRupees,
                currency: (pIntent as any).currency,
              },
            },
          },
          { upsert: true }
        );
      }
      await cancelActiveJournalingSolo(userId, "bundle_purchase");
    } else if (pKey === ALGO_SKU_KEY) {
      // ALGO is monthly only; extend from current if renewing
      const existing = await db.collection("user_products").findOne({
        userId,
        productId: prodId,
        variantId: varId,
      });
      const currentEnds = existing?.endsAt ? new Date(existing.endsAt) : null;
      const base =
        isRenew && currentEnds && currentEnds.getTime() > now.getTime()
          ? currentEnds
          : now;
      const ends = computeEndsAt("monthly", base);

      await db.collection("user_products").updateOne(
        { userId, productId: prodId, variantId: varId },
        {
          $setOnInsert: { startedAt: now },
          $set: {
            status: "active",
            endsAt: ends,
            lastPaymentAt: now,
            "meta.source": "payment",
            "meta.interval": "monthly",
            paymentMeta: {
              provider: "razorpay",
              orderId: razorpay_order_id,
              paymentId: razorpay_payment_id,
              amount: paidAmountRupees,
              currency: (pIntent as any).currency,
            },
          },
        },
        { upsert: true }
      );

      if (varId && brokerConfig) {
        await db.collection("broker_configs").insertOne({
          userId,
          productId: prodId,
          variantId: varId,
          brokerName: brokerConfig?.brokerName,
          createdAt: now,
          updatedAt: now,
          ...(brokerConfig || {}),
        });
      }
    } else if (pKey === JOURNALING_SOLO_SKU_KEY) {
      const existing = await db.collection("user_products").findOne({
        userId,
        productId: prodId,
        variantId: null,
      });
      const currentEnds = existing?.endsAt ? new Date(existing.endsAt) : null;
      const base =
        isRenew && currentEnds && currentEnds.getTime() > now.getTime()
          ? currentEnds
          : now;
      const endsAt = computeEndsAt(intervalToSet(interval, isUpgrade), base);

      await db.collection("user_products").updateOne(
        { userId, productId: prodId, variantId: null },
        {
          $setOnInsert: { startedAt: now },
          $set: {
            status: "active",
            endsAt,
            lastPaymentAt: now,
            "meta.source": "payment",
            "meta.interval": intervalToSet(interval, isUpgrade),
            paymentMeta: {
              provider: "razorpay",
              orderId: razorpay_order_id,
              paymentId: razorpay_payment_id,
              amount: paidAmountRupees,
              currency: (pIntent as any).currency,
            },
          },
        },
        { upsert: true }
      );
    } else {
      res.status(400).json({ message: "This product is not purchasable" });
      return;
    }

    await db.collection("payment_intents").updateOne(
      { _id: intentObjectId },
      {
        $set: {
          status: "paid",
          updatedAt: now,
          razorpay_order_id,
          razorpay_payment_id,
          razorpay_signature,
        },
      }
    );

    // ---- build + send invoice email (DIRECT) ----
    try {
      const user = await db.collection("users").findOne({ _id: userId });
      const to = (user as any)?.email;
      const invoiceNo = await generateInvoiceNo();

      const variantDoc = varId
        ? await db.collection("product_variants").findOne({ _id: varId })
        : null;
      const name = productDisplayName(product, interval, variantDoc, pKey === BUNDLE_SKU_KEY);

      const payload: InvoicePayload = {
        invoiceNo,
        invoiceDate: now.toISOString().slice(0, 10),
        billTo: {
          name: (user as any)?.name,
          email: (user as any)?.email,
          phone: (user as any)?.phone,
        },
        items: [
          { name, qty: 1, rate: paidAmountRupees, gst: 18, inclusive: true },
        ],
        gstInclusive: true,
      };

      if (to) {
        await sendInvoiceEmail(to, payload);
      }
      await db.collection("invoices").insertOne({
        invoiceNo,
        userId,
        intentId,
        orderId: razorpay_order_id,
        paymentId: razorpay_payment_id,
        amount: paidAmountRupees,
        currency: (pIntent as any).currency,
        payload,
        createdAt: now,
      });
    } catch (e) {
      console.warn("[invoice] email failed (direct):", (e as any)?.message || e);
    }

    res.json({ success: true });
    return;
  } catch (err: any) {
    const simplified = {
      name: err?.name,
      code: (err as any)?.code,
      message: err?.message,
      stackTop: err?.stack?.split("\n").slice(0, 3).join("\n"),
    };
    console.error("verifyPayment fatal:", simplified);
    if ((err as any)?.code === 11000) {
      res.status(409).json({ message: "Duplicate resource conflict while finalizing payment" });
      return;
    }
    res.status(500).json({ message: "Verification failed" });
    return;
  }
};
