// server/src/scripts/seedProducts.ts
import path from "path";
import dotenv from "dotenv";
import crypto from "crypto";
import { MongoClient, ObjectId } from "mongodb";

const envPath = path.resolve(__dirname, "../../.env");
dotenv.config({ path: envPath });

// Deterministic ObjectId from a string key
const oidFrom = (ns: string) => {
  const hex = crypto.createHash("md5").update(ns).digest("hex").slice(0, 24);
  return new ObjectId(hex);
};

(async () => {
  const uri = process.env.MONGO_URI;
  const dbName = process.env.MONGO_DB_NAME;
  if (!uri || !dbName) {
    console.error("Loaded .env from:", envPath);
    console.error("MONGO_URI =", process.env.MONGO_URI);
    console.error("MONGO_DB_NAME =", process.env.MONGO_DB_NAME);
    throw new Error("❌ MONGO_URI or MONGO_DB_NAME is not defined in .env");
  }

  const client = new MongoClient(uri);

  try {
    // console.log("⏳ Connecting to MongoDB…", uri);
    await client.connect();
    const db = client.db(dbName);
    // console.log(`✅ Connected to MongoDB (${db.databaseName})`);

    // ===== Canonical product list with STABLE _id =====
    const P = (key: string) => oidFrom(`product:${key}`);
    const V = (productKey: string, variantKey: string) =>
      oidFrom(`variant:${productKey}:${variantKey}`);

    const products = [
      // BUNDLE → monthly + yearly
      {
        _id: P("essentials_bundle"),
        key: "essentials_bundle",
        name: "Trader's Essential Bundle (2-in-1)",
        isActive: true,
        hasVariants: false,
        forSale: true,
        route: "/dashboard",
        priceMonthly: Number(process.env.BUNDLE_MONTHLY_PRICE ?? 499),   //499
        priceYearly: Number(process.env.BUNDLE_YEARLY_PRICE ?? 4999),    //4999
        components: [
          // "technical_scanner",
          // "fundamental_scanner",
          // "fno_khazana",
          "journaling",
          "fii_dii_data",
        ],
      },

      // Non-sale components (not purchasable individually)
      // {
      //   _id: P("technical_scanner"),
      //   key: "technical_scanner",
      //   name: "Technical Scanner",
      //   isActive: true,
      //   hasVariants: false,
      //   forSale: false,
      //   route: "/technical",
      // },
      // {
      //   _id: P("fundamental_scanner"),
      //   key: "fundamental_scanner",
      //   name: "Fundamental Scanner",
      //   isActive: true,
      //   hasVariants: false,
      //   forSale: false,
      //   route: "/fundamental",
      // },
      // {
      //   _id: P("fno_khazana"),
      //   key: "fno_khazana",
      //   name: "FNO Khazana",
      //   isActive: true,
      //   hasVariants: false,
      //   forSale: false,
      //   route: "/fno",
      // },
      {
        _id: P("journaling"),
        key: "journaling",
        name: "TradeKhata",
        isActive: true,
        hasVariants: false,
        forSale: false,
        route: "/journal",
      },
      {
        _id: P("fii_dii_data"),
        key: "fii_dii_data",
        name: "FIIs/DIIs Data",
        isActive: true,
        hasVariants: false,
        forSale: false,
        route: "/main-fii-dii",
      },

      // ALGO → variants (monthly only)
      {
        _id: P("algo_simulator"),
        key: "algo_simulator",
        name: "ALGO Simulator",
        isActive: true,
        hasVariants: true,
        forSale: true,
        route: "/dashboard",
      },

      // TradeKhata → monthly + yearly
      {
        _id: P("journaling_solo"),
        key: "journaling_solo",
        name: "TradeKhata",
        isActive: true,
        hasVariants: false,
        forSale: true,
        route: "/journal",
        priceMonthly: Number(process.env.JOURNALING_SOLO_MONTHLY_PRICE ?? 299),    //299
        priceYearly: Number(process.env.JOURNALING_SOLO_YEARLY_PRICE ?? 2499),      //2499
      },
    ];

    // Upsert products by _id (stable)
    await db.collection("products").bulkWrite(
      products.map((doc) => ({
        replaceOne: {
          filter: { _id: doc._id },
          replacement: doc,
          upsert: true,
        },
      })),
      { ordered: false }
    );

    // Remove any old products not in our canonical list
    const keepProductIds = products.map((p) => p._id);
    await db.collection("products").deleteMany({ _id: { $nin: keepProductIds } });

    // ===== Variants for ALGO (stable IDs, monthly only) =====
    const algoId = P("algo_simulator");

    const variants = [
      {
        _id: V("algo_simulator", "starter"),
        productId: algoId,
        key: "starter",
        name: "Starter Scalping",
        description: "Beginner-friendly scalping suite",
        priceMonthly: 5999,  //5999
        interval: "monthly",
        isActive: true,
      },
      {
        _id: V("algo_simulator", "pro"),
        productId: algoId,
        key: "pro",
        name: "Option Scalper PRO",
        description: "Advanced option scalping engine",
        priceMonthly: 14999,   //14999
        interval: "monthly",
        isActive: true,
      },
      {
        _id: V("algo_simulator", "swing"),
        productId: algoId,
        key: "swing",
        name: "Sniper Algo",
        description: "Sniper trading strategy system",
        priceMonthly: 9999,   //9999
        interval: "monthly",
        isActive: true,
      },
    ];

    await db.collection("product_variants").bulkWrite(
      variants.map((doc) => ({
        replaceOne: {
          filter: { _id: doc._id },
          replacement: doc,
          upsert: true,
        },
      })),
      { ordered: false }
    );

    // Remove any old variants for algo_simulator not in the canonical list
    const keepVariantIds = variants.map((v) => v._id);
    await db
      .collection("product_variants")
      .deleteMany({ productId: algoId, _id: { $nin: keepVariantIds } });

    // ----- Helpful indexes (safe to re-run) -----
    await db.collection("users").createIndex({ email: 1 }, { unique: true });
    await db.collection("products").createIndex({ key: 1 }, { unique: true });
    await db.collection("products").createIndex({ forSale: 1, isActive: 1 });
    await db
      .collection("product_variants")
      .createIndex({ productId: 1, key: 1 }, { unique: true });
    await db
      .collection("user_products")
      .createIndex({ userId: 1, productId: 1, variantId: 1 }, { unique: true });
    await db
      .collection("broker_configs")
      .createIndex({ userId: 1, productId: 1, variantId: 1 }, { unique: true });

    // Summary
    const productsCount = await db.collection("products").countDocuments();
    const variantsCount = await db
      .collection("product_variants")
      .countDocuments({ productId: algoId });

    console.log(`📦 products count: ${productsCount}`);
    console.log(`🧩 ALGO variants count: ${variantsCount}`);
    console.log("✅ Seed complete (stable IDs + yearly pricing).");
  } catch (err) {
    console.error("❌ Seeding failed:", err);
    process.exitCode = 1;
  } finally {
    await client.close();
    console.log("🔌 MongoDB connection closed.");
  }
})();
