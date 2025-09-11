// server/src/middleware/requireEntitlement.middleware.ts
import { RequestHandler } from "express";
import { Db, ObjectId } from "mongodb";
import jwt from "jsonwebtoken";

let db: Db | null = null;
export const setRequireEntitlementDb = (database: Db) => {
  db = database;
};

const getUserIdFromReq = (req: any): ObjectId | null => {
  if (req.user?.id && ObjectId.isValid(req.user.id)) return new ObjectId(req.user.id);

  const auth = req.headers.authorization;
  if (auth?.startsWith("Bearer ") && process.env.JWT_SECRET) {
    try {
      const decoded: any = jwt.verify(auth.split(" ")[1], process.env.JWT_SECRET);
      if (decoded?.id && ObjectId.isValid(decoded.id)) return new ObjectId(decoded.id);
    } catch {}
  }
  return null;
};

/**
 * Blocks unless the user has at least one of the given product keys active
 * (status=active and endsAt null or in the future).
 */
export const requireEntitlement = (...anyOfKeys: string[]): RequestHandler => {
  return async (req, res, next) => {
    try {
      if (!db) throw new Error("[requireEntitlement] DB not initialized");

      const userId = getUserIdFromReq(req);
      if (!userId) {
        res.status(401).json({ message: "Not authorized" });
        return;
      }

      const now = new Date();

      // Find products with those keys
      const products = await db
        .collection("products")
        .find({ key: { $in: anyOfKeys }, isActive: true })
        .project({ _id: 1, key: 1 })
        .toArray();

      if (!products.length) {
        res.status(403).json({ message: "Access denied: product not available" });
        return;
      }

      const productIds = products.map((p: any) => p._id as ObjectId);

      // Active entitlements for those productIds
      const ent = await db.collection("user_products").findOne({
        userId,
        productId: { $in: productIds },
        status: "active",
        $or: [{ endsAt: null }, { endsAt: { $gt: now } }],
      });

      if (!ent) {
        res.status(403).json({ message: "Access denied: no active entitlement" });
        return;
      }

      next();
    } catch (err) {
      console.error("[requireEntitlement] error:", err);
      res.status(500).json({ message: "Server error" });
    }
  };
};
