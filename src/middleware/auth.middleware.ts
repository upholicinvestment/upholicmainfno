import { RequestHandler } from "express";
import jwt from "jsonwebtoken";
import type { Db, ObjectId } from "mongodb";
import { ObjectId as _ObjectId } from "mongodb";

// optional DB handle (native driver). We'll inject it from appnew.ts
let authDb: Db | null = null;
export function setAuthDb(db: Db) {
  authDb = db;
}

declare global {
  namespace Express {
    interface Request {
      user?: {
        id: string;
        role: string;
      };
    }
  }
}

export const authenticate: RequestHandler = async (req, res, next) => {
  if (process.env.PUBLIC_MODE === "true") {
    return next();
  }

  let token: string | undefined;
  if (req.headers.authorization?.startsWith("Bearer ")) {
    token = req.headers.authorization.split(" ")[1];
  }

  if (!token) {
    res.status(401).json({ message: "Not authorized, no token provided" });
    return;
  }

  try {
    const decoded = jwt.verify(token, process.env.JWT_SECRET!) as any;

    // Accept common shapes: { id }, {_id}, { userId }, nested { user: {...} }
    const uid: string | undefined =
      decoded?.id ??
      decoded?._id ??
      decoded?.userId ??
      decoded?.user?.id ??
      decoded?.user?._id ??
      decoded?.user?.userId;

    if (!uid) {
      res.status(401).json({ message: "Not authorized, malformed token" });
      return;
    }

    // role from token if present; default to "user"
    let role: string = decoded?.role ?? decoded?.user?.role ?? "user";

    // Try to enrich from DB if available (native driver)
    if (authDb) {
      try {
        const _id = _ObjectId.isValid(uid) ? new _ObjectId(uid) : (uid as any as ObjectId);
        const dbUser = await authDb.collection("users").findOne(
          { _id },
          { projection: { _id: 1, role: 1 } }
        );

        // If found, prefer DB role; otherwise continue with token defaults
        if (dbUser) {
          role = (dbUser as any).role || role;
        }
      } catch (e) {
        // don't fail auth just because the lookup errored
        // console.warn("Non-fatal user lookup error:", e);
      }
    }

    req.user = { id: String(uid), role };
    next();
  } catch (err) {
    console.error("Authentication error:", err);
    res.status(401).json({ message: "Not authorized, token failed" });
  }
};

export const authorize =
  (...roles: string[]): RequestHandler =>
  (req, res, next) => {
    if (process.env.PUBLIC_MODE === "true") return next();
    if (!req.user || !roles.includes(req.user.role)) {
      res.status(403).json({ message: "Not authorized to access this route" });
      return;
    }
    next();
  };
