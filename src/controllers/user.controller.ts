// server/src/controllers/user.controller.ts
import type { Request, Response, RequestHandler } from "express";
import { Db, ObjectId } from "mongodb";
import jwt from "jsonwebtoken";

let db: Db | undefined;

/** Inject DB once at bootstrap */
export const setUserDatabase = (database: Db) => {
  db = database;
};

const requireDb = (): Db => {
  if (!db) throw new Error("[user.controller] DB not initialized. Call setUserDatabase(db).");
  return db;
};

const toObjectId = (id: string | ObjectId): ObjectId => {
  if (id instanceof ObjectId) return id;
  if (!ObjectId.isValid(id)) throw new Error("Invalid ObjectId");
  return new ObjectId(id);
};

/** Resolve userId from multiple sources */
const getUserIdFromReq = (req: Request): ObjectId | null => {
  const u = (req as any).user;
  if (u?.id) {
    try { return toObjectId(u.id); } catch { /* ignore */ }
  }

  const auth = req.headers.authorization;
  if (auth?.startsWith("Bearer ") && process.env.JWT_SECRET) {
    const token = auth.split(" ")[1];
    try {
      const decoded = jwt.verify(token, process.env.JWT_SECRET) as { id?: string };
      if (decoded?.id) return toObjectId(decoded.id);
    } catch { /* invalid token -> fall through */ }
  }

  const hdrUserId = req.header("X-User-Id");
  if (hdrUserId && ObjectId.isValid(hdrUserId)) {
    return new ObjectId(hdrUserId);
  }

  const qpUserId = (req.query.userId as string) || "";
  if (qpUserId && ObjectId.isValid(qpUserId)) {
    return new ObjectId(qpUserId);
  }

  return null;
};

/** ---------- Avatars (frontend maps keys -> images in src/assets/avatars) ---------- */
export const AVATAR_KEYS = [
  "sienna",
  "analyst",
  "rose",
  "comet",
  "crimson",
  "prime",

] as const;
type AvatarKey = typeof AVATAR_KEYS[number];

/** GET /api/users/me/avatar-options */
export const getAvatarOptions: RequestHandler = async (_req, res) => {
  res.json({ avatars: AVATAR_KEYS });
};

/** GET /api/users/me */
export const getProfile: RequestHandler = async (req, res) => {
  try {
    const userId = getUserIdFromReq(req);
    if (!userId) {
      res.status(401).json({ message: "Not authorized" });
      return;
    }

    const _db = requireDb();
    const user = await _db.collection("users").findOne(
      { _id: userId },
      { projection: { password: 0 } }
    );

    if (!user) {
      res.status(404).json({ message: "User not found" });
      return;
    }

    res.json(user);
  } catch (err) {
    console.error("[users.getProfile] error:", err);
    res.status(500).json({ message: "Server error" });
  }
};

/** PUT /api/users/me
 * Accepts:
 * name?, email?, phone?, bio?, broker?, location?, avatarKey?
 * tradingStyle?, experienceYears?, riskProfile?, instruments?, timezone?
 * notifyAnnouncements?, notifyOrderAlerts?, notifyRenewals?, twoFactorEnabled?
 */
export const updateProfile: RequestHandler = async (req, res) => {
  try {
    const userId = getUserIdFromReq(req);
    if (!userId) {
      res.status(401).json({ message: "Not authorized" });
      return;
    }

    const body = (req.body ?? {}) as {
      name?: string;
      email?: string;
      phone?: string;
      bio?: string;
      broker?: string;
      location?: string;
      avatarKey?: AvatarKey;

      tradingStyle?: string;
      experienceYears?: string;
      riskProfile?: string;
      instruments?: string[];
      timezone?: string;

      notifyAnnouncements?: boolean;
      notifyOrderAlerts?: boolean;
      notifyRenewals?: boolean;
      twoFactorEnabled?: boolean;
    };

    if (!Object.keys(body).length) {
      res.status(400).json({ message: "Nothing to update" });
      return;
    }

    const _db = requireDb();
    const existing = await _db.collection("users").findOne({ _id: userId });
    if (!existing) {
      res.status(404).json({ message: "User not found" });
      return;
    }

    if (body.email && body.email !== (existing as any).email) {
      const dup = await _db.collection("users").findOne({ email: body.email });
      if (dup) {
        res.status(400).json({ message: "Email already in use" });
        return;
      }
    }

    if (body.avatarKey && !AVATAR_KEYS.includes(body.avatarKey)) {
      res.status(400).json({ message: "Invalid avatarKey" });
      return;
    }

    const $set: Record<string, any> = { updatedAt: new Date() };

    if (typeof body.name === "string") $set.name = body.name;
    if (typeof body.email === "string") $set.email = body.email;
    if (typeof body.phone === "string") $set.phone = body.phone;
    if (typeof body.bio === "string") $set.bio = body.bio;
    if (typeof body.broker === "string") $set.broker = body.broker;
    if (typeof body.location === "string") $set.location = body.location;
    if (typeof body.avatarKey === "string") $set.avatarKey = body.avatarKey;

    if (typeof body.tradingStyle === "string") $set.tradingStyle = body.tradingStyle;
    if (typeof body.experienceYears === "string") $set.experienceYears = body.experienceYears;
    if (typeof body.riskProfile === "string") $set.riskProfile = body.riskProfile;
    if (Array.isArray(body.instruments)) $set.instruments = body.instruments;
    if (typeof body.timezone === "string") $set.timezone = body.timezone;

    if (typeof body.notifyAnnouncements === "boolean") $set.notifyAnnouncements = body.notifyAnnouncements;
    if (typeof body.notifyOrderAlerts === "boolean") $set.notifyOrderAlerts = body.notifyOrderAlerts;
    if (typeof body.notifyRenewals === "boolean") $set.notifyRenewals = body.notifyRenewals;
    if (typeof body.twoFactorEnabled === "boolean") $set.twoFactorEnabled = body.twoFactorEnabled;

    await _db.collection("users").updateOne({ _id: userId }, { $set });

    const fresh = await _db
      .collection("users")
      .findOne({ _id: userId }, { projection: { password: 0 } });

    res.json(fresh);
  } catch (err) {
    console.error("[users.updateProfile] error:", err);
    res.status(500).json({ message: "Server error" });
  }
};

/** Shape returned by /users/me/products */
type OutVariant = {
  variantId: ObjectId;
  key: string;       // starter | pro | swing
  name: string;
  priceMonthly?: number | null;
  interval?: string | null;
};
type OutItem = {
  productId: ObjectId;
  key: string;
  name: string;
  route: string;
  hasVariants: boolean;
  forSale: boolean;
  status: string;
  startedAt: Date | null;
  endsAt: Date | null;
  meta?: any;
  /** Back-compat: first variant (if any) */
  variant: OutVariant | null;
  /** NEW: all owned variants for this product */
  variants?: OutVariant[];
};

/** GET /api/users/me/products
 * Returns active entitlements (status=active and (endsAt null or future)).
 * - DOES NOT filter by forSale (so bundle components like 'journaling' are returned)
 * - Hides 'journaling_solo' if 'journaling' (bundle component) exists
 * - Groups multiple entitlements of the same product into ONE item with variants[]
 * - Accepts ?debug=1 to include raw entitlements for troubleshooting
 */
export const getMyProducts: RequestHandler = async (req, res) => {
  try {
    const userId = getUserIdFromReq(req);
    if (!userId) {
      res.status(401).json({ message: "Not authorized" });
      return;
    }

    const debug = req.query.debug === "1";
    const _db = requireDb();
    const now = new Date();

    // 1) All active entitlements
    const entitlements = await _db
      .collection("user_products")
      .find({
        userId,
        status: "active",
        $or: [{ endsAt: null }, { endsAt: { $gt: now } }],
      })
      .project({
        userId: 1,
        productId: 1,
        variantId: 1,
        status: 1,
        startedAt: 1,
        endsAt: 1,
        meta: 1,
      })
      .toArray();

    if (!entitlements.length) {
      res.json({ items: [], ...(debug ? { debug: { entitlements } } : {}) });
      return;
    }

    // 2) Products (active only), no forSale filter
    const productIds = Array.from(
      new Set(entitlements.map((e: any) => (e.productId as ObjectId).toString()))
    ).map((s) => new ObjectId(s));

    const products = await _db
      .collection("products")
      .find({ _id: { $in: productIds }, isActive: true })
      .project({ key: 1, name: 1, route: 1, hasVariants: 1, forSale: 1 })
      .toArray();

    const productMap = new Map<string, any>();
    products.forEach((p: any) => productMap.set((p._id as ObjectId).toString(), p));

    // 3) Variants
    const variantIds = Array.from(
      new Set(
        entitlements
          .map((e: any) => e.variantId)
          .filter((v: any) => v && ObjectId.isValid(v))
          .map((v: any) => v.toString())
      )
    ).map((s) => new ObjectId(s));

    const variantMap = new Map<string, any>();
    if (variantIds.length) {
      const variants = await _db
        .collection("product_variants")
        .find({ _id: { $in: variantIds }, isActive: true })
        .project({ key: 1, name: 1, priceMonthly: 1, interval: 1, productId: 1 })
        .toArray();

      variants.forEach((v: any) => variantMap.set((v._id as ObjectId).toString(), v));
    }

    // 4) Group by productId → collect ALL variants
    const grouped = new Map<string, OutItem & { _variantSet?: Set<string> }>();

    for (const e of entitlements) {
      const pid = (e.productId as ObjectId).toString();
      const prod = productMap.get(pid);
      if (!prod) continue; // product might be inactive/removed

      const baseKey = (prod.key as string) || "";
      const existing = grouped.get(pid);

      const mkOutVariant = (): OutVariant | null => {
        if (!e.variantId) return null;
        const vid = (e.variantId as ObjectId).toString();
        const v = variantMap.get(vid);
        if (!v) return null;
        return {
          variantId: v._id,
          key: String(v.key || "").toLowerCase(),
          name: v.name as string,
          priceMonthly: v.priceMonthly ?? null,
          interval: v.interval ?? null,
        };
      };

      if (!existing) {
        const firstVariant = mkOutVariant();
        const set = new Set<string>();
        if (firstVariant) set.add((firstVariant.variantId as ObjectId).toString());

        grouped.set(pid, {
          productId: prod._id,
          key: baseKey,
          name: prod.name as string,
          route: prod.route as string,
          hasVariants: !!prod.hasVariants,
          forSale: !!prod.forSale,
          status: e.status as string,
          startedAt: e.startedAt ?? null,
          endsAt: e.endsAt ?? null,
          meta: e.meta ?? null,
          variant: firstVariant || null,        // back-compat
          variants: firstVariant ? [firstVariant] : [], // NEW: begin list
          _variantSet: set,
        });
      } else {
        // merge date range/status; keep earliest start & latest end
        const prevStart = existing.startedAt ? new Date(existing.startedAt).getTime() : Infinity;
        const currStart = e.startedAt ? new Date(e.startedAt).getTime() : Infinity;
        existing.startedAt = isFinite(prevStart) && isFinite(currStart)
          ? new Date(Math.min(prevStart, currStart))
          : (existing.startedAt || (e.startedAt ?? null));

        const prevEnd = existing.endsAt ? new Date(existing.endsAt).getTime() : 0;
        const currEnd = e.endsAt ? new Date(e.endsAt).getTime() : 0;
        if (currEnd > prevEnd) existing.endsAt = e.endsAt ?? existing.endsAt;

        // collect variant if present (de-dup by variantId)
        const ov = mkOutVariant();
        if (ov) {
          const vidStr = (ov.variantId as ObjectId).toString();
          if (!existing._variantSet!.has(vidStr)) {
            existing._variantSet!.add(vidStr);
            (existing.variants as OutVariant[]).push(ov);
          }
          // keep a stable "variant" for back-compat (first)
          if (!existing.variant) existing.variant = ov;
        }
      }
    }

    // 5) Convert to array
    let items: OutItem[] = Array.from(grouped.values()).map((x) => {
      const { _variantSet, ...rest } = x;
      return rest;
    });

    // 6) Hide Journaling (Solo) if Journaling (bundle component) exists
    const hasBundleJournaling = items.some((it) => it.key === "journaling");
    if (hasBundleJournaling) {
      items = items.filter((it) => it.key !== "journaling_solo");
    }

    // 7) Sort by name for stability
    items.sort((a, b) => String(a.name).localeCompare(String(b.name)));

    res.json({
      items,
      ...(debug
        ? {
            debug: {
              entitlements,
              productIds,
              hasBundleJournaling,
              keptKeys: items.map((x) => x.key),
            },
          }
        : {}),
    });
  } catch (err) {
    console.error("[users.getMyProducts] error:", err);
    res.status(500).json({ message: "Server error" });
  }
};
