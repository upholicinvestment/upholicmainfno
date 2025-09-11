// import { Router } from "express";
// import { getProfile, updateProfile, getMyProducts } from "../controllers/user.controller";
// // import { authenticate } from "../middleware/auth.middleware"; // optional

// const router = Router();

// // Public-friendly; add `authenticate` if you want strict auth
// router.get("/me", /* authenticate, */ getProfile);
// router.put("/me", /* authenticate, */ updateProfile);
// router.get("/me/products", /* authenticate, */ getMyProducts);

// export default router;

// server/src/routes/user.routes.ts
import { Router } from "express";
import {
  getProfile,
  updateProfile,
  getMyProducts,
  getAvatarOptions,
} from "../controllers/user.controller";
// import { authenticate } from "../middleware/auth.middleware";

const router = Router();

// Profile
router.get("/me", /* authenticate, */ getProfile);
router.put("/me", /* authenticate, */ updateProfile);

// Subscriptions
router.get("/me/products", /* authenticate, */ getMyProducts);

// Avatar options (keys only; frontend maps to assets)
router.get("/me/avatar-options", /* authenticate, */ getAvatarOptions);

export default router;
