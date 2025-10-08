//server/src/auth.routes.ts 
import { Router } from "express";
import {
  registerIntent,
  finalizeSignup,
  login,
  forgotPassword,
  resetPassword,
  devGetLastOtp,
} from "../controllers/auth.controller";

const router = Router();

/**
 * Helpful guard: if something accidentally does a GET to this path,
 * respond with 405 to make the mistake obvious in the browser.
 */
router.get("/register-intent", (_req, res) => {
  res.status(405).send("Use POST /api/auth/register-intent");
});

router.post("/register-intent", registerIntent);
router.post("/finalize-signup", finalizeSignup);
router.post("/login", login);
router.post("/forgot-password", forgotPassword);
router.post("/reset-password", resetPassword);

if (process.env.NODE_ENV !== "production") {
  router.get("/dev/last-otp", devGetLastOtp);
}

export default router;