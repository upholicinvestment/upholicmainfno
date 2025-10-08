//server/src/routes/payment.routes.ts
import { Router } from "express";
import { createOrder, verifyPayment } from "../controllers/payment.controller";

const router = Router();

router.post("/create-order", createOrder);
router.post("/verify", verifyPayment);

export default router;
