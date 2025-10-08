//server/src/routes/products.routes.ts
import { Router } from 'express';
import { Db, ObjectId } from 'mongodb';
import { setDatabase as setAuthDb } from '../controllers/auth.controller';

let db: Db;
export const setProductsDb = (database: Db) => {
  db = database;
  setAuthDb(database);
};

const router = Router();

router.get('/', async (_req, res) => {
  try {
    // Only return purchasable products (bundle + algo)
    const products = await db
      .collection('products')
      .find({ isActive: true, forSale: true })
      .toArray();

    const withVariants = products.filter((p: any) => p.hasVariants);
    const ids = withVariants.map((p: any) => p._id as ObjectId);

    const variants = ids.length
      ? await db
          .collection('product_variants')
          .find({ productId: { $in: ids }, isActive: true })
          .toArray()
      : [];

    const variantMap: Record<string, any[]> = {};
    variants.forEach((v: any) => {
      const pid = v.productId.toString();
      (variantMap[pid] ||= []).push(v);
    });

    res.json(
      products.map((p: any) => ({
        ...p,
        variants: p.hasVariants ? (variantMap[p._id.toString()] || []) : [],
      }))
    );
  } catch (e) {
    console.error('products.routes error:', e);
    res.status(500).json({ message: 'Failed to load products' });
  }
});

export default router;