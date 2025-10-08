//server/src/models/data.model.ts
import { Schema, model, Document } from 'mongoose';

export interface IData extends Document {
  symbol: string;
  price: number;
  volume: number;
  timestamp: Date;
  metadata?: object;
  source?: string;
}

const dataSchema = new Schema<IData>(
  {
    symbol: { type: String, required: true, index: true },
    price: { type: Number, required: true },
    volume: { type: Number, required: true },
    timestamp: { type: Date, default: Date.now, index: true },
    metadata: { type: Object },
    source: { type: String, default: 'manual' },
  },
  { timestamps: true }
);

// Index for faster queries
dataSchema.index({ symbol: 1, timestamp: -1 });

export const Data = model<IData>('Data', dataSchema);