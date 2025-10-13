// src/utils/time.ts
export function istNowString(): string {
  const now = new Date();
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Kolkata",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).formatToParts(now);
  const map = Object.fromEntries(parts.map(p => [p.type, p.value]));
  const ms = String(now.getMilliseconds()).padStart(3, "0");
  return `${map.year}-${map.month}-${map.day} ${map.hour}:${map.minute}:${map.second}.${ms} IST`;
}

export function istTimestamp(d: Date): string {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Kolkata",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).formatToParts(d);
  const map = Object.fromEntries(parts.map(p => [p.type, p.value]));
  const ms = String(d.getMilliseconds()).padStart(3, "0");
  return `${map.year}-${map.month}-${map.day} ${map.hour}:${map.minute}:${map.second}.${ms} IST`;
}
