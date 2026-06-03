// MongoDB initialization — creates collections and indexes for eventdb

db = db.getSiblingDB('eventdb');

// ── Orders collection ──────────────────────────────────────────────────────
db.createCollection('orders');

db.orders.createIndex({ orderNumber: 1 }, { unique: true });
db.orders.createIndex({ customerId: 1 });
db.orders.createIndex({ status: 1 });
db.orders.createIndex({ createdAt: 1 });
db.orders.createIndex({ customerId: 1, status: 1 });

// ── Event Audit Log collection ─────────────────────────────────────────────
db.createCollection('event_audit_log');

db.event_audit_log.createIndex({ eventId: 1 },     { unique: true });
db.event_audit_log.createIndex({ aggregateId: 1 });
db.event_audit_log.createIndex({ eventType: 1 });
db.event_audit_log.createIndex({ processingStatus: 1 });
db.event_audit_log.createIndex({ createdAt: 1 });

// TTL index — auto-delete audit logs older than 90 days
db.event_audit_log.createIndex(
  { createdAt: 1 },
  { expireAfterSeconds: 7776000, name: "ttl_90d" }
);

print("✅ MongoDB eventdb initialised — indexes created.");
