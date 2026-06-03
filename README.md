# Event-Driven System — Java 21 + Spring Boot + Kafka + Redis + MongoDB

A production-grade event-driven order management system demonstrating clean
architecture principles, idempotent event processing, multi-layer caching, and
full event audit trail.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        REST API Layer                           │
│                    OrderController (Spring MVC)                 │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│                      Service Layer                              │
│                   OrderServiceImpl                              │
│         ┌──────────────┐  ┌──────────┐  ┌──────────────┐       │
│         │   MongoDB    │  │  Redis   │  │    Kafka     │       │
│         │  Persistence │  │  Cache   │  │   Producer   │       │
│         └──────────────┘  └──────────┘  └──────┬───────┘       │
└───────────────────────────────────────────────┼─────────────────┘
                                                 │ Domain Events
              ┌─────────────────────────────────▼──────────────┐
              │               Kafka Topics                      │
              │  order.created │ order.updated │ order.cancelled │
              │  payment.processed │ notification.send          │
              └──────────────────────┬─────────────────────────┘
                                     │
┌────────────────────────────────────▼────────────────────────────┐
│                    Consumer Layer                               │
│                  OrderEventConsumer                             │
│  • Idempotency check (EventAuditLog)                           │
│  • MongoDB state update                                         │
│  • Redis cache eviction                                         │
│  • Audit log persistence                                        │
│  • Manual Kafka ACK                                             │
│  • DLT on failure (after 3 retries)                            │
└─────────────────────────────────────────────────────────────────┘
```

---

## Key Design Decisions

### Java 21 Features Used
- **Sealed interfaces** — `DomainEvent` sealed hierarchy with exhaustive pattern matching
- **Records** — All domain events are immutable records (`OrderCreatedEvent`, etc.)
- **Pattern matching switch** — `DomainEvent.describe()` uses exhaustive switch expressions
- **Text blocks** and `String.formatted()` throughout

### Kafka
| Concern | Approach |
|---|---|
| Ordering | `aggregateId` used as partition key → same order always goes to same partition |
| Idempotency | `EventAuditLog` checked before processing — duplicate events are skipped |
| Reliability | `acks=all` + `enable.idempotence=true` on producer |
| Error handling | `DefaultErrorHandler` with 3-retry `FixedBackOff` + Dead Letter Topic |
| Manual commit | `AckMode.MANUAL` — offset only ACKed after successful processing |

### Redis
- Cache-aside pattern: `@Cacheable` on reads, `@CacheEvict` on writes
- Per-region TTLs: orders (10 min), customer order lists (10 min)
- `allKeys-lru` eviction policy — handles memory pressure gracefully
- Polymorphic serialization via `GenericJackson2JsonRedisSerializer`

### MongoDB
- `@EnableMongoAuditing` for automatic `createdAt` / `updatedAt`
- Compound indexes on `customerId + status` for common query patterns
- TTL index on `event_audit_log.createdAt` → auto-deletes logs after 90 days

---

## Project Structure

```
src/main/java/com/example/eventdriven/
├── EventDrivenApplication.java
├── config/
│   ├── KafkaConfig.java          # Topics, producer, consumer factory, DLT
│   └── RedisConfig.java          # Cache manager, TTLs, serialization
├── controller/
│   ├── OrderController.java      # REST endpoints
│   └── GlobalExceptionHandler.java
├── domain/
│   ├── model/
│   │   ├── Order.java            # MongoDB document
│   │   └── EventAuditLog.java    # Event audit trail document
│   └── event/                    # Sealed interface + record events
│       ├── DomainEvent.java
│       ├── OrderCreatedEvent.java
│       ├── OrderUpdatedEvent.java
│       ├── OrderCancelledEvent.java
│       ├── PaymentProcessedEvent.java
│       └── NotificationEvent.java
├── kafka/
│   ├── producer/EventProducer.java
│   └── consumer/OrderEventConsumer.java
├── repository/
│   ├── OrderRepository.java
│   └── EventAuditLogRepository.java
└── service/
    ├── OrderService.java
    ├── CacheService.java
    └── impl/OrderServiceImpl.java
```

---

## Running Locally

### 1. Start infrastructure

```bash
cd docker
docker compose up -d
```

Wait for all services to be healthy:

```bash
docker compose ps
```

### 2. Run the application

```bash
./mvnw spring-boot:run
```

### 3. Management UIs

| Service | URL | Credentials |
|---|---|---|
| Kafka UI | http://localhost:8090 | — |
| Redis Commander | http://localhost:8091 | — |
| Mongo Express | http://localhost:8092 | admin / admin |
| App Actuator | http://localhost:8080/actuator/health | — |

---

## API Reference

### Create Order
```http
POST /api/v1/orders
Content-Type: application/json

{
  "customerId": "cust-001",
  "customerEmail": "alice@example.com",
  "shippingAddress": "123 Main St, Melbourne VIC 3000",
  "items": [
    {
      "productId": "prod-widget",
      "productName": "Super Widget",
      "quantity": 2,
      "unitPrice": 49.99
    }
  ]
}
```

### Get Order
```http
GET /api/v1/orders/{orderId}
GET /api/v1/orders/number/{orderNumber}
GET /api/v1/orders/customer/{customerId}
GET /api/v1/orders/status/PENDING
```

### Update Status
```http
PATCH /api/v1/orders/{orderId}/status
{ "status": "SHIPPED", "updatedBy": "warehouse-system" }
```

### Cancel Order
```http
DELETE /api/v1/orders/{orderId}
{ "reason": "Customer request", "cancelledBy": "support-agent-42" }
```

---

## Event Flow

```
POST /orders
    │
    ├─→ MongoDB: save(order, status=PENDING)
    ├─→ Kafka:   publish(OrderCreatedEvent) → order.created
    │
    │   [Consumer picks up order.created]
    ├─→ Idempotency: check EventAuditLog
    ├─→ MongoDB: order.status = CONFIRMED
    ├─→ Redis:   evict(orderId, orderNumber, customerId)
    └─→ MongoDB: save(EventAuditLog, status=PROCESSED)
```
