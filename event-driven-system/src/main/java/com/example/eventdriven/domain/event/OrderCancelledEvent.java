package com.example.eventdriven.domain.event;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * OrderCancelledEvent — emitted when an order is cancelled.
 */
public record OrderCancelledEvent(
        String eventId,
        String eventType,
        Instant occurredAt,
        String aggregateId,
        String orderId,
        String orderNumber,
        String customerId,
        String reason,
        String cancelledBy
) implements DomainEvent {}
