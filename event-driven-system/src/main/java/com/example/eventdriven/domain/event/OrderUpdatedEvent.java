package com.example.eventdriven.domain.event;

import com.example.eventdriven.domain.model.Order;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * OrderUpdatedEvent — emitted on any order status change.
 */
public record OrderUpdatedEvent(
        String eventId,
        String eventType,
        Instant occurredAt,
        String aggregateId,
        String orderId,
        Order.OrderStatus previousStatus,
        Order.OrderStatus newStatus,
        String updatedBy
) implements DomainEvent {

    public static OrderUpdatedEvent from(Order order, Order.OrderStatus previousStatus, String updatedBy) {
        return new OrderUpdatedEvent(
                DomainEvent.newEventId(),
                "ORDER_UPDATED",
                Instant.now(),
                order.getId(),
                order.getId(),
                previousStatus,
                order.getStatus(),
                updatedBy
        );
    }
}
