package com.example.eventdriven.domain.event;

import com.example.eventdriven.domain.model.Order;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/**
 * OrderCreatedEvent — emitted when a new order is placed.
 */
public record OrderCreatedEvent(
        String eventId,
        String eventType,
        Instant occurredAt,
        String aggregateId,
        String orderId,
        String orderNumber,
        String customerId,
        String customerEmail,
        List<Order.OrderItem> items,
        BigDecimal totalAmount,
        String shippingAddress
) implements DomainEvent {

    public static OrderCreatedEvent from(Order order) {
        return new OrderCreatedEvent(
                DomainEvent.newEventId(),
                "ORDER_CREATED",
                Instant.now(),
                order.getId(),
                order.getId(),
                order.getOrderNumber(),
                order.getCustomerId(),
                order.getCustomerEmail(),
                order.getItems(),
                order.getTotalAmount(),
                order.getShippingAddress()
        );
    }
}
