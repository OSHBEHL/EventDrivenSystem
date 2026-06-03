package com.example.eventdriven.domain.event;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

/**
 * Base sealed interface for all domain events.
 * Uses Java 21 sealed classes + records for exhaustive pattern matching.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "eventType")
@JsonSubTypes({
    @JsonSubTypes.Type(value = OrderCreatedEvent.class,   name = "ORDER_CREATED"),
    @JsonSubTypes.Type(value = OrderUpdatedEvent.class,   name = "ORDER_UPDATED"),
    @JsonSubTypes.Type(value = OrderCancelledEvent.class, name = "ORDER_CANCELLED"),
    @JsonSubTypes.Type(value = PaymentProcessedEvent.class, name = "PAYMENT_PROCESSED"),
    @JsonSubTypes.Type(value = NotificationEvent.class,   name = "NOTIFICATION")
})
public sealed interface DomainEvent extends Serializable
    permits OrderCreatedEvent, OrderUpdatedEvent, OrderCancelledEvent,
            PaymentProcessedEvent, NotificationEvent {

    String eventId();
    String eventType();
    Instant occurredAt();
    String aggregateId();

    static String newEventId() {
        return UUID.randomUUID().toString();
    }

    /**
     * Java 21 pattern matching — exhaustive switch over all event subtypes.
     */
    default String describe() {
        return switch (this) {
            case OrderCreatedEvent e   -> "Order %s created for customer %s".formatted(e.orderNumber(), e.customerId());
            case OrderUpdatedEvent e   -> "Order %s updated to status %s".formatted(e.orderId(), e.newStatus());
            case OrderCancelledEvent e -> "Order %s cancelled: %s".formatted(e.orderId(), e.reason());
            case PaymentProcessedEvent e -> "Payment %s for order %s: %s".formatted(e.transactionId(), e.orderId(), e.paymentStatus());
            case NotificationEvent e   -> "Notification to %s: %s".formatted(e.recipient(), e.subject());
        };
    }
}
