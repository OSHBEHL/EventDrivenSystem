package com.example.eventdriven.domain.event;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * PaymentProcessedEvent — emitted by the payment service after processing.
 */
public record PaymentProcessedEvent(
        String eventId,
        String eventType,
        Instant occurredAt,
        String aggregateId,
        String orderId,
        String transactionId,
        String paymentMethod,
        BigDecimal amount,
        String paymentStatus,   // SUCCESS | FAILED | REFUNDED
        String failureReason
) implements DomainEvent {}
