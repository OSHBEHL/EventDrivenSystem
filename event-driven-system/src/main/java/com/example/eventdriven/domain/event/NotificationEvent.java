package com.example.eventdriven.domain.event;

import java.time.Instant;

/**
 * NotificationEvent — emitted to trigger downstream notification delivery.
 */
public record NotificationEvent(
        String eventId,
        String eventType,
        Instant occurredAt,
        String aggregateId,
        String recipient,
        String channel,         // EMAIL | SMS | PUSH
        String subject,
        String body,
        String correlatedOrderId
) implements DomainEvent {}
