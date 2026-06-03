package com.example.eventdriven.domain.model;

import lombok.Builder;
import lombok.Data;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

/**
 * Persisted audit log of every domain event processed.
 * Provides a full event sourcing trail in MongoDB.
 */
@Data
@Builder
@Document(collection = "event_audit_log")
public class EventAuditLog {

    @Id
    private String id;

    @Indexed
    private String eventId;

    @Indexed
    private String eventType;

    @Indexed
    private String aggregateId;

    private String payload;         // JSON serialized event

    private String processingStatus; // RECEIVED | PROCESSED | FAILED

    private String errorMessage;

    private String kafkaTopic;

    private int kafkaPartition;

    private long kafkaOffset;

    @CreatedDate
    private Instant createdAt;

    private Instant processedAt;
}
