package com.example.eventdriven.repository;

import com.example.eventdriven.domain.model.EventAuditLog;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface EventAuditLogRepository extends MongoRepository<EventAuditLog, String> {

    Optional<EventAuditLog> findByEventId(String eventId);

    List<EventAuditLog> findByAggregateId(String aggregateId);

    List<EventAuditLog> findByEventType(String eventType);

    List<EventAuditLog> findByProcessingStatus(String processingStatus);

    boolean existsByEventId(String eventId);
}
