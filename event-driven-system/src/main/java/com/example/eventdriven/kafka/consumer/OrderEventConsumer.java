package com.example.eventdriven.kafka.consumer;

import com.example.eventdriven.domain.event.*;
import com.example.eventdriven.domain.model.EventAuditLog;
import com.example.eventdriven.domain.model.Order;
import com.example.eventdriven.repository.EventAuditLogRepository;
import com.example.eventdriven.repository.OrderRepository;
import com.example.eventdriven.service.CacheService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Kafka consumers for all order-related domain events.
 *
 * <p>Each listener:
 * <ol>
 *   <li>Checks idempotency via EventAuditLog (prevents duplicate processing)</li>
 *   <li>Processes the event (updates MongoDB state)</li>
 *   <li>Evicts stale Redis cache entries</li>
 *   <li>Persists an audit log entry</li>
 *   <li>Manually ACKs the Kafka offset</li>
 * </ol>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final OrderRepository          orderRepository;
    private final EventAuditLogRepository  auditLogRepository;
    private final CacheService             cacheService;
    private final ObjectMapper             objectMapper;

    // ── ORDER CREATED ────────────────────────────────────────────────────────

    @KafkaListener(
            topics   = "${app.kafka.topics.order-created}",
            groupId  = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onOrderCreated(
            ConsumerRecord<String, OrderCreatedEvent> record,
            Acknowledgment ack,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        OrderCreatedEvent event = record.value();
        log.info("▶ ORDER_CREATED [eventId={}, orderId={}]", event.eventId(), event.orderId());

        if (isDuplicate(event.eventId())) {
            log.warn("Duplicate event detected [eventId={}], skipping", event.eventId());
            ack.acknowledge();
            return;
        }

        try {
            // Update order status → CONFIRMED
            orderRepository.findById(event.orderId()).ifPresent(order -> {
                order.setStatus(Order.OrderStatus.CONFIRMED);
                orderRepository.save(order);
                cacheService.evictOrder(order.getId(), order.getOrderNumber(), order.getCustomerId());
                log.info("Order [{}] confirmed in MongoDB", event.orderId());
            });

            persistAuditLog(event, topic, partition, offset, "PROCESSED", null);
            ack.acknowledge();

        } catch (Exception ex) {
            log.error("Error processing ORDER_CREATED [eventId={}]: {}", event.eventId(), ex.getMessage(), ex);
            persistAuditLog(event, topic, partition, offset, "FAILED", ex.getMessage());
            throw ex; // Let DefaultErrorHandler handle retry / DLT
        }
    }

    // ── ORDER UPDATED ────────────────────────────────────────────────────────

    @KafkaListener(
            topics   = "${app.kafka.topics.order-updated}",
            groupId  = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onOrderUpdated(
            ConsumerRecord<String, OrderUpdatedEvent> record,
            Acknowledgment ack,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        OrderUpdatedEvent event = record.value();
        log.info("▶ ORDER_UPDATED [eventId={}, orderId={}, status={}→{}]",
                event.eventId(), event.orderId(), event.previousStatus(), event.newStatus());

        if (isDuplicate(event.eventId())) {
            ack.acknowledge();
            return;
        }

        try {
            orderRepository.findById(event.orderId()).ifPresent(order -> {
                order.setStatus(event.newStatus());
                orderRepository.save(order);
                cacheService.evictOrder(order.getId(), order.getOrderNumber(), order.getCustomerId());
            });

            persistAuditLog(event, topic, partition, offset, "PROCESSED", null);
            ack.acknowledge();

        } catch (Exception ex) {
            log.error("Error processing ORDER_UPDATED [eventId={}]: {}", event.eventId(), ex.getMessage(), ex);
            persistAuditLog(event, topic, partition, offset, "FAILED", ex.getMessage());
            throw ex;
        }
    }

    // ── ORDER CANCELLED ──────────────────────────────────────────────────────

    @KafkaListener(
            topics   = "${app.kafka.topics.order-cancelled}",
            groupId  = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onOrderCancelled(
            ConsumerRecord<String, OrderCancelledEvent> record,
            Acknowledgment ack,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        OrderCancelledEvent event = record.value();
        log.info("▶ ORDER_CANCELLED [eventId={}, orderId={}, reason={}]",
                event.eventId(), event.orderId(), event.reason());

        if (isDuplicate(event.eventId())) {
            ack.acknowledge();
            return;
        }

        try {
            orderRepository.findById(event.orderId()).ifPresent(order -> {
                order.setStatus(Order.OrderStatus.CANCELLED);
                orderRepository.save(order);
                cacheService.evictOrder(order.getId(), order.getOrderNumber(), order.getCustomerId());
            });

            persistAuditLog(event, topic, partition, offset, "PROCESSED", null);
            ack.acknowledge();

        } catch (Exception ex) {
            log.error("Error processing ORDER_CANCELLED [eventId={}]: {}", event.eventId(), ex.getMessage(), ex);
            persistAuditLog(event, topic, partition, offset, "FAILED", ex.getMessage());
            throw ex;
        }
    }

    // ── PAYMENT PROCESSED ────────────────────────────────────────────────────

    @KafkaListener(
            topics   = "${app.kafka.topics.payment-processed}",
            groupId  = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onPaymentProcessed(
            ConsumerRecord<String, PaymentProcessedEvent> record,
            Acknowledgment ack,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        PaymentProcessedEvent event = record.value();
        log.info("▶ PAYMENT_PROCESSED [eventId={}, orderId={}, status={}]",
                event.eventId(), event.orderId(), event.paymentStatus());

        if (isDuplicate(event.eventId())) {
            ack.acknowledge();
            return;
        }

        try {
            orderRepository.findById(event.orderId()).ifPresent(order -> {
                boolean success = "SUCCESS".equals(event.paymentStatus());
                order.setStatus(success ? Order.OrderStatus.PROCESSING : Order.OrderStatus.PAYMENT_FAILED);

                var paymentDetails = Order.PaymentDetails.builder()
                        .transactionId(event.transactionId())
                        .paymentMethod(event.paymentMethod())
                        .paymentStatus(event.paymentStatus())
                        .processedAt(event.occurredAt())
                        .build();
                order.setPaymentDetails(paymentDetails);
                orderRepository.save(order);
                cacheService.evictOrder(order.getId(), order.getOrderNumber(), order.getCustomerId());
            });

            persistAuditLog(event, topic, partition, offset, "PROCESSED", null);
            ack.acknowledge();

        } catch (Exception ex) {
            log.error("Error processing PAYMENT_PROCESSED [eventId={}]: {}", event.eventId(), ex.getMessage(), ex);
            persistAuditLog(event, topic, partition, offset, "FAILED", ex.getMessage());
            throw ex;
        }
    }

    // ── HELPERS ──────────────────────────────────────────────────────────────

    private boolean isDuplicate(String eventId) {
        return auditLogRepository.existsByEventId(eventId);
    }

    private void persistAuditLog(DomainEvent event, String topic, int partition,
                                  long offset, String status, String errorMessage) {
        try {
            var log_ = EventAuditLog.builder()
                    .eventId(event.eventId())
                    .eventType(event.eventType())
                    .aggregateId(event.aggregateId())
                    .payload(objectMapper.writeValueAsString(event))
                    .processingStatus(status)
                    .errorMessage(errorMessage)
                    .kafkaTopic(topic)
                    .kafkaPartition(partition)
                    .kafkaOffset(offset)
                    .processedAt(Instant.now())
                    .build();
            auditLogRepository.save(log_);
        } catch (Exception ex) {
            log.error("Failed to persist audit log for eventId={}: {}", event.eventId(), ex.getMessage());
        }
    }
}
