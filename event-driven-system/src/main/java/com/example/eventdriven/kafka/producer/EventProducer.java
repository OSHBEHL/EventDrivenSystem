package com.example.eventdriven.kafka.producer;

import com.example.eventdriven.domain.event.DomainEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * Generic Kafka event publisher.
 * Routes each DomainEvent to its designated topic and uses the aggregateId
 * as the partition key to preserve ordering per aggregate.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EventProducer {

    private final KafkaTemplate<String, DomainEvent> kafkaTemplate;

    @Value("${app.kafka.topics.order-created}")
    private String orderCreatedTopic;

    @Value("${app.kafka.topics.order-updated}")
    private String orderUpdatedTopic;

    @Value("${app.kafka.topics.order-cancelled}")
    private String orderCancelledTopic;

    @Value("${app.kafka.topics.payment-processed}")
    private String paymentProcessedTopic;

    @Value("${app.kafka.topics.notification}")
    private String notificationTopic;

    /**
     * Publish a domain event to its corresponding topic.
     * Key = aggregateId ensures partition affinity for ordering.
     */
    public CompletableFuture<SendResult<String, DomainEvent>> publish(DomainEvent event) {
        String topic = resolveTopic(event);
        String key   = event.aggregateId();

        log.info("Publishing event [type={}, id={}, aggregateId={}] to topic [{}]",
                event.eventType(), event.eventId(), key, topic);

        return kafkaTemplate.send(topic, key, event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("Event [{}] published successfully → topic={}, partition={}, offset={}",
                                event.eventId(),
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    } else {
                        log.error("Failed to publish event [type={}, id={}]: {}",
                                event.eventType(), event.eventId(), ex.getMessage(), ex);
                    }
                });
    }

    /**
     * Publish to an explicit topic (e.g. DLT or custom routing).
     */
    public CompletableFuture<SendResult<String, DomainEvent>> publishTo(String topic, DomainEvent event) {
        log.info("Publishing event [type={}, id={}] to explicit topic [{}]",
                event.eventType(), event.eventId(), topic);

        return kafkaTemplate.send(topic, event.aggregateId(), event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.debug("Event [{}] published to {} at offset {}",
                                event.eventId(), topic, result.getRecordMetadata().offset());
                    } else {
                        log.error("Failed to publish event to {}: {}", topic, ex.getMessage(), ex);
                    }
                });
    }

    // ── Topic Resolution ────────────────────────────────────────────────────

    private String resolveTopic(DomainEvent event) {
        return switch (event.eventType()) {
            case "ORDER_CREATED"     -> orderCreatedTopic;
            case "ORDER_UPDATED"     -> orderUpdatedTopic;
            case "ORDER_CANCELLED"   -> orderCancelledTopic;
            case "PAYMENT_PROCESSED" -> paymentProcessedTopic;
            case "NOTIFICATION"      -> notificationTopic;
            default -> throw new IllegalArgumentException(
                    "No topic mapping for event type: " + event.eventType());
        };
    }
}
