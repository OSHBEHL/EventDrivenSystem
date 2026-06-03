package com.example.eventdriven.service.impl;

import com.example.eventdriven.domain.event.*;
import com.example.eventdriven.domain.model.Order;
import com.example.eventdriven.kafka.producer.EventProducer;
import com.example.eventdriven.repository.OrderRepository;
import com.example.eventdriven.service.CacheService;
import com.example.eventdriven.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Core order service.
 *
 * <p>Flow per operation:
 * <ol>
 *   <li>Validate &amp; persist to MongoDB</li>
 *   <li>Update Redis cache</li>
 *   <li>Publish domain event to Kafka</li>
 * </ol>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderServiceImpl implements OrderService {

    private final OrderRepository orderRepository;
    private final EventProducer   eventProducer;
    private final CacheService    cacheService;

    // ── CREATE ───────────────────────────────────────────────────────────────

    @Override
    public Order createOrder(Order order) {
        order.setOrderNumber(generateOrderNumber());
        order.setStatus(Order.OrderStatus.PENDING);

        Order saved = orderRepository.save(order);
        log.info("Order created [id={}, number={}]", saved.getId(), saved.getOrderNumber());

        // Publish event asynchronously — fire and forget
        eventProducer.publish(OrderCreatedEvent.from(saved));

        return saved;
    }

    // ── UPDATE STATUS ────────────────────────────────────────────────────────

    @Override
    @CachePut(value = "orders", key = "#orderId")
    public Order updateOrderStatus(String orderId, Order.OrderStatus newStatus, String updatedBy) {
        Order order = findOrThrow(orderId);
        Order.OrderStatus previousStatus = order.getStatus();

        order.setStatus(newStatus);
        Order saved = orderRepository.save(order);

        cacheService.evictOrder(saved.getId(), saved.getOrderNumber(), saved.getCustomerId());

        eventProducer.publish(OrderUpdatedEvent.from(saved, previousStatus, updatedBy));

        log.info("Order [{}] status updated {}→{} by {}", orderId, previousStatus, newStatus, updatedBy);
        return saved;
    }

    // ── CANCEL ───────────────────────────────────────────────────────────────

    @Override
    public Order cancelOrder(String orderId, String reason, String cancelledBy) {
        Order order = findOrThrow(orderId);

        if (order.getStatus() == Order.OrderStatus.CANCELLED) {
            throw new IllegalStateException("Order %s is already cancelled".formatted(orderId));
        }
        if (order.getStatus() == Order.OrderStatus.DELIVERED) {
            throw new IllegalStateException("Cannot cancel a delivered order");
        }

        order.setStatus(Order.OrderStatus.CANCELLED);
        Order saved = orderRepository.save(order);

        cacheService.evictOrder(saved.getId(), saved.getOrderNumber(), saved.getCustomerId());

        var cancelEvent = new OrderCancelledEvent(
                DomainEvent.newEventId(),
                "ORDER_CANCELLED",
                Instant.now(),
                saved.getId(),
                saved.getId(),
                saved.getOrderNumber(),
                saved.getCustomerId(),
                reason,
                cancelledBy
        );
        eventProducer.publish(cancelEvent);

        log.info("Order [{}] cancelled by {} — reason: {}", orderId, cancelledBy, reason);
        return saved;
    }

    // ── QUERIES (cache-read-through) ─────────────────────────────────────────

    @Override
    @Cacheable(value = "orders", key = "#orderId", unless = "#result == null")
    public Optional<Order> findById(String orderId) {
        log.debug("Cache miss → loading order [{}] from MongoDB", orderId);
        return orderRepository.findById(orderId);
    }

    @Override
    @Cacheable(value = "orders-by-number", key = "#orderNumber", unless = "#result == null")
    public Optional<Order> findByOrderNumber(String orderNumber) {
        log.debug("Cache miss → loading order [number={}] from MongoDB", orderNumber);
        return orderRepository.findByOrderNumber(orderNumber);
    }

    @Override
    @Cacheable(value = "customer-orders", key = "#customerId", unless = "#result == null || #result.isEmpty()")
    public List<Order> findByCustomerId(String customerId) {
        log.debug("Cache miss → loading orders for customer [{}] from MongoDB", customerId);
        return orderRepository.findByCustomerId(customerId);
    }

    @Override
    public List<Order> findByStatus(Order.OrderStatus status) {
        return orderRepository.findByStatus(status);
    }

    // ── PRIVATE ──────────────────────────────────────────────────────────────

    private Order findOrThrow(String orderId) {
        return orderRepository.findById(orderId)
                .orElseThrow(() -> new IllegalArgumentException("Order not found: " + orderId));
    }

    private String generateOrderNumber() {
        return "ORD-" + UUID.randomUUID().toString().replace("-", "").substring(0, 10).toUpperCase();
    }
}
