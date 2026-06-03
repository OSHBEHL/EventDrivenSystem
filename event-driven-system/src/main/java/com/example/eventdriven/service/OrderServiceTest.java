package com.example.eventdriven.service;

import com.example.eventdriven.domain.event.OrderCreatedEvent;
import com.example.eventdriven.domain.model.Order;
import com.example.eventdriven.kafka.producer.EventProducer;
import com.example.eventdriven.repository.OrderRepository;
import com.example.eventdriven.service.impl.OrderServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    @Mock OrderRepository      orderRepository;
    @Mock EventProducer        eventProducer;
    @Mock CacheService         cacheService;

    @InjectMocks OrderServiceImpl orderService;

    private Order sampleOrder;

    @BeforeEach
    void setUp() {
        sampleOrder = Order.builder()
                .id("order-123")
                .orderNumber("ORD-ABC123")
                .customerId("cust-001")
                .customerEmail("test@example.com")
                .totalAmount(new BigDecimal("99.99"))
                .status(Order.OrderStatus.PENDING)
                .items(List.of(
                        Order.OrderItem.builder()
                                .productId("prod-1").productName("Widget")
                                .quantity(2).unitPrice(new BigDecimal("49.99"))
                                .subtotal(new BigDecimal("99.98")).build()
                ))
                .build();
    }

    @Test
    @DisplayName("createOrder → persists to MongoDB, publishes OrderCreatedEvent")
    void createOrder_persistsAndPublishesEvent() {
        when(orderRepository.save(any(Order.class))).thenReturn(sampleOrder);
        when(eventProducer.publish(any())).thenReturn(CompletableFuture.completedFuture(null));

        Order result = orderService.createOrder(sampleOrder);

        assertThat(result).isNotNull();
        assertThat(result.getId()).isEqualTo("order-123");

        var eventCaptor = ArgumentCaptor.forClass(OrderCreatedEvent.class);
        verify(eventProducer).publish(eventCaptor.capture());
        assertThat(eventCaptor.getValue().eventType()).isEqualTo("ORDER_CREATED");
        assertThat(eventCaptor.getValue().orderId()).isEqualTo("order-123");
    }

    @Test
    @DisplayName("updateOrderStatus → updates MongoDB, evicts cache, publishes event")
    void updateOrderStatus_updatesAndEvicts() {
        when(orderRepository.findById("order-123")).thenReturn(Optional.of(sampleOrder));
        when(orderRepository.save(any())).thenReturn(sampleOrder);
        when(eventProducer.publish(any())).thenReturn(CompletableFuture.completedFuture(null));

        orderService.updateOrderStatus("order-123", Order.OrderStatus.SHIPPED, "system");

        verify(cacheService).evictOrder("order-123", "ORD-ABC123", "cust-001");
        verify(eventProducer).publish(any());
    }

    @Test
    @DisplayName("cancelOrder → throws when order already cancelled")
    void cancelOrder_throwsWhenAlreadyCancelled() {
        sampleOrder.setStatus(Order.OrderStatus.CANCELLED);
        when(orderRepository.findById("order-123")).thenReturn(Optional.of(sampleOrder));

        assertThatThrownBy(() -> orderService.cancelOrder("order-123", "duplicate", "user"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already cancelled");
    }

    @Test
    @DisplayName("findById → returns empty when not found")
    void findById_returnsEmptyWhenNotFound() {
        when(orderRepository.findById("nonexistent")).thenReturn(Optional.empty());
        assertThat(orderService.findById("nonexistent")).isEmpty();
    }
}
