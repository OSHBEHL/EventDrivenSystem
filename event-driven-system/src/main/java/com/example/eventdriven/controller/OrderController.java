package com.example.eventdriven.controller;

import com.example.eventdriven.domain.model.Order;
import com.example.eventdriven.service.OrderService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/v1/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    // ── Create Order ─────────────────────────────────────────────────────────

    @PostMapping
    public ResponseEntity<Order> createOrder(@Valid @RequestBody CreateOrderRequest request) {
        var items = request.items().stream()
                .map(i -> Order.OrderItem.builder()
                        .productId(i.productId())
                        .productName(i.productName())
                        .quantity(i.quantity())
                        .unitPrice(i.unitPrice())
                        .subtotal(i.unitPrice().multiply(BigDecimal.valueOf(i.quantity())))
                        .build())
                .toList();

        BigDecimal total = items.stream()
                .map(Order.OrderItem::getSubtotal)
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        var order = Order.builder()
                .customerId(request.customerId())
                .customerEmail(request.customerEmail())
                .items(items)
                .totalAmount(total)
                .shippingAddress(request.shippingAddress())
                .notes(request.notes())
                .build();

        return ResponseEntity.status(HttpStatus.CREATED).body(orderService.createOrder(order));
    }

    // ── Get Order by ID ──────────────────────────────────────────────────────

    @GetMapping("/{orderId}")
    public ResponseEntity<Order> getOrder(@PathVariable String orderId) {
        return orderService.findById(orderId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    // ── Get Order by Number ──────────────────────────────────────────────────

    @GetMapping("/number/{orderNumber}")
    public ResponseEntity<Order> getOrderByNumber(@PathVariable String orderNumber) {
        return orderService.findByOrderNumber(orderNumber)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    // ── Get Orders by Customer ───────────────────────────────────────────────

    @GetMapping("/customer/{customerId}")
    public ResponseEntity<List<Order>> getCustomerOrders(@PathVariable String customerId) {
        return ResponseEntity.ok(orderService.findByCustomerId(customerId));
    }

    // ── Get Orders by Status ─────────────────────────────────────────────────

    @GetMapping("/status/{status}")
    public ResponseEntity<List<Order>> getOrdersByStatus(
            @PathVariable Order.OrderStatus status) {
        return ResponseEntity.ok(orderService.findByStatus(status));
    }

    // ── Update Order Status ──────────────────────────────────────────────────

    @PatchMapping("/{orderId}/status")
    public ResponseEntity<Order> updateStatus(
            @PathVariable String orderId,
            @Valid @RequestBody UpdateStatusRequest request) {
        return ResponseEntity.ok(
                orderService.updateOrderStatus(orderId, request.status(), request.updatedBy()));
    }

    // ── Cancel Order ─────────────────────────────────────────────────────────

    @DeleteMapping("/{orderId}")
    public ResponseEntity<Order> cancelOrder(
            @PathVariable String orderId,
            @Valid @RequestBody CancelOrderRequest request) {
        return ResponseEntity.ok(
                orderService.cancelOrder(orderId, request.reason(), request.cancelledBy()));
    }

    // ── Request/Response Records ─────────────────────────────────────────────

    public record CreateOrderRequest(
            @NotBlank String customerId,
            @NotBlank String customerEmail,
            @NotEmpty List<OrderItemRequest> items,
            @NotBlank String shippingAddress,
            String notes
    ) {}

    public record OrderItemRequest(
            @NotBlank String productId,
            @NotBlank String productName,
            @Positive int quantity,
            @NotNull @Positive BigDecimal unitPrice
    ) {}

    public record UpdateStatusRequest(
            @NotNull Order.OrderStatus status,
            @NotBlank String updatedBy
    ) {}

    public record CancelOrderRequest(
            @NotBlank String reason,
            @NotBlank String cancelledBy
    ) {}
}
