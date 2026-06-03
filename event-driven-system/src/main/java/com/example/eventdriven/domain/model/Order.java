package com.example.eventdriven.domain.model;

import lombok.Builder;
import lombok.Data;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

@Data
@Builder
@Document(collection = "orders")
public class Order implements Serializable {

    @Id
    private String id;

    @Indexed(unique = true)
    private String orderNumber;

    @Indexed
    private String customerId;

    private String customerEmail;

    private List<OrderItem> items;

    private BigDecimal totalAmount;

    private OrderStatus status;

    private String shippingAddress;

    private PaymentDetails paymentDetails;

    private String notes;

    @CreatedDate
    private Instant createdAt;

    @LastModifiedDate
    private Instant updatedAt;

    // Java 21 record-style sealed hierarchy for status
    public enum OrderStatus {
        PENDING, CONFIRMED, PAYMENT_PROCESSING, PAYMENT_FAILED,
        PROCESSING, SHIPPED, DELIVERED, CANCELLED, REFUNDED
    }

    @Data
    @Builder
    public static class OrderItem implements Serializable {
        private String productId;
        private String productName;
        private int quantity;
        private BigDecimal unitPrice;
        private BigDecimal subtotal;
    }

    @Data
    @Builder
    public static class PaymentDetails implements Serializable {
        private String transactionId;
        private String paymentMethod;
        private String paymentStatus;
        private Instant processedAt;
    }
}
