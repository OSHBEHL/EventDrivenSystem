package com.example.eventdriven.service;

import com.example.eventdriven.domain.model.Order;

import java.util.List;
import java.util.Optional;

public interface OrderService {

    Order createOrder(Order order);

    Order updateOrderStatus(String orderId, Order.OrderStatus newStatus, String updatedBy);

    Order cancelOrder(String orderId, String reason, String cancelledBy);

    Optional<Order> findById(String orderId);

    Optional<Order> findByOrderNumber(String orderNumber);

    List<Order> findByCustomerId(String customerId);

    List<Order> findByStatus(Order.OrderStatus status);
}
