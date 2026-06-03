package com.example.eventdriven.repository;

import com.example.eventdriven.domain.model.Order;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Repository
public interface OrderRepository extends MongoRepository<Order, String> {

    Optional<Order> findByOrderNumber(String orderNumber);

    List<Order> findByCustomerId(String customerId);

    List<Order> findByStatus(Order.OrderStatus status);

    @Query("{ 'customerId': ?0, 'status': { $in: ?1 } }")
    List<Order> findByCustomerIdAndStatuses(String customerId, List<Order.OrderStatus> statuses);

    @Query("{ 'createdAt': { $gte: ?0, $lte: ?1 } }")
    List<Order> findByCreatedAtBetween(Instant from, Instant to);

    boolean existsByOrderNumber(String orderNumber);
}
