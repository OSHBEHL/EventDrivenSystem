package com.example.eventdriven.service;

import com.example.eventdriven.config.RedisConfig;
import com.example.eventdriven.domain.model.Order;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.Caching;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

/**
 * Cache abstraction over Redis.
 *
 * <p>Uses Spring's @Cacheable / @CacheEvict annotations so the cache layer
 * is completely transparent to callers. All keys are namespaced by cache name.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CacheService {

    /**
     * Cache a single order by its MongoDB ID.
     */
    @Cacheable(value = RedisConfig.CACHE_ORDERS, key = "#orderId", unless = "#result == null")
    public Optional<Order> getCachedOrder(String orderId) {
        // Returns empty — actual DB fetch happens in OrderService
        return Optional.empty();
    }

    /**
     * Cache a single order by its business order number.
     */
    @Cacheable(value = RedisConfig.CACHE_ORDER_BY_NUMBER, key = "#orderNumber", unless = "#result == null")
    public Optional<Order> getCachedOrderByNumber(String orderNumber) {
        return Optional.empty();
    }

    /**
     * Cache the list of orders for a given customer.
     */
    @Cacheable(value = RedisConfig.CACHE_CUSTOMER_ORDERS, key = "#customerId", unless = "#result == null || #result.isEmpty()")
    public List<Order> getCachedCustomerOrders(String customerId) {
        return List.of();
    }

    /**
     * Evict all cache entries for a given order across all cache regions.
     * Called whenever an order is mutated (status change, cancellation, payment update).
     */
    @Caching(evict = {
        @CacheEvict(value = RedisConfig.CACHE_ORDERS,            key = "#orderId"),
        @CacheEvict(value = RedisConfig.CACHE_ORDER_BY_NUMBER,   key = "#orderNumber"),
        @CacheEvict(value = RedisConfig.CACHE_CUSTOMER_ORDERS,   key = "#customerId")
    })
    public void evictOrder(String orderId, String orderNumber, String customerId) {
        log.debug("Cache evicted for orderId={}, orderNumber={}, customerId={}",
                orderId, orderNumber, customerId);
    }

    /**
     * Evict all entries in the customer-orders cache region for a given customer.
     */
    @CacheEvict(value = RedisConfig.CACHE_CUSTOMER_ORDERS, key = "#customerId")
    public void evictCustomerOrders(String customerId) {
        log.debug("Customer orders cache evicted for customerId={}", customerId);
    }

    /**
     * Nuke all cache regions — useful for admin/testing.
     */
    @Caching(evict = {
        @CacheEvict(value = RedisConfig.CACHE_ORDERS,            allEntries = true),
        @CacheEvict(value = RedisConfig.CACHE_ORDER_BY_NUMBER,   allEntries = true),
        @CacheEvict(value = RedisConfig.CACHE_CUSTOMER_ORDERS,   allEntries = true)
    })
    public void evictAll() {
        log.warn("All caches evicted");
    }
}
