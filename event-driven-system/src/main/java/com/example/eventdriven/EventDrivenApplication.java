package com.example.eventdriven;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.data.mongodb.config.EnableMongoAuditing;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
@EnableCaching
@EnableMongoAuditing
public class EventDrivenApplication {

    public static void main(String[] args) {
        SpringApplication.run(EventDrivenApplication.class, args);
    }
}
