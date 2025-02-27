package com.kafka_consumer;

import com.shared_core_library.ProductCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@KafkaListener(topics = "product-created-event-topic")
@RequiredArgsConstructor
public class ProductCreatedEventHandler {

    @KafkaHandler
    public void handler(ProductCreatedEvent productCreatedEvent) {
        log.info("*************Received a new event with ID: {} *************", productCreatedEvent.getTitle());
    }
}
