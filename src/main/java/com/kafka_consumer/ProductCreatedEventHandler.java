package com.kafka_consumer;

import com.shared_core_library.ProductCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Slf4j
@Component
@KafkaListener(topics = "product-created-event-topic")
@RequiredArgsConstructor
public class ProductCreatedEventHandler {
    private final ProcessEventRepository repository;
    private final KafkaTemplate<?, ?> kafkaTemplate;

    @Transactional
    @KafkaHandler
    public void handler(
            @Payload ProductCreatedEvent productCreatedEvent,
            @Header(value = "messageId") String messageId,
            @Header(KafkaHeaders.RECEIVED_KEY) String messageKey) {
        log.info("*************Received a new event with ID: {} *************", productCreatedEvent.getTitle());
        log.info("*************Received a new event with productId: {} *************", productCreatedEvent.getProductId());

        Optional.ofNullable(repository.findByMessageId(messageId))
                .ifPresentOrElse(processEventEntity -> {
                            log.info("ProcessEventEntity with this messageId {}, present already", processEventEntity.getMessageId());
                        },
                        () -> {
                            val event = repository.save(new ProcessEventEntity(messageId, productCreatedEvent.getProductId()));
                            log.info("New ProcessEventEntity with this messageId {}, created", event.getMessageId());
                        }
                );

       // localTransaction(productCreatedEvent, messageId);
    }

    private void localTransaction(ProductCreatedEvent productCreatedEvent, String messageId) {
        log.info("BEFORE LOCAL TRANSACTION");
        try {
            kafkaTemplate.executeInTransaction(operations -> {
                log.info("ProcessEventEntity with this messageId {} was caught in local transaction", messageId);
                return productCreatedEvent;
            });
        } catch (Exception e) {
            log.error("Transaction failed", e);
        }
        log.info("AFTER LOCAL TRANSACTION");
    }
}
