package com.kafka_consumer;

import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessEventRepository extends JpaRepository<ProcessEventEntity, Long> {
    ProcessEventEntity findByMessageId(String messageId);
}
