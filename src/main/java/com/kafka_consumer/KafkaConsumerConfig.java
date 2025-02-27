package com.kafka_consumer;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class KafkaConsumerConfig {
    private final Environment environment;


    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, environment.getProperty("spring.kafka.consumer.bootstrap-servers"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, environment.getProperty("spring.kafka.consumer.group-id"));
        props.put(JsonDeserializer.TRUSTED_PACKAGES, environment.getProperty("spring.kafka.consumer.properties.spring.json.trusted.packages"));

        return new DefaultKafkaConsumerFactory<>(props);
    }


    /**
     * <h2>Dead Letter Queue (DLQ) Handling in Kafka</h2>
     * <p>
     * A <b>Dead Letter Queue (DLQ)</b> is a mechanism in Kafka to handle messages
     * that <i>cannot be processed</i> by a consumer due to errors such as:
     * </p>
     * <ul>
     *     <li>Deserialization issues</li>
     *     <li>Missing fields</li>
     *     <li>Transient failures</li>
     *     <li>Application-specific exceptions</li>
     * </ul>
     *
     * <p>
     * Instead of discarding these messages or causing infinite retries, Kafka sends
     * them to a <b>separate topic</b> (Dead Letter Queue) for later analysis and manual reprocessing.
     * </p>
     *
     * <h3>How DLQ Works in Kafka?</h3>
     * <ol>
     *     <li>A consumer tries to process a message but fails (e.g., due to deserialization issues).</li>
     *     <li><code>Spring Kafka's DefaultErrorHandler</code> catches the failure.</li>
     *     <li>A <code>DeadLetterPublishingRecoverer</code> moves the failed message to a DLQ topic
     *         (e.g., <code>"product-created-event-topic.DLT"</code>).</li>
     *     <li>The message remains in the DLQ topic for debugging or reprocessing.</li>
     * </ol>
     */


    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            KafkaTemplate<String, Object> kafkaTemplate
    ) {

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                new DeadLetterPublishingRecoverer(kafkaTemplate), // Sends failed messages to a Dead Letter Queue (DLQ)
                new FixedBackOff(5000, 3) // Retries the failed message every 5000ms (5s), up to 3 times
        );
        // Define an exception that should NOT be retried
        errorHandler.addNotRetryableExceptions(NotRetryableException.class);
        // If this exception occurs, the message is immediately sent to the Dead Letter Queue (DLQ)
        // without retrying. This is useful for known failures that cannot be resolved by retrying (e.g., validation errors).

        // Define an exception that SHOULD be retried
        errorHandler.addRetryableExceptions(RetryableException.class);
        // If this exception occurs, the consumer will retry processing the message based on the defined backoff strategy (3 attempts with a 5s delay).
        // This is useful for transient issues (e.g., temporary network failures, database downtime).

        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }

    @Bean
    KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, environment.getProperty("spring.kafka.consumer.bootstrap-servers"));
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return new DefaultKafkaProducerFactory<>(props);
    }
}
