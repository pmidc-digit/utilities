package org.egov.epass.chat.config;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value(value = "${kafka.config.bootstrap_server_config}")
    private String bootstrapAddress;
    @Value("${spring.kafka.consumer.group-id}")
    private String consumerGroupId;
    @Value("${kafka.consumer.poll.ms.for.karix.send}")
    private Integer kafkaConsumerPollMs;
    @Value("${kafka.producer.linger.ms}")
    private Integer kafkaProducerLingerMs;
    @Value("${kafka.consumer.threads.for.karix.send}")
    private Integer kafkaConsumerThreads;
    @Value("${kafka.consumer.max.poll.records.for.karix.send}")
    private String maxBatchSize;

    @Bean
    public ConsumerFactory<String, JsonNode> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, kafkaConsumerPollMs);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxBatchSize);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, JsonNode> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, JsonNode> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setBatchListener(true);
        factory.setConcurrency(kafkaConsumerThreads);
        return factory;
    }

    @Bean
    public ProducerFactory<String, JsonNode> producerFactoryJson() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, JsonNode> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactoryJson());
    }

}
