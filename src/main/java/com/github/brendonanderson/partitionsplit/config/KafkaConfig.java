package com.github.brendonanderson.partitionsplit.config;

import com.github.brendonanderson.partitionsplit.dto.Product;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Configuration
public class KafkaConfig {

    // these are the properties set in the application.yaml
    private final KafkaProperties kafkaProperties;
    private final Integer numThreads;

    public KafkaConfig(KafkaProperties kafkaProperties, @Value("${app.numThreads}") Integer numThreads) {
        this.kafkaProperties = kafkaProperties;
        this.numThreads = numThreads;
    }

    @Bean
    public List<KafkaConsumer<String, Product>> kafkaConsumers() {
        // need different group id for each consumer
        return IntStream.range(0, numThreads)
                .mapToObj(i -> {
                    Map<String, Object> props = kafkaProperties.buildConsumerProperties();
                    props.put(ConsumerConfig.GROUP_ID_CONFIG, props.get(ConsumerConfig.GROUP_ID_CONFIG) + "-" + i);
                    return new KafkaConsumer<String, Product>(props);
                })
                .collect(Collectors.toList());
    }
}
