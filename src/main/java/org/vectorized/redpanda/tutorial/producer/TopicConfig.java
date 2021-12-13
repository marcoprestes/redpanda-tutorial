package org.vectorized.redpanda.tutorial.producer;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

//@Configuration
public class TopicConfig {

    @Value(value = "${spring.kafka.producer.bootstrap-servers}")
    private String bootstrapAddress;

    @Value("${order-updates.topic}")
    private String orderUpdatesTopic;

    @Value("${cancelled-orders.topic")
    private String cancelledOrdersTopic;

    @Value("${pending-orders.topic}")
    private String pendingOrdersTopic;

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopic topic1() {
        return new NewTopic(orderUpdatesTopic, 1, (short) 1);
    }

    @Bean
    public NewTopic topic2() {
        return new NewTopic(cancelledOrdersTopic, 1, (short) 1);
    }

    @Bean
    public NewTopic topic3() {
        return new NewTopic(pendingOrdersTopic, 1, (short) 1);
    }

}
