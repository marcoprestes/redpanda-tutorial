package org.vectorized.redpanda.tutorial.consumer;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.vectorized.redpanda.tutorial.entity.Order;

@Slf4j
@Getter
@Component
public class ConsumerListeners {

    @Value("${order-updates.topic}")
    private String orderUpdatesTopic;

    @Value("${cancelled-orders.topic}")
    private String cancelledOrdersTopic;

    @Value("${pending-orders.topic}")
    private String pendingOrdersTopic;

    private String payload = null;

    private String topic = null;

    @KafkaListener(topics = "${order-updates.topic}", containerFactory = "orderUpdatesKafkaListenerContainerFactory")
    public void orderUpdatesListener(Order order) {
        this.payload = order.toString();
        this.topic = orderUpdatesTopic;
        log.info("Received order update: {}", order);
    }

    @KafkaListener(topics = "${pending-orders.topic}", containerFactory = "pendingOrdersKafkaListenerContainerFactory")
    public void pendingOrdersListener(Order order) {
        this.payload = order.toString();
        this.topic = pendingOrdersTopic;
        log.info("Received pending order: {}", order);
    }

    @KafkaListener(topics = "${cancelled-orders.topic}", containerFactory = "cancelledOrdersKafkaListenerContainerFactory")
    public void cancelledOrdersListener(Order order) {
        this.payload = order.toString();
        this.topic = cancelledOrdersTopic;
        log.info("Received cancelled order: {}", order);
    }

}
