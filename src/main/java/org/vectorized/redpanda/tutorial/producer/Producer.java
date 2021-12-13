package org.vectorized.redpanda.tutorial.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.vectorized.redpanda.tutorial.entity.Order;

@Slf4j
@Component
@RequiredArgsConstructor
public class Producer {

    @Value("${order-updates.topic}")
    private String orderUpdatesTopic;

    @Value("${cancelled-orders.topic}")
    private String cancelledOrdersTopic;

    @Value("${pending-orders.topic}")
    private String pendingOrdersTopic;

    private final KafkaTemplate<String, Order> kafkaTemplate;

    public void send(Order order){
        log.info("Order sent: {}", order);

        if (order.getOrderStatus() == null) {
            log.error("Invalid order status: {}", order);
            return;
        }

        switch(order.getOrderStatus()) {
            case PENDING:
                System.out.println("> sending to pending topic");
                kafkaTemplate.send(pendingOrdersTopic, order);
                break;
            case CANCELLED:
                System.out.println("> sending to cancelled topic");
                kafkaTemplate.send(cancelledOrdersTopic, order);
                break;
            default:
                log.info("Sending to order updates topic");
        }

        kafkaTemplate.send(orderUpdatesTopic, order);
    }

}
