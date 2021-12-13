package org.vectorized.redpanda.tutorial;

import org.apache.kafka.clients.producer.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.test.util.ReflectionTestUtils;
import org.vectorized.redpanda.tutorial.entity.Order;
import org.vectorized.redpanda.tutorial.enums.OrderStatus;
import org.vectorized.redpanda.tutorial.producer.Producer;

class RedpandaIntegrationTest {

    private Producer producer;

    @BeforeEach
    public void init() {
        ProducerFactory<String, Order> producerFactory = () -> new KafkaProducer<>(TestConfig.getProducerProperties());
        producer = new Producer(new KafkaTemplate<>(producerFactory));

        ReflectionTestUtils.setField(producer, "orderUpdatesTopic", TestConfig.ORDER_UPDATES_TOPIC);
        ReflectionTestUtils.setField(producer, "cancelledOrdersTopic", TestConfig.CANCELLED_ORDERS_TOPIC);
        ReflectionTestUtils.setField(producer, "pendingOrdersTopic", TestConfig.PENDING_ORDERS_TOPIC);

        TestConfig.deleteTopics();
        for(String topic : TestConfig.getTopics()) {
            TestConfig.createTopic(topic);
        }
    }

    @Test
    public void givenLocalRedpandaBroker_whenSendingInvalidOrder_thenWontBeSent() throws Exception {
        long currentOrderUpdatesOffset = getTopicOffset(TestConfig.ORDER_UPDATES_TOPIC);
        long currentPendingOrdersOffset = getTopicOffset(TestConfig.PENDING_ORDERS_TOPIC);
        long currentCancelledOrdersOffset = getTopicOffset(TestConfig.CANCELLED_ORDERS_TOPIC);

        Order order = new Order();
        order.setId(1L);
        order.setDescription("order 1 description");
        order.setValue(10.00);
        producer.send(order);
        Thread.sleep(2000);

        Assertions.assertEquals(currentOrderUpdatesOffset, getTopicOffset(TestConfig.ORDER_UPDATES_TOPIC));
        Assertions.assertEquals(currentPendingOrdersOffset, getTopicOffset(TestConfig.PENDING_ORDERS_TOPIC));
        Assertions.assertEquals(currentCancelledOrdersOffset, getTopicOffset(TestConfig.CANCELLED_ORDERS_TOPIC));
    }

    @Test
    public void givenLocalRedpandaBroker_whenSendingNewOrder_thenMessageReceivedOnOrderUpdatesTopicOnly() throws Exception {
        long currentOrderUpdatesOffset = getTopicOffset(TestConfig.ORDER_UPDATES_TOPIC);
        long currentPendingOrdersOffset = getTopicOffset(TestConfig.PENDING_ORDERS_TOPIC);
        long currentCancelledOrdersOffset = getTopicOffset(TestConfig.CANCELLED_ORDERS_TOPIC);

        Order order = new Order();
        order.setId(1L);
        order.setDescription("order 1 description");
        order.setValue(10.00);
        order.setOrderStatus(OrderStatus.NEW);
        producer.send(order);
        Thread.sleep(2000);

        Assertions.assertEquals(currentOrderUpdatesOffset + 1, getTopicOffset(TestConfig.ORDER_UPDATES_TOPIC));
        Assertions.assertEquals(currentPendingOrdersOffset, getTopicOffset(TestConfig.PENDING_ORDERS_TOPIC));
        Assertions.assertEquals(currentCancelledOrdersOffset, getTopicOffset(TestConfig.CANCELLED_ORDERS_TOPIC));
    }

    @Test
    public void givenLocalRedpandaBroker_whenSendingInHoldOrder_thenMessageReceivedOnOrderUpdatesTopicOnly() throws Exception {
        long currentOrderUpdatesOffset = getTopicOffset(TestConfig.ORDER_UPDATES_TOPIC);
        long currentPendingOrdersOffset = getTopicOffset(TestConfig.PENDING_ORDERS_TOPIC);
        long currentCancelledOrdersOffset = getTopicOffset(TestConfig.CANCELLED_ORDERS_TOPIC);

        Order order = new Order();
        order.setId(1L);
        order.setDescription("order 1 description");
        order.setValue(10.00);
        order.setOrderStatus(OrderStatus.ON_HOLD);
        producer.send(order);
        Thread.sleep(2000);

        Assertions.assertEquals(currentOrderUpdatesOffset + 1, getTopicOffset(TestConfig.ORDER_UPDATES_TOPIC));
        Assertions.assertEquals(currentPendingOrdersOffset, getTopicOffset(TestConfig.PENDING_ORDERS_TOPIC));
        Assertions.assertEquals(currentCancelledOrdersOffset, getTopicOffset(TestConfig.CANCELLED_ORDERS_TOPIC));
    }

    @Test
    public void givenLocalRedpandaBroker_whenSendingCompletedOrder_thenMessageReceivedOnOrderUpdatesTopicOnly() throws Exception {
        long currentOrderUpdatesOffset = getTopicOffset(TestConfig.ORDER_UPDATES_TOPIC);
        long currentPendingOrdersOffset = getTopicOffset(TestConfig.PENDING_ORDERS_TOPIC);
        long currentCancelledOrdersOffset = getTopicOffset(TestConfig.CANCELLED_ORDERS_TOPIC);

        Order order = new Order();
        order.setId(1L);
        order.setDescription("order 1 description");
        order.setValue(10.00);
        order.setOrderStatus(OrderStatus.COMPLETED);
        producer.send(order);
        Thread.sleep(2000);

        Assertions.assertEquals(currentOrderUpdatesOffset + 1, getTopicOffset(TestConfig.ORDER_UPDATES_TOPIC));
        Assertions.assertEquals(currentPendingOrdersOffset, getTopicOffset(TestConfig.PENDING_ORDERS_TOPIC));
        Assertions.assertEquals(currentCancelledOrdersOffset, getTopicOffset(TestConfig.CANCELLED_ORDERS_TOPIC));
    }

    @Test
    public void givenLocalRedpandaBroker_whenSendingPendingOrder_thenMessageReceivedOnOrderUpdatesAndPendingOrdersTopics() throws Exception {
        long currentOrderUpdatesOffset = getTopicOffset(TestConfig.ORDER_UPDATES_TOPIC);
        long currentPendingOrdersOffset = getTopicOffset(TestConfig.PENDING_ORDERS_TOPIC);
        long currentCancelledOrdersOffset = getTopicOffset(TestConfig.CANCELLED_ORDERS_TOPIC);
        Order order = new Order();
        order.setId(2L);
        order.setDescription("order 2 description");
        order.setValue(19.00);
        order.setOrderStatus(OrderStatus.PENDING);
        producer.send(order);
        Thread.sleep(2000);

        Assertions.assertEquals(currentOrderUpdatesOffset + 1, getTopicOffset(TestConfig.ORDER_UPDATES_TOPIC));
        Assertions.assertEquals(currentPendingOrdersOffset + 1, getTopicOffset(TestConfig.PENDING_ORDERS_TOPIC));
        Assertions.assertEquals(currentCancelledOrdersOffset, getTopicOffset(TestConfig.CANCELLED_ORDERS_TOPIC));
    }

    @Test
    public void givenLocalRedpandaBroker_whenSendingCancelledOrder_thenMessageReceivedOnOrderUpdatesAndCancelledOrdersTopics() throws Exception {
        long currentOrderUpdatesOffset = getTopicOffset(TestConfig.ORDER_UPDATES_TOPIC);
        long currentPendingOrdersOffset = getTopicOffset(TestConfig.PENDING_ORDERS_TOPIC);
        long currentCancelledOrdersOffset = getTopicOffset(TestConfig.CANCELLED_ORDERS_TOPIC);

        Order order = new Order();
        order.setId(2L);
        order.setDescription("order 2 description");
        order.setValue(19.00);
        order.setOrderStatus(OrderStatus.CANCELLED);
        producer.send(order);
        Thread.sleep(2000);

        Assertions.assertEquals(currentOrderUpdatesOffset + 1, getTopicOffset(TestConfig.ORDER_UPDATES_TOPIC));
        Assertions.assertEquals(currentPendingOrdersOffset, getTopicOffset(TestConfig.PENDING_ORDERS_TOPIC));
        Assertions.assertEquals(currentCancelledOrdersOffset + 1, getTopicOffset(TestConfig.CANCELLED_ORDERS_TOPIC));
    }

    @AfterAll
    public static void resetTopics() {
        TestConfig.deleteTopics();
        for(String topic : TestConfig.getTopics()) {
            TestConfig.createTopic(topic);
        }
    }

    private long getTopicOffset(String topic) {
        return TestConfig.getEndOffset("", topic, 0);
    }

}