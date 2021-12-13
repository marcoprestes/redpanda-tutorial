package org.vectorized.redpanda.tutorial;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import org.junit.Before;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.test.util.ReflectionTestUtils;
import org.vectorized.redpanda.tutorial.consumer.ConsumerListeners;
import org.vectorized.redpanda.tutorial.entity.Order;
import org.vectorized.redpanda.tutorial.enums.OrderStatus;
import org.vectorized.redpanda.tutorial.producer.Producer;

class EmbeddedKafkaIntegrationTest {

    protected static final short DEFAULT_REPLICATION = 1;


    protected static final String BOOTSTRAP_SERVERS_PROP = "kafka.bootstrap.servers";

    protected static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";

    @Autowired
    private ConsumerListeners consumer;

    private Producer producer;

    @Before
    public void init() {
        ProducerFactory<String, Order> producerFactory = () -> new KafkaProducer<String, Order>(getProducerProperties());
        producer = new Producer(new KafkaTemplate<>(producerFactory));
    }

    @Test
    public void givenEmbeddedKafkaBroker_whenSendingtoDefaultTemplate_thenMessageReceived() throws Exception {
        ProducerFactory<String, Order> producerFactory = () -> new KafkaProducer<String, Order>(getProducerProperties());
        producer = new Producer(new KafkaTemplate<String, Order>(producerFactory));
        ReflectionTestUtils.setField(producer, "orderUpdatesTopic", "order_updates");

        Long currentTopicOffset = getEndOffset("", "order_updates", 0);

        Order order = new Order();
        order.setId(1L);
        order.setDescription("order 1 description");
        order.setValue(10.00);
        order.setOrderStatus(OrderStatus.NEW);
        producer.send(order);

        assertEquals(currentTopicOffset + 1, getEndOffset("", "order_updates", 0));

        //assertThat(consumer.getPayload(), containsString(order.getDescription()));
       // assertThat(consumer.getTopic(), equalTo(consumer.getOrderUpdatesTopic()));
    }

    public String getPrefixedTopic(String topic) {
        return "test-" + System.currentTimeMillis() + "-" + topic;
    }

    //@Test
    public void testEndOffset() {
        String topic = getPrefixedTopic("testSendMessage");
        int partitions = 1;
        int records = 1;
        createTopic(topic, partitions);
        // produces some records

        ProducerFactory<String, Order> producerFactory = () -> new KafkaProducer<String, Order>(getProducerProperties());
        producer = new Producer(new KafkaTemplate<>(producerFactory));
        // Kafka returns 1, RedPanda 2 because the first offset is 1 instead of 0
        assertEquals(1, getEndOffset("orderUpdates", topic, 0));
        deleteTopic(topic);
    }

    protected long getEndOffset(String group, String topic, int partition) {
        try (KafkaConsumer<String, Order> consumer = new KafkaConsumer(getConsumerProperties(group))) {
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(Collections.singleton(topicPartition));
            return endOffsets.get(topicPartition);
        }
    }

    public static String getBootstrapServers() {
        String bootstrapServers = System.getProperty(BOOTSTRAP_SERVERS_PROP, DEFAULT_BOOTSTRAP_SERVERS);
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            bootstrapServers = DEFAULT_BOOTSTRAP_SERVERS;
        }
        return bootstrapServers;
    }

    protected Properties getAdminProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        return props;
    }

    protected Properties getProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.springframework.kafka.support.serializer.JsonSerializer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // default
        props.put(ProducerConfig.RETRIES_CONFIG, 0); // default
        props.put(ProducerConfig.ACKS_CONFIG, "1"); // default
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384); // default
        return props;
    }

    protected Properties getConsumerProperties(String group) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.springframework.kafka.support.serializer.JsonDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        return props;
    }

    protected boolean topicExists(String topic) {
        try (AdminClient adminClient = AdminClient.create(getAdminProperties())) {
            adminClient.describeTopics(Collections.singletonList(topic)).values().get(topic).get();
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                return false;
            }
            throw new RuntimeException(e);
        }
    }

    protected void createTopic(String topic, int partitions) {
        if (topicExists(topic)) {
            System.out.println("Existing topic " + topic);
            return;
        }
        try (AdminClient adminClient = AdminClient.create(getAdminProperties())) {
            CreateTopicsResult ret = adminClient.createTopics(
                    Collections.singletonList(new NewTopic(topic, partitions, DEFAULT_REPLICATION)));
            ret.all().get(2, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException("Unable to create topics " + topic + " within the timeout", e);
        }
        System.out.println("CREATE topic " + topic);
    }

    protected void deleteTopic(String topic) {
        try (AdminClient adminClient = AdminClient.create(getAdminProperties())) {
            adminClient.deleteTopics(Collections.singleton(topic));
            System.out.println("DELETE topic " + topic);
        }
    }

    protected static class TestCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                System.out.println("Error while producing message to topic :" + recordMetadata);
                e.printStackTrace();
            } else {
                System.out.printf("Message appended to topic: %s partition:%s  offset:%s%n",
                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
            }
        }
    }

}