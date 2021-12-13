package org.vectorized.redpanda.tutorial;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.vectorized.redpanda.tutorial.entity.Order;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TestConfig {

  protected static final short DEFAULT_REPLICATION = 1;

  protected static final String BOOTSTRAP_SERVERS_PROP = "kafka.bootstrap.servers";

  protected static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:51768";

  protected static final String ORDER_UPDATES_TOPIC = "order_updates";

  protected static final String PENDING_ORDERS_TOPIC = "pending_orders";

  protected static final String CANCELLED_ORDERS_TOPIC = "cancelled_orders";

  public static String getBootstrapServers() {
    String bootstrapServers = System.getProperty(BOOTSTRAP_SERVERS_PROP, DEFAULT_BOOTSTRAP_SERVERS);
    if (bootstrapServers == null || bootstrapServers.isEmpty()) {
      bootstrapServers = DEFAULT_BOOTSTRAP_SERVERS;
    }
    return bootstrapServers;
  }

  protected static Properties getAdminProperties() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
    return props;
  }

  protected static Properties getProducerProperties() {
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

  protected static Properties getConsumerProperties(String group) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            "org.springframework.kafka.support.serializer.JsonDeserializer");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
    return props;
  }

  protected static long getEndOffset(String group, String topic, int partition) {
    try (KafkaConsumer<String, Order> consumer = new KafkaConsumer(getConsumerProperties(group))) {
      TopicPartition topicPartition = new TopicPartition(topic, partition);
      Map<TopicPartition, Long> endOffsets = consumer.endOffsets(Collections.singleton(topicPartition));
      return endOffsets.get(topicPartition);
    }
  }

  protected static boolean topicExists(String topic) {
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

  protected static void createTopic(String topic) {
    if (topicExists(topic)) {
      System.out.println("Existing topic " + topic);
      return;
    }
    try (AdminClient adminClient = AdminClient.create(getAdminProperties())) {
      CreateTopicsResult ret = adminClient.createTopics(
              Collections.singletonList(new NewTopic(topic, 1, DEFAULT_REPLICATION)));
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

  protected static void deleteTopics() {
    try (AdminClient adminClient = AdminClient.create(getAdminProperties())) {
      adminClient.deleteTopics(getTopics());
    }
  }

  protected static List<String> getTopics() {
    return Arrays.asList(ORDER_UPDATES_TOPIC, PENDING_ORDERS_TOPIC, CANCELLED_ORDERS_TOPIC);
  }


}
