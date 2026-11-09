package dev.flyermiles.recon;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * Counts the committed records in a topic by reading it end to end with read_committed. Offset
 * arithmetic would be wrong for transactional topics: commit/abort markers take up offsets too.
 */
final class TopicCounter {

  private final String bootstrap;

  TopicCounter(String bootstrap) {
    this.bootstrap = bootstrap;
  }

  long count(String topic) {
    var props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5000);
    props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 64 * 1024 * 1024);
    String jaas = System.getenv("KAFKA_SASL_JAAS_CONFIG");
    if (jaas != null && !jaas.isBlank()) { // Strimzi KafkaUser (SCRAM) on Kubernetes
      props.put("security.protocol", "SASL_PLAINTEXT");
      props.put("sasl.mechanism", "SCRAM-SHA-512");
      props.put("sasl.jaas.config", jaas);
    }

    try (var consumer = new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
      List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
          .map(p -> new TopicPartition(topic, p.partition())).toList();
      consumer.assign(partitions);
      consumer.seekToBeginning(partitions);
      // With read_committed, endOffsets returns the last stable offset.
      Map<TopicPartition, Long> end = consumer.endOffsets(partitions);

      long count = 0;
      while (partitions.stream().anyMatch(tp -> consumer.position(tp) < end.get(tp))) {
        count += consumer.poll(Duration.ofMillis(500)).count();
      }
      return count;
    }
  }
}
