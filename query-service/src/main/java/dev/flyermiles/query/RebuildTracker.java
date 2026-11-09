package dev.flyermiles.query;

import dev.flyermiles.avro.Topics;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

/**
 * Drops the read model and replays it from ledger.events, timing how long it takes to catch up to
 * the end offsets captured at the start. Run it with a single query-service replica: the consumer
 * group has to be empty to rewind its offsets.
 */
@Component
public class RebuildTracker {

  private static final Logger log = LoggerFactory.getLogger(RebuildTracker.class);

  private final KafkaListenerEndpointRegistry listeners;
  private final KafkaAdmin kafkaAdmin;
  private final JdbcTemplate jdbc;
  private final BalanceCache cache;

  private volatile boolean running;
  private volatile Instant startedAt;
  private volatile Duration elapsed;
  private volatile long target;
  private final Map<TopicPartition, Long> endOffsets = new ConcurrentHashMap<>();
  private final Map<TopicPartition, Long> startOffsets = new ConcurrentHashMap<>();
  private final Map<TopicPartition, Long> reached = new ConcurrentHashMap<>();

  public RebuildTracker(
      KafkaListenerEndpointRegistry listeners, KafkaAdmin kafkaAdmin, JdbcTemplate jdbc, BalanceCache cache) {
    this.listeners = listeners;
    this.kafkaAdmin = kafkaAdmin;
    this.jdbc = jdbc;
    this.cache = cache;
  }

  public boolean running() {
    return running;
  }

  public synchronized Map<String, Object> start() throws Exception {
    if (running) return status();
    MessageListenerContainer container = listeners.getListenerContainer(LedgerProjector.LISTENER_ID);
    container.stop();
    while (container.isRunning()) Thread.sleep(50);

    try (Admin admin = Admin.create(kafkaAdmin.getConfigurationProperties())) {
      var partitions = admin.describeTopics(List.of(Topics.LEDGER_EVENTS)).allTopicNames().get()
          .get(Topics.LEDGER_EVENTS).partitions().stream()
          .map(p -> new TopicPartition(Topics.LEDGER_EVENTS, p.partition())).toList();
      var earliest = admin.listOffsets(partitions.stream().collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.earliest())))
          .all().get(30, TimeUnit.SECONDS);
      var latest = admin.listOffsets(partitions.stream().collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest())))
          .all().get(30, TimeUnit.SECONDS);

      startOffsets.clear();
      endOffsets.clear();
      reached.clear();
      long total = 0;
      Map<TopicPartition, OffsetAndMetadata> rewind = new HashMap<>();
      for (TopicPartition tp : partitions) {
        long from = earliest.get(tp).offset();
        long to = latest.get(tp).offset();
        startOffsets.put(tp, from);
        endOffsets.put(tp, to);
        reached.put(tp, from);
        total += to - from;
        rewind.put(tp, new OffsetAndMetadata(from));
      }
      target = total;

      jdbc.execute("TRUNCATE statement_lines, member_balances");
      cache.clear();
      admin.alterConsumerGroupOffsets("query-service", rewind).all().get(30, TimeUnit.SECONDS);
    }

    running = true;
    elapsed = null;
    startedAt = Instant.now();
    log.info("rebuilding read model from {} ledger events", target);
    container.start();
    return status();
  }

  void onBatch(List<? extends ConsumerRecord<?, ?>> records) {
    if (!running) return;
    for (var r : records) {
      reached.merge(new TopicPartition(r.topic(), r.partition()), r.offset() + 1, Math::max);
    }
    boolean done = endOffsets.entrySet().stream().allMatch(e -> reached.getOrDefault(e.getKey(), 0L) >= e.getValue());
    if (done) {
      elapsed = Duration.between(startedAt, Instant.now());
      running = false;
      log.info("rebuild complete: {} events in {} ms", target, elapsed.toMillis());
    }
  }

  public Map<String, Object> status() {
    long processed = reached.entrySet().stream()
        .mapToLong(e -> Math.min(e.getValue(), endOffsets.getOrDefault(e.getKey(), 0L)) - startOffsets.getOrDefault(e.getKey(), 0L))
        .sum();
    Duration d = elapsed != null ? elapsed : startedAt == null ? Duration.ZERO : Duration.between(startedAt, Instant.now());
    var out = new LinkedHashMap<String, Object>();
    out.put("running", running);
    out.put("complete", startedAt != null && !running);
    out.put("targetEvents", target);
    out.put("processedEvents", processed);
    out.put("elapsedMs", d.toMillis());
    out.put("eventsPerSecond", d.toMillis() == 0 ? 0 : processed * 1000 / d.toMillis());
    return out;
  }
}
