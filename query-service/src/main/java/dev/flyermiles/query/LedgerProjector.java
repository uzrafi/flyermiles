package dev.flyermiles.query;

import dev.flyermiles.avro.EntryType;
import dev.flyermiles.avro.LedgerEvent;
import dev.flyermiles.avro.Topics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Applies ledger.events to the read model. Delivery is at-least-once, so the projection is
 * idempotent: statement lines are keyed by (member_id, seq) and a balance only moves forward in seq.
 * Each Kafka batch is one Postgres transaction and offsets are committed after it, so a crash replays
 * the batch and the duplicates are absorbed.
 */
@Component
public class LedgerProjector {

  public static final String LISTENER_ID = "ledger-projector";
  private static final Logger log = LoggerFactory.getLogger(LedgerProjector.class);

  private final JdbcTemplate jdbc;
  private final TransactionTemplate tx;
  private final BalanceCache cache;
  private final EarnLatency latency;
  private final RebuildTracker rebuild;
  private final Counter applied;
  private final Counter duplicates;
  private final Counter gaps;

  public LedgerProjector(
      JdbcTemplate jdbc, TransactionTemplate tx, BalanceCache cache, EarnLatency latency,
      RebuildTracker rebuild, MeterRegistry meters) {
    this.jdbc = jdbc;
    this.tx = tx;
    this.cache = cache;
    this.latency = latency;
    this.rebuild = rebuild;
    this.applied = Counter.builder("flyermiles_projection_events_total").tag("result", "applied").register(meters);
    this.duplicates = Counter.builder("flyermiles_projection_events_total").tag("result", "duplicate").register(meters);
    this.gaps = Counter.builder("flyermiles_projection_gaps_total")
        .description("events whose seq skipped ahead of the projected seq (should stay 0)").register(meters);
  }

  @KafkaListener(id = LISTENER_ID, topics = Topics.LEDGER_EVENTS, groupId = "query-service", batch = "true")
  public void project(List<ConsumerRecord<String, LedgerEvent>> records) {
    List<LedgerEvent> events = new ArrayList<>(records.size());
    for (var r : records) {
      if (r.value() == null) {
        log.error("undeserialisable record at {}-{}@{}, skipped", r.topic(), r.partition(), r.offset());
      } else {
        events.add(r.value());
      }
    }

    List<LedgerEvent> inserted = events.isEmpty() ? List.of() : tx.execute(status -> apply(events));

    // After commit: invalidate the cache, then measure how long the points took to become visible.
    Set<UUID> touched = new HashSet<>();
    for (LedgerEvent e : events) touched.add(UUID.fromString(e.getMemberId()));
    cache.evict(touched);

    if (!rebuild.running()) {
      long now = System.currentTimeMillis();
      for (LedgerEvent e : inserted) {
        if (e.getType() == EntryType.EARNED && e.getSourceTimestampMs() != null) {
          latency.record(now - e.getSourceTimestampMs());
        }
      }
    }
    applied.increment(inserted.size());
    duplicates.increment(events.size() - inserted.size());
    rebuild.onBatch(records);
  }

  private List<LedgerEvent> apply(List<LedgerEvent> events) {
    int[] counts = flatten(jdbc.batchUpdate(
        """
        INSERT INTO statement_lines (member_id, seq, entry_id, type, points, balance_after, occurred_at)
        VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING
        """,
        events, 500, (ps, e) -> {
          ps.setObject(1, UUID.fromString(e.getMemberId()));
          ps.setLong(2, e.getSeq());
          ps.setObject(3, UUID.fromString(e.getEntryId()));
          ps.setString(4, e.getType().name());
          ps.setLong(5, e.getPoints());
          ps.setLong(6, e.getBalanceAfter());
          ps.setTimestamp(7, new Timestamp(e.getOccurredAtMs()));
        }));

    List<LedgerEvent> inserted = new ArrayList<>();
    for (int i = 0; i < events.size(); i++) {
      if (counts[i] != 0) inserted.add(events.get(i));
    }

    // Latest event per member in this batch: its balanceAfter already includes the earlier ones.
    Map<String, LedgerEvent> latest = new HashMap<>();
    for (LedgerEvent e : events) latest.merge(e.getMemberId(), e, (a, b) -> a.getSeq() >= b.getSeq() ? a : b);

    jdbc.batchUpdate(
        """
        INSERT INTO member_balances AS b (member_id, balance, last_seq, updated_at)
        VALUES (?, ?, ?, now())
        ON CONFLICT (member_id) DO UPDATE
          SET balance = EXCLUDED.balance, last_seq = EXCLUDED.last_seq, updated_at = now()
          WHERE b.last_seq < EXCLUDED.last_seq
        """,
        new ArrayList<>(latest.values()), 500, (ps, e) -> {
          ps.setObject(1, UUID.fromString(e.getMemberId()));
          ps.setLong(2, e.getBalanceAfter());
          ps.setLong(3, e.getSeq());
        });

    detectGaps(inserted);
    return inserted;
  }

  private static int[] flatten(int[][] perBatch) {
    int n = 0;
    for (int[] b : perBatch) n += b.length;
    int[] out = new int[n];
    int i = 0;
    for (int[] b : perBatch) {
      System.arraycopy(b, 0, out, i, b.length);
      i += b.length;
    }
    return out;
  }

  /** A jump in seq within a batch means an event was lost upstream. Reconciliation would catch it too. */
  private void detectGaps(List<LedgerEvent> inserted) {
    Map<String, long[]> range = new HashMap<>(); // min seq, max seq, count
    for (LedgerEvent e : inserted) {
      range.merge(e.getMemberId(), new long[] {e.getSeq(), e.getSeq(), 1},
          (a, b) -> new long[] {Math.min(a[0], b[0]), Math.max(a[1], b[1]), a[2] + b[2]});
    }
    range.forEach((member, r) -> {
      if (r[1] - r[0] + 1 != r[2]) {
        gaps.increment();
        log.warn("seq gap for member {} within batch: {}..{} but {} events", member, r[0], r[1], r[2]);
      }
    });
  }
}
