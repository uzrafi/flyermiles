package dev.flyermiles.ledger.kafka;

import dev.flyermiles.avro.LedgerEvent;
import dev.flyermiles.avro.Topics;
import dev.flyermiles.ledger.LedgerProperties;
import dev.flyermiles.ledger.domain.LedgerEntry;
import dev.flyermiles.ledger.domain.LedgerRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Publishes committed ledger entries to {@code ledger.events}, in append order.
 *
 * <p>The publish queue is written by a trigger in the same transaction as the entry, so an entry can
 * never be committed without eventually being published. Each relay transaction sends a batch, waits
 * for the broker acks, then deletes the batch from the queue. A crash between the ack and the commit
 * re-sends the batch: delivery is at-least-once and the read side dedupes on (member_id, seq).
 * A transaction-scoped advisory lock keeps exactly one relay active across replicas.
 */
@Component
public class LedgerEventRelay {

  private static final Logger log = LoggerFactory.getLogger(LedgerEventRelay.class);

  private final LedgerRepository repo;
  private final TransactionTemplate tx;
  private final KafkaTemplate<String, LedgerEvent> kafka;
  private final LedgerProperties props;
  private final AtomicLong backlog = new AtomicLong();

  public LedgerEventRelay(
      LedgerRepository repo,
      TransactionTemplate tx,
      KafkaTemplate<String, LedgerEvent> kafka,
      LedgerProperties props,
      MeterRegistry meters) {
    this.repo = repo;
    this.tx = tx;
    this.kafka = kafka;
    this.props = props;
    Gauge.builder("ledger_publish_backlog", backlog, AtomicLong::get)
        .description("Ledger entries committed but not yet published to ledger.events")
        .register(meters);
  }

  @Scheduled(fixedDelayString = "${ledger.relay-interval-ms:20}")
  public void drain() {
    try {
      int published;
      do {
        published = tx.execute(status -> publishBatch());
      } while (published == props.relayBatchSize());
    } catch (Exception e) {
      log.warn("relay batch failed, will retry: {}", e.toString());
    }
  }

  @Scheduled(fixedDelay = 5_000)
  public void sampleBacklog() {
    backlog.set(repo.publishBacklog());
  }

  private int publishBatch() {
    if (!repo.tryRelayLock()) {
      return 0;
    }
    List<LedgerEntry> batch = repo.pendingPublication(props.relayBatchSize());
    if (batch.isEmpty()) {
      return 0;
    }
    var sends = new ArrayList<CompletableFuture<?>>(batch.size());
    for (LedgerEntry e : batch) {
      sends.add(kafka.send(Topics.LEDGER_EVENTS, e.memberId().toString(), toEvent(e)));
    }
    try {
      CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new)).get(30, TimeUnit.SECONDS);
    } catch (Exception ex) {
      throw new IllegalStateException("ledger.events send failed", ex); // rolls back; queue intact
    }
    repo.markPublished(batch.stream().map(LedgerEntry::globalSeq).toList());
    return batch.size();
  }

  static LedgerEvent toEvent(LedgerEntry e) {
    return LedgerEvent.newBuilder()
        .setEntryId(e.entryId().toString())
        .setMemberId(e.memberId().toString())
        .setSeq(e.seq())
        .setType(e.type())
        .setPoints(e.points())
        .setBalanceAfter(e.balanceAfter())
        .setReason(e.reasonJson())
        .setOccurredAtMs(e.occurredAt().toEpochMilli())
        .setSourceTimestampMs(e.sourceTimestamp() == null ? null : e.sourceTimestamp().toEpochMilli())
        .build();
  }
}
