package dev.flyermiles.ledger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.flyermiles.avro.EntryType;
import dev.flyermiles.avro.LedgerEvent;
import dev.flyermiles.ledger.domain.AppendCommand;
import dev.flyermiles.ledger.domain.AppendResult;
import dev.flyermiles.ledger.domain.LedgerService;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The proof that the ledger can't be overdrawn: N redemptions race for one member whose balance only
 * covers half of them. Each client follows the redemption-service protocol (read version, decide,
 * write with expectedSeq, on 409 re-read and retry).
 */
@Testcontainers
@SpringBootTest(properties = {
    "spring.kafka.listener.auto-startup=false",
    "spring.kafka.admin.auto-create=false",
    "ledger.relay-interval-ms=3600000",
    "spring.datasource.hikari.maximum-pool-size=30"
})
class ConcurrentRedemptionTest {

  static final int REDEMPTIONS = Integer.getInteger("redemptions", 200);
  static final long POINTS_EACH = 100;

  @Container
  @ServiceConnection
  static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

  @MockBean KafkaTemplate<String, LedgerEvent> kafka;

  @Autowired LedgerService ledger;
  @Autowired JdbcTemplate jdbc;

  @Test
  void concurrentRedemptionsNeverOverdraw() throws Exception {
    UUID member = UUID.randomUUID();
    long startingBalance = (REDEMPTIONS / 2) * POINTS_EACH;
    // Fund the account with many small earns, which also crosses snapshot boundaries.
    for (int i = 0; i < REDEMPTIONS / 2; i++) {
      ledger.appendWithRetry(earn(member, POINTS_EACH));
    }
    assertThat(ledger.state(member).balance()).isEqualTo(startingBalance);

    var accepted = new AtomicInteger();
    var insufficient = new AtomicInteger();
    var conflictsSeen = new AtomicInteger();
    var gate = new CountDownLatch(1);
    List<Future<?>> futures = new ArrayList<>();

    long t0;
    try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
      for (int i = 0; i < REDEMPTIONS; i++) {
        futures.add(pool.submit(() -> {
          gate.await();
          UUID entryId = UUID.randomUUID();
          while (true) {
            long expected = ledger.state(member).seq();
            var result = ledger.append(new AppendCommand(
                member, entryId, EntryType.REDEEMED, -POINTS_EACH, "{\"rewardId\":\"lounge-pass\"}",
                Instant.now(), null, expected));
            switch (result) {
              case AppendResult.Appended a -> { accepted.incrementAndGet(); return null; }
              case AppendResult.Duplicate d -> { accepted.incrementAndGet(); return null; }
              case AppendResult.InsufficientBalance x -> { insufficient.incrementAndGet(); return null; }
              case AppendResult.Conflict c -> {
                conflictsSeen.incrementAndGet();
                Thread.sleep(ThreadLocalRandom.current().nextLong(1, 10));
              }
            }
          }
        }));
      }
      t0 = System.nanoTime();
      gate.countDown();
      for (Future<?> f : futures) f.get();
    }
    long elapsedMs = (System.nanoTime() - t0) / 1_000_000;

    Long minBalance = jdbc.queryForObject(
        "SELECT min(balance_after) FROM ledger_entries WHERE member_id = ?", Long.class, member);
    Long redeemedTotal = jdbc.queryForObject(
        "SELECT -sum(points) FROM ledger_entries WHERE member_id = ? AND type = 'REDEEMED'", Long.class, member);

    System.out.printf(
        "RESULT concurrent_redemptions=%d accepted=%d rejected_insufficient=%d conflicts_retried=%d"
            + " overdrafts=%d elapsed_ms=%d%n",
        REDEMPTIONS, accepted.get(), insufficient.get(), conflictsSeen.get(),
        minBalance < 0 ? 1 : 0, elapsedMs);

    assertThat(minBalance).isGreaterThanOrEqualTo(0);
    assertThat(redeemedTotal).isEqualTo(startingBalance);
    assertThat(accepted.get()).isEqualTo(REDEMPTIONS / 2);
    assertThat(insufficient.get()).isEqualTo(REDEMPTIONS - REDEMPTIONS / 2);
    assertThat(ledger.state(member).balance()).isZero();
  }

  @Test
  void earnIsIdempotentOnEntryId() {
    UUID member = UUID.randomUUID();
    var cmd = earn(member, 500);
    assertThat(ledger.append(cmd)).isInstanceOf(AppendResult.Appended.class);
    assertThat(ledger.append(cmd)).isInstanceOf(AppendResult.Duplicate.class);
    assertThat(ledger.state(member)).extracting("seq", "balance").containsExactly(1L, 500L);
  }

  @Test
  void reversalAppendsACompensatingEntry() {
    UUID member = UUID.randomUUID();
    var earn = earn(member, 1200);
    ledger.append(earn);
    ledger.reverse(member, earn.entryId(), "{\"why\":\"flight cancelled\"}");
    ledger.reverse(member, earn.entryId(), "{\"why\":\"retry\"}"); // idempotent

    assertThat(ledger.state(member)).extracting("seq", "balance").containsExactly(2L, 0L);
  }

  @Test
  void ledgerRejectsUpdatesAndDeletes() {
    UUID member = UUID.randomUUID();
    ledger.append(earn(member, 10));
    assertThatThrownBy(() -> jdbc.update("UPDATE ledger_entries SET points = 999 WHERE member_id = ?", member))
        .hasMessageContaining("append-only");
    assertThatThrownBy(() -> jdbc.update("DELETE FROM ledger_entries WHERE member_id = ?", member))
        .hasMessageContaining("append-only");
  }

  private static AppendCommand earn(UUID member, long points) {
    return new AppendCommand(member, UUID.randomUUID(), EntryType.EARNED, points,
        "{\"flightNumber\":\"FM100\"}", Instant.now(), Instant.now(), null);
  }
}
