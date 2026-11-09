package dev.flyermiles.query;

import static org.assertj.core.api.Assertions.assertThat;

import dev.flyermiles.avro.EntryType;
import dev.flyermiles.avro.LedgerEvent;
import dev.flyermiles.avro.Topics;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@SpringBootTest(properties = {
    "spring.kafka.listener.auto-startup=false",
    "query.cache.enabled=false"
})
class ProjectionTest {

  @Container
  @ServiceConnection
  static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

  @Autowired LedgerProjector projector;
  @Autowired QueryController api;
  @Autowired JdbcTemplate jdbc;

  @Test
  void replayedEventsAreAbsorbed() {
    UUID member = UUID.randomUUID();
    var batch = events(member, 10);
    projector.project(batch);
    projector.project(batch); // at-least-once delivery: the same batch again
    projector.project(batch.subList(3, 6));

    var balance = api.balance(member, null).getBody();
    assertThat(balance.seq()).isEqualTo(10);
    assertThat(balance.balance()).isEqualTo(10 * 100);
    assertThat(jdbc.queryForObject("SELECT count(*) FROM statement_lines WHERE member_id = ?", Long.class, member))
        .isEqualTo(10);
  }

  @Test
  void anOlderEventNeverMovesTheBalanceBack() {
    UUID member = UUID.randomUUID();
    var all = events(member, 5);
    projector.project(all);
    projector.project(List.of(all.get(1)));
    assertThat(api.balance(member, null).getBody().seq()).isEqualTo(5);
  }

  @Test
  void keysetPagesMatchOffsetPages() {
    UUID member = UUID.randomUUID();
    projector.project(events(member, 237));

    List<Long> keyset = new ArrayList<>();
    String cursor = null;
    do {
      var page = api.statement(member, cursor, 50);
      page.lines().forEach(l -> keyset.add(l.seq()));
      cursor = page.nextCursor();
    } while (cursor != null);

    List<Long> offset = new ArrayList<>();
    for (int p = 0; p < 5; p++) {
      api.statementOffset(member, p, 50).lines().forEach(l -> offset.add(l.seq()));
    }

    assertThat(keyset).hasSize(237).isEqualTo(offset);
    assertThat(keyset.get(0)).isEqualTo(237);
  }

  @Test
  void readYourWritesAnswers202WhileTheProjectionIsBehind() {
    UUID member = UUID.randomUUID();
    projector.project(events(member, 3));
    var res = api.balance(member, 4L);
    assertThat(res.getStatusCode().value()).isEqualTo(202);
    assertThat(res.getBody().seq()).isEqualTo(3);
    assertThat(api.balance(member, 3L).getStatusCode().value()).isEqualTo(200);
  }

  private static List<ConsumerRecord<String, LedgerEvent>> events(UUID member, int n) {
    var out = new ArrayList<ConsumerRecord<String, LedgerEvent>>();
    long t0 = 1_760_000_000_000L;
    for (int i = 1; i <= n; i++) {
      var e = LedgerEvent.newBuilder()
          .setEntryId(UUID.randomUUID().toString()).setMemberId(member.toString()).setSeq(i)
          .setType(EntryType.EARNED).setPoints(100).setBalanceAfter(i * 100L).setReason("{}")
          // two entries share each millisecond, so the (occurred_at, seq) tie-break is exercised
          .setOccurredAtMs(t0 + i / 2).setSourceTimestampMs(null).build();
      out.add(new ConsumerRecord<>(Topics.LEDGER_EVENTS, 0, i, member.toString(), e));
    }
    return out;
  }
}
