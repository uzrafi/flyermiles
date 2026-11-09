package dev.flyermiles.recon;

import dev.flyermiles.avro.Topics;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * One pass of every check. Each check compares two independently derived numbers; any difference is
 * a mismatch.
 */
final class Reconciler {

  record Check(String name, long expected, long actual) {
    boolean ok() {
      return expected == actual;
    }
  }

  record Result(Map<String, Check> checks, Map<String, Long> totals) {
    long mismatches() {
      return checks.values().stream().filter(c -> !c.ok()).count();
    }
  }

  private final JdbcTemplate ledger;
  private final JdbcTemplate readModel;
  private final TopicCounter topics;

  Reconciler(JdbcTemplate ledger, JdbcTemplate readModel, TopicCounter topics) {
    this.ledger = ledger;
    this.readModel = readModel;
    this.topics = topics;
  }

  Result run() {
    var checks = new LinkedHashMap<String, Check>();
    var totals = new LinkedHashMap<String, Long>();

    // --- Kafka: did the earning engine lose or duplicate anything? ---
    long flights = topics.count(Topics.FLIGHT_COMPLETED);
    long pointsEarned = topics.count(Topics.POINTS_EARNED);
    totals.put("flight_completed_records", flights);
    totals.put("points_earned_records", pointsEarned);
    add(checks, "flights_vs_points_earned", flights, pointsEarned);

    // --- Ledger ---
    Map<String, Object> l = ledger.queryForMap(
        """
        SELECT count(*) AS entries,
               count(*) FILTER (WHERE type = 'EARNED')   AS earned_entries,
               COALESCE(sum(points) FILTER (WHERE type = 'EARNED'), 0)   AS earned,
               COALESCE(sum(points) FILTER (WHERE type = 'REDEEMED'), 0) AS redeemed,
               COALESCE(sum(points) FILTER (WHERE type = 'REVERSED'), 0) AS reversed,
               COALESCE(sum(points), 0) AS net,
               count(*) FILTER (WHERE type = 'REDEEMED' AND balance_after < 0) AS overdrafts
        FROM ledger_entries
        """);
    long earnedEntries = num(l, "earned_entries");
    long net = num(l, "net");
    totals.put("ledger_entries", num(l, "entries"));
    totals.put("ledger_points_earned", num(l, "earned"));
    totals.put("ledger_points_redeemed", -num(l, "redeemed"));
    totals.put("ledger_points_reversed", -num(l, "reversed"));
    add(checks, "points_earned_vs_ledger_earned_entries", pointsEarned, earnedEntries);
    add(checks, "ledger_overdrafts", 0, num(l, "overdrafts"));

    // Each member's history is gap-free (seq 1..n) and its last balance_after equals the sum.
    long brokenHistories = ledger.queryForObject(
        """
        SELECT count(*) FROM (
          SELECT member_id, count(*) AS n, max(seq) AS max_seq, sum(points) AS total,
                 (array_agg(balance_after ORDER BY seq DESC))[1] AS last_balance
          FROM ledger_entries GROUP BY member_id
        ) m WHERE n <> max_seq OR total <> last_balance
        """, Long.class);
    add(checks, "ledger_member_histories_broken", 0, brokenHistories);

    // --- Read model vs ledger ---
    Map<String, Object> r = readModel.queryForMap(
        "SELECT COALESCE(sum(balance), 0) AS total, count(*) AS members FROM member_balances");
    long lines = readModel.queryForObject("SELECT count(*) FROM statement_lines", Long.class);
    totals.put("read_model_balance_total", num(r, "total"));
    totals.put("read_model_statement_lines", lines);
    // Σ(EARNED) − Σ(REDEEMED) − Σ(REVERSED), all signed in the ledger, so it's just the net sum.
    add(checks, "ledger_net_vs_read_model_balances", net, num(r, "total"));
    add(checks, "ledger_entries_vs_statement_lines", num(l, "entries"), lines);

    Map<UUID, long[]> perMember = new HashMap<>();
    ledger.query("SELECT member_id, sum(points) AS total, max(seq) AS seq FROM ledger_entries GROUP BY member_id",
        rs -> {
          perMember.put(rs.getObject("member_id", UUID.class), new long[] {rs.getLong("total"), rs.getLong("seq")});
        });
    long[] wrong = {0};
    readModel.query("SELECT member_id, balance, last_seq FROM member_balances", rs -> {
      long[] expected = perMember.remove(rs.getObject("member_id", UUID.class));
      if (expected == null || expected[0] != rs.getLong("balance") || expected[1] != rs.getLong("last_seq")) {
        wrong[0]++;
      }
    });
    wrong[0] += perMember.size(); // in the ledger but missing from the read model
    totals.put("members", (long) num(r, "members"));
    add(checks, "members_with_wrong_balance", 0, wrong[0]);

    return new Result(checks, totals);
  }

  private static void add(Map<String, Check> checks, String name, long expected, long actual) {
    checks.put(name, new Check(name, expected, actual));
  }

  private static long num(Map<String, Object> row, String col) {
    return ((Number) row.get(col)).longValue();
  }
}
