package dev.flyermiles.query;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class QueryController {

  public record Balance(UUID memberId, long balance, long seq, String source) {}

  public record Line(long seq, Instant occurredAt, String type, long points, long balanceAfter) {}

  public record Statement(List<Line> lines, String nextCursor) {}

  private static final RowMapper<Line> LINE = (rs, i) -> new Line(
      rs.getLong("seq"), rs.getTimestamp("occurred_at").toInstant(), rs.getString("type"),
      rs.getLong("points"), rs.getLong("balance_after"));

  private static final long MIN_SEQ_WAIT_MS = 1_000;

  private final JdbcTemplate jdbc;
  private final BalanceCache cache;
  private final EarnLatency latency;
  private final RebuildTracker rebuild;

  public QueryController(JdbcTemplate jdbc, BalanceCache cache, EarnLatency latency, RebuildTracker rebuild) {
    this.jdbc = jdbc;
    this.cache = cache;
    this.latency = latency;
    this.rebuild = rebuild;
  }

  /**
   * Cache-aside read. With {@code minSeq} (the seq a redemption returned) the caller reads its own
   * write: we wait briefly for the projection to reach that version, and answer 202 with the current
   * value if it hasn't yet.
   */
  @GetMapping("/members/{id}/balance")
  public ResponseEntity<Balance> balance(@PathVariable UUID id, @RequestParam(required = false) Long minSeq) {
    var cached = cache.get(id);
    if (cached.isPresent() && (minSeq == null || cached.get().seq() >= minSeq)) {
      return ResponseEntity.ok(new Balance(id, cached.get().balance(), cached.get().seq(), "cache"));
    }

    Optional<BalanceCache.Cached> row = loadBalance(id);
    if (minSeq != null) {
      long deadline = System.currentTimeMillis() + MIN_SEQ_WAIT_MS;
      while (row.map(r -> r.seq() < minSeq).orElse(true) && System.currentTimeMillis() < deadline) {
        sleep(25);
        row = loadBalance(id);
      }
      if (row.map(r -> r.seq() < minSeq).orElse(true)) {
        var current = row.orElse(new BalanceCache.Cached(0, 0));
        return ResponseEntity.accepted().body(new Balance(id, current.balance(), current.seq(), "projection-behind"));
      }
    }
    if (row.isEmpty()) {
      return ResponseEntity.notFound().build();
    }
    cache.put(id, row.get());
    return ResponseEntity.ok(new Balance(id, row.get().balance(), row.get().seq(), "db"));
  }

  /** Keyset pagination: cost is independent of how deep the page is. */
  @GetMapping("/members/{id}/statement")
  public Statement statement(
      @PathVariable UUID id,
      @RequestParam(required = false) String cursor,
      @RequestParam(defaultValue = "50") int limit) {
    int n = Math.min(Math.max(limit, 1), 200);
    List<Line> lines;
    if (cursor == null) {
      lines = jdbc.query(
          """
          SELECT seq, occurred_at, type, points, balance_after FROM statement_lines
          WHERE member_id = ?
          ORDER BY occurred_at DESC, seq DESC LIMIT ?
          """, LINE, id, n);
    } else {
      long[] c = decodeCursor(cursor);
      lines = jdbc.query(
          """
          SELECT seq, occurred_at, type, points, balance_after FROM statement_lines
          WHERE member_id = ? AND (occurred_at, seq) < (?, ?)
          ORDER BY occurred_at DESC, seq DESC LIMIT ?
          """, LINE, id, Timestamp.from(Instant.ofEpochMilli(c[0])), c[1], n);
    }
    String next = lines.size() < n ? null : encodeCursor(lines.get(lines.size() - 1));
    return new Statement(lines, next);
  }

  /** The baseline the benchmark compares against: OFFSET has to walk and discard every earlier row. */
  @GetMapping("/members/{id}/statement-offset")
  public Statement statementOffset(
      @PathVariable UUID id,
      @RequestParam(defaultValue = "0") int page,
      @RequestParam(defaultValue = "50") int limit) {
    int n = Math.min(Math.max(limit, 1), 200);
    var lines = jdbc.query(
        """
        SELECT seq, occurred_at, type, points, balance_after FROM statement_lines
        WHERE member_id = ?
        ORDER BY occurred_at DESC, seq DESC OFFSET ? LIMIT ?
        """, LINE, id, (long) page * n, n);
    return new Statement(lines, null);
  }

  // --- admin: benchmarks and operations ---

  @PostMapping("/admin/cache")
  public Map<String, Object> setCache(@RequestParam boolean enabled) {
    cache.setEnabled(enabled);
    if (!enabled) cache.clear();
    return Map.of("cacheEnabled", cache.enabled());
  }

  @GetMapping("/admin/earn-latency")
  public Map<String, Object> earnLatency() {
    return latency.snapshot();
  }

  @PostMapping("/admin/earn-latency/reset")
  public Map<String, Object> resetEarnLatency() {
    latency.reset();
    return latency.snapshot();
  }

  @PostMapping("/admin/rebuild")
  public Map<String, Object> rebuild() throws Exception {
    return rebuild.start();
  }

  @GetMapping("/admin/rebuild")
  public Map<String, Object> rebuildStatus() {
    return rebuild.status();
  }

  @GetMapping("/admin/totals")
  public Map<String, Object> totals() {
    var out = new LinkedHashMap<String, Object>();
    out.putAll(jdbc.queryForMap("SELECT count(*) AS members, COALESCE(sum(balance),0) AS balance_total FROM member_balances"));
    out.putAll(jdbc.queryForMap("SELECT count(*) AS statement_lines FROM statement_lines"));
    return out;
  }

  private Optional<BalanceCache.Cached> loadBalance(UUID id) {
    return jdbc.query("SELECT balance, last_seq FROM member_balances WHERE member_id = ?",
        (rs, i) -> new BalanceCache.Cached(rs.getLong("balance"), rs.getLong("last_seq")), id).stream().findFirst();
  }

  static String encodeCursor(Line last) {
    String raw = last.occurredAt().toEpochMilli() + ":" + last.seq();
    return Base64.getUrlEncoder().withoutPadding().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
  }

  static long[] decodeCursor(String cursor) {
    String raw = new String(Base64.getUrlDecoder().decode(cursor), StandardCharsets.UTF_8);
    int sep = raw.indexOf(':');
    return new long[] {Long.parseLong(raw.substring(0, sep)), Long.parseLong(raw.substring(sep + 1))};
  }

  private static void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
