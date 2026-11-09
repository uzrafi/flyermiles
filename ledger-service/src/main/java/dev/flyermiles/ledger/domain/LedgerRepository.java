package dev.flyermiles.ledger.domain;

import dev.flyermiles.avro.EntryType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class LedgerRepository {

  private static final String COLUMNS =
      "e.global_seq, e.member_id, e.seq, e.entry_id, e.type, e.points, e.balance_after,"
          + " e.reason::text AS reason, e.occurred_at, e.source_ts";

  private static final RowMapper<LedgerEntry> ENTRY = LedgerRepository::mapEntry;

  private final JdbcTemplate jdbc;

  public LedgerRepository(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  public Optional<LedgerEntry> findByEntryId(UUID entryId) {
    return jdbc.query("SELECT " + COLUMNS + " FROM ledger_entries e WHERE e.entry_id = ?", ENTRY, entryId)
        .stream().findFirst();
  }

  /** Latest snapshot plus the entries after it, so the cost is bounded by the snapshot interval. */
  public MemberState currentState(UUID memberId) {
    return jdbc.queryForObject(
        """
        WITH snap AS (
          SELECT seq, balance FROM ledger_snapshots
          WHERE member_id = ? ORDER BY seq DESC LIMIT 1
        )
        SELECT COALESCE(MAX(e.seq), (SELECT seq FROM snap), 0) AS seq,
               COALESCE((SELECT balance FROM snap), 0) + COALESCE(SUM(e.points), 0) AS balance
        FROM ledger_entries e
        WHERE e.member_id = ? AND e.seq > COALESCE((SELECT seq FROM snap), 0)
        """,
        (rs, i) -> new MemberState(rs.getLong("seq"), rs.getLong("balance")),
        memberId, memberId);
  }

  /** Throws DuplicateKeyException if (member_id, seq) or entry_id already exists. */
  public long insert(AppendCommand cmd, long seq, long balanceAfter) {
    return jdbc.queryForObject(
        """
        INSERT INTO ledger_entries
          (member_id, seq, entry_id, type, points, balance_after, reason, occurred_at, source_ts)
        VALUES (?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?)
        RETURNING global_seq
        """,
        Long.class,
        cmd.memberId(), seq, cmd.entryId(), cmd.type().name(), cmd.points(), balanceAfter,
        cmd.reasonJson(), Timestamp.from(cmd.occurredAt()),
        cmd.sourceTimestamp() == null ? null : Timestamp.from(cmd.sourceTimestamp()));
  }

  public void insertSnapshot(UUID memberId, long seq, long balance) {
    jdbc.update(
        "INSERT INTO ledger_snapshots (member_id, seq, balance) VALUES (?, ?, ?) ON CONFLICT DO NOTHING",
        memberId, seq, balance);
  }

  public List<LedgerEntry> entries(UUID memberId, long afterSeq, int limit) {
    return jdbc.query(
        "SELECT " + COLUMNS + " FROM ledger_entries e WHERE e.member_id = ? AND e.seq > ?"
            + " ORDER BY e.seq LIMIT ?",
        ENTRY, memberId, afterSeq, limit);
  }

  // --- publication queue (drained by LedgerEventRelay) ---

  /** Only one relay may drain the queue at a time, across all replicas. Released at commit. */
  public boolean tryRelayLock() {
    return Boolean.TRUE.equals(
        jdbc.queryForObject("SELECT pg_try_advisory_xact_lock(4242)", Boolean.class));
  }

  public List<LedgerEntry> pendingPublication(int limit) {
    return jdbc.query(
        "SELECT " + COLUMNS
            + " FROM ledger_publish_queue q JOIN ledger_entries e ON e.global_seq = q.global_seq"
            + " ORDER BY q.global_seq LIMIT ?",
        ENTRY, limit);
  }

  public void markPublished(List<Long> globalSeqs) {
    jdbc.update(
        "DELETE FROM ledger_publish_queue WHERE global_seq = ANY (?)",
        ps -> ps.setArray(1, ps.getConnection().createArrayOf("bigint", globalSeqs.toArray())));
  }

  public long publishBacklog() {
    return jdbc.queryForObject("SELECT count(*) FROM ledger_publish_queue", Long.class);
  }

  private static LedgerEntry mapEntry(ResultSet rs, int i) throws SQLException {
    Timestamp source = rs.getTimestamp("source_ts");
    return new LedgerEntry(
        rs.getLong("global_seq"),
        rs.getObject("member_id", UUID.class),
        rs.getLong("seq"),
        rs.getObject("entry_id", UUID.class),
        EntryType.valueOf(rs.getString("type")),
        rs.getLong("points"),
        rs.getLong("balance_after"),
        rs.getString("reason"),
        rs.getTimestamp("occurred_at").toInstant(),
        source == null ? null : source.toInstant());
  }
}
