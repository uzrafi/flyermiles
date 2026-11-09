package dev.flyermiles.member;

import dev.flyermiles.avro.Tier;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class MemberRepository {

  public record Member(UUID memberId, String name, String email, Tier tier, Instant updatedAt) {}

  private static final RowMapper<Member> MEMBER = (rs, i) -> new Member(
      rs.getObject("member_id", UUID.class), rs.getString("name"), rs.getString("email"),
      Tier.valueOf(rs.getString("tier")), rs.getTimestamp("updated_at").toInstant());

  private final JdbcTemplate jdbc;

  public MemberRepository(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  public Member insert(UUID id, String name, String email) {
    return jdbc.queryForObject(
        "INSERT INTO members (member_id, name, email) VALUES (?, ?, ?) RETURNING *", MEMBER, id, name, email);
  }

  public Optional<Member> find(UUID id) {
    return jdbc.query("SELECT * FROM members WHERE member_id = ?", MEMBER, id).stream().findFirst();
  }

  public List<UUID> ids(int limit) {
    return jdbc.queryForList("SELECT member_id FROM members ORDER BY created_at LIMIT ?", UUID.class, limit);
  }

  public List<Member> page(UUID after, int limit) {
    return jdbc.query("SELECT * FROM members WHERE member_id > ? ORDER BY member_id LIMIT ?", MEMBER,
        after == null ? new UUID(0, 0) : after, limit);
  }

  /** Only ever upgrades: a late or replayed tier.changed can't demote anyone. */
  public Optional<Member> upgradeTier(UUID id, Tier newTier) {
    return jdbc.query(
        """
        UPDATE members SET tier = ?, updated_at = now()
        WHERE member_id = ?
          AND array_position(ARRAY['BASE','SILVER','GOLD','PLATINUM'], tier)
            < array_position(ARRAY['BASE','SILVER','GOLD','PLATINUM'], ?)
        RETURNING *
        """, MEMBER, newTier.name(), id, newTier.name()).stream().findFirst();
  }
}
