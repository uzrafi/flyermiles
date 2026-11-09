package dev.flyermiles.redemption;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.flyermiles.redemption.LedgerClient.Outcome;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

/**
 * Read the member's version, decide, write with that version, and on a 409 start over. Retries use
 * "full jitter" exponential backoff so a crowd of losers doesn't retry in lockstep.
 *
 * <p>The ledger entryId is derived from the Idempotency-Key, so even if this service crashes after the
 * ledger write but before recording the outcome, the client's retry lands on the same entry.
 */
@Service
public class RedemptionService {

  public record Request(UUID memberId, long points, String rewardId) {}

  public record Response(int status, Map<String, Object> body) {}

  private final LedgerClient ledger;
  private final JdbcTemplate jdbc;
  private final ObjectMapper json;
  private final int maxAttempts;
  private final MeterRegistry meters;
  private final DistributionSummary attemptsSummary;

  public RedemptionService(
      LedgerClient ledger, JdbcTemplate jdbc, ObjectMapper json, MeterRegistry meters,
      @Value("${redemption.max-attempts:10}") int maxAttempts) {
    this.ledger = ledger;
    this.jdbc = jdbc;
    this.json = json;
    this.meters = meters;
    this.maxAttempts = maxAttempts;
    this.attemptsSummary = DistributionSummary.builder("redemption_attempts")
        .description("Ledger write attempts per redemption (1 = no contention)")
        .register(meters);
  }

  public Response redeem(String idempotencyKey, Request req) {
    String hash = hash(req);
    Optional<Response> previous = findPrevious(idempotencyKey, hash);
    if (previous.isPresent()) {
      return previous.get();
    }

    UUID entryId = UUID.nameUUIDFromBytes(("redemption:" + idempotencyKey).getBytes(StandardCharsets.UTF_8));
    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      var state = ledger.state(req.memberId());
      if (state.balance() < req.points()) {
        attemptsSummary.record(attempt);
        return record(idempotencyKey, hash, req, 422, Map.of(
            "error", "INSUFFICIENT_BALANCE", "balance", state.balance(), "seq", state.seq()));
      }
      Outcome outcome = ledger.redeem(req.memberId(), entryId, req.points(), state.seq(), req.rewardId());
      switch (outcome) {
        case Outcome.Written w -> {
          attemptsSummary.record(attempt);
          return record(idempotencyKey, hash, req, 201, Map.of(
              "redemptionId", w.entryId(), "memberId", req.memberId(), "points", req.points(),
              "rewardId", req.rewardId(), "seq", w.seq(), "balanceAfter", w.balanceAfter()));
        }
        case Outcome.Insufficient i -> {
          attemptsSummary.record(attempt);
          return record(idempotencyKey, hash, req, 422, Map.of(
              "error", "INSUFFICIENT_BALANCE", "balance", i.balance()));
        }
        case Outcome.VersionConflict c -> {
          meters.counter("redemption_version_conflicts_total").increment();
          sleepWithFullJitter(attempt);
        }
      }
    }
    meters.counter("redemption_gave_up_total").increment();
    return new Response(503, Map.of("error", "CONTENDED", "retryable", true));
  }

  private Optional<Response> findPrevious(String key, String hash) {
    return jdbc.query(
        "SELECT request_hash, http_status, response::text AS response FROM redemptions WHERE idempotency_key = ?",
        (rs, i) -> {
          if (!rs.getString("request_hash").equals(hash)) {
            return new Response(422, Map.of("error", "IDEMPOTENCY_KEY_REUSED",
                "message", "this Idempotency-Key was used with a different request"));
          }
          return new Response(rs.getInt("http_status"), readMap(rs.getString("response")));
        }, key).stream().findFirst();
  }

  private Response record(String key, String hash, Request req, int status, Map<String, Object> body) {
    jdbc.update(
        """
        INSERT INTO redemptions (idempotency_key, request_hash, member_id, points, reward_id, http_status, response)
        VALUES (?, ?, ?, ?, ?, ?, ?::jsonb) ON CONFLICT (idempotency_key) DO NOTHING
        """, key, hash, req.memberId(), req.points(), req.rewardId(), status, writeJson(body));
    meters.counter("redemptions_total", "status", String.valueOf(status)).increment();
    return new Response(status, body);
  }

  /** sleep = random(0, min(cap, base × 2^attempt)) */
  static void sleepWithFullJitter(int attempt) {
    long ceiling = Math.min(200, 5L << Math.min(attempt, 10));
    try {
      Thread.sleep(ThreadLocalRandom.current().nextLong(ceiling + 1));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static String hash(Request r) {
    try {
      byte[] d = MessageDigest.getInstance("SHA-256")
          .digest((r.memberId() + "|" + r.points() + "|" + r.rewardId()).getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(d);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> readMap(String s) {
    try {
      return json.readValue(s, Map.class);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private String writeJson(Map<String, Object> m) {
    try {
      return json.writeValueAsString(m);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
