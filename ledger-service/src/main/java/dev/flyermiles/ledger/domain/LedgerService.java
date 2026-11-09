package dev.flyermiles.ledger.domain;

import dev.flyermiles.avro.EntryType;
import dev.flyermiles.ledger.LedgerProperties;
import dev.flyermiles.ledger.domain.AppendResult.Appended;
import dev.flyermiles.ledger.domain.AppendResult.Conflict;
import dev.flyermiles.ledger.domain.AppendResult.Duplicate;
import dev.flyermiles.ledger.domain.AppendResult.InsufficientBalance;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Appends to the event store with optimistic concurrency:
 *
 * <ol>
 *   <li>read the member's current {@code seq} and balance;
 *   <li>decide (for a redemption: is the balance enough?);
 *   <li>{@code INSERT ... seq = current + 1}.
 * </ol>
 *
 * If two writers read the same version, the primary key {@code (member_id, seq)} lets exactly one of
 * them in. The loser gets a {@link Conflict} and must re-read and decide again, which is what keeps
 * concurrent redemptions from overdrawing an account. No row is ever locked for the decision.
 */
@Service
public class LedgerService {

  private final LedgerRepository repo;
  private final TransactionTemplate tx;
  private final LedgerProperties props;
  private final Counter conflicts;
  private final MeterRegistry meters;

  public LedgerService(
      LedgerRepository repo, TransactionTemplate tx, LedgerProperties props, MeterRegistry meters) {
    this.repo = repo;
    this.tx = tx;
    this.props = props;
    this.meters = meters;
    this.conflicts = Counter.builder("ledger_append_conflicts_total")
        .description("Appends that lost an optimistic-concurrency race")
        .register(meters);
  }

  public MemberState state(UUID memberId) {
    return repo.currentState(memberId);
  }

  /** A single attempt. Callers that pass an expectedSeq handle {@link Conflict} themselves. */
  public AppendResult append(AppendCommand cmd) {
    var existing = repo.findByEntryId(cmd.entryId());
    if (existing.isPresent()) {
      return new Duplicate(existing.get());
    }
    try {
      AppendResult result = tx.execute(status -> tryAppend(cmd));
      if (result instanceof Appended a) {
        meters.counter("ledger_entries_appended_total", "type", a.entry().type().name()).increment();
      }
      return result;
    } catch (DuplicateKeyException e) {
      // Either a concurrent retry of the same entryId (idempotent) or another entry took our seq.
      var raced = repo.findByEntryId(cmd.entryId());
      if (raced.isPresent()) {
        return new Duplicate(raced.get());
      }
      conflicts.increment();
      return new Conflict(repo.currentState(cmd.memberId()).seq());
    }
  }

  /**
   * For appends that don't depend on a version the caller read earlier (earned points, reversals):
   * on a conflict, simply re-read and try again with jittered backoff.
   */
  public AppendResult appendWithRetry(AppendCommand cmd) {
    for (int attempt = 1; ; attempt++) {
      AppendResult result = append(cmd);
      if (!(result instanceof Conflict) || attempt >= props.maxAppendAttempts()) {
        return result;
      }
      backoff(attempt);
    }
  }

  /** The compensating entry for an earn: never edit or delete, append the opposite instead. */
  public AppendResult reverse(UUID memberId, UUID earnEntryId, String reasonJson) {
    var earn = repo.findByEntryId(earnEntryId)
        .filter(e -> e.memberId().equals(memberId) && e.type() == EntryType.EARNED)
        .orElseThrow(() -> new IllegalArgumentException("no EARNED entry " + earnEntryId + " for member"));
    var reversalId = UUID.nameUUIDFromBytes(("reversal:" + earnEntryId).getBytes(StandardCharsets.UTF_8));
    return appendWithRetry(new AppendCommand(
        memberId, reversalId, EntryType.REVERSED, -earn.points(), reasonJson,
        java.time.Instant.now(), null, null));
  }

  private AppendResult tryAppend(AppendCommand cmd) {
    MemberState state = repo.currentState(cmd.memberId());
    if (cmd.expectedSeq() != null && cmd.expectedSeq() != state.seq()) {
      conflicts.increment();
      return new Conflict(state.seq());
    }
    long balanceAfter = state.balance() + cmd.points();
    if (cmd.type() == EntryType.REDEEMED && balanceAfter < 0) {
      return new InsufficientBalance(state.balance(), state.seq());
    }
    long seq = state.seq() + 1;
    long globalSeq = repo.insert(cmd, seq, balanceAfter);
    if (seq % props.snapshotEvery() == 0) {
      repo.insertSnapshot(cmd.memberId(), seq, balanceAfter);
    }
    return new Appended(new LedgerEntry(
        globalSeq, cmd.memberId(), seq, cmd.entryId(), cmd.type(), cmd.points(), balanceAfter,
        cmd.reasonJson(), cmd.occurredAt(), cmd.sourceTimestamp()));
  }

  private static void backoff(int attempt) {
    long capMs = Math.min(50, 1L << Math.min(attempt, 6));
    try {
      Thread.sleep(ThreadLocalRandom.current().nextLong(1, capMs + 1));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
