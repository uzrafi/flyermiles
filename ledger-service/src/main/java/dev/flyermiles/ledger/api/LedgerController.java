package dev.flyermiles.ledger.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.flyermiles.avro.EntryType;
import dev.flyermiles.ledger.domain.AppendCommand;
import dev.flyermiles.ledger.domain.AppendResult;
import dev.flyermiles.ledger.domain.LedgerEntry;
import dev.flyermiles.ledger.domain.LedgerService;
import dev.flyermiles.ledger.domain.LedgerRepository;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Positive;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import jakarta.validation.constraints.NotNull;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/ledger/members/{memberId}")
public class LedgerController {

  public record StateResponse(UUID memberId, long seq, long balance) {}

  /**
   * @param points the amount to spend (positive)
   * @param expectedSeq the version the caller read; the write is rejected with 409 if it moved
   */
  public record RedemptionRequest(
      @NotNull UUID entryId, @Positive long points, @NotNull Long expectedSeq, Map<String, Object> reason) {}

  public record ReversalRequest(@NotNull UUID earnEntryId, Map<String, Object> reason) {}

  public record EntryResponse(UUID entryId, long seq, EntryType type, long points, long balanceAfter, Instant occurredAt) {
    static EntryResponse of(LedgerEntry e) {
      return new EntryResponse(e.entryId(), e.seq(), e.type(), e.points(), e.balanceAfter(), e.occurredAt());
    }
  }

  public record ErrorResponse(String error, long currentSeq, Long balance) {}

  private final LedgerService ledger;
  private final LedgerRepository repo;
  private final ObjectMapper json;

  public LedgerController(LedgerService ledger, LedgerRepository repo, ObjectMapper json) {
    this.ledger = ledger;
    this.repo = repo;
    this.json = json;
  }

  @GetMapping
  public StateResponse state(@PathVariable UUID memberId) {
    var s = ledger.state(memberId);
    return new StateResponse(memberId, s.seq(), s.balance());
  }

  @GetMapping("/entries")
  public List<EntryResponse> entries(
      @PathVariable UUID memberId,
      @RequestParam(defaultValue = "0") long afterSeq,
      @RequestParam(defaultValue = "100") int limit) {
    return repo.entries(memberId, afterSeq, Math.min(limit, 1000)).stream().map(EntryResponse::of).toList();
  }

  /** Called by redemption-service. One attempt: the caller owns the retry, because it owns the decision. */
  @PostMapping("/redemptions")
  public ResponseEntity<?> redeem(@PathVariable UUID memberId, @Valid @RequestBody RedemptionRequest req) {
    var cmd = new AppendCommand(
        memberId, req.entryId(), EntryType.REDEEMED, -req.points(), toJson(req.reason()),
        Instant.now(), null, req.expectedSeq());
    return toResponse(ledger.append(cmd));
  }

  @PostMapping("/reversals")
  public ResponseEntity<?> reverse(@PathVariable UUID memberId, @Valid @RequestBody ReversalRequest req) {
    return toResponse(ledger.reverse(memberId, req.earnEntryId(), toJson(req.reason())));
  }

  @ExceptionHandler(IllegalArgumentException.class)
  ResponseEntity<Map<String, String>> badRequest(IllegalArgumentException e) {
    return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
  }

  private static ResponseEntity<?> toResponse(AppendResult result) {
    return switch (result) {
      case AppendResult.Appended a -> ResponseEntity.status(HttpStatus.CREATED).body(EntryResponse.of(a.entry()));
      case AppendResult.Duplicate d -> ResponseEntity.ok(EntryResponse.of(d.entry()));
      case AppendResult.Conflict c ->
          ResponseEntity.status(HttpStatus.CONFLICT).body(new ErrorResponse("VERSION_CONFLICT", c.currentSeq(), null));
      case AppendResult.InsufficientBalance i ->
          ResponseEntity.unprocessableEntity().body(new ErrorResponse("INSUFFICIENT_BALANCE", i.currentSeq(), i.balance()));
    };
  }

  private String toJson(Map<String, Object> reason) {
    try {
      return json.writeValueAsString(reason == null ? Map.of() : reason);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("reason is not serialisable", e);
    }
  }
}
