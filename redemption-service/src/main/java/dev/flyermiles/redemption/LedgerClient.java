package dev.flyermiles.redemption;

import java.util.Map;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

@Component
public class LedgerClient {

  public record State(long seq, long balance) {}

  public sealed interface Outcome {
    record Written(UUID entryId, long seq, long balanceAfter) implements Outcome {}
    record VersionConflict(long currentSeq) implements Outcome {}
    record Insufficient(long balance) implements Outcome {}
  }

  record EntryBody(UUID entryId, long seq, long balanceAfter) {}

  record ErrorBody(String error, long currentSeq, Long balance) {}

  private final RestClient http;

  public LedgerClient(RestClient.Builder builder, @Value("${redemption.ledger-url}") String ledgerUrl) {
    this.http = builder.baseUrl(ledgerUrl).build();
  }

  public State state(UUID memberId) {
    return http.get().uri("/ledger/members/{id}", memberId).retrieve().body(State.class);
  }

  public Outcome redeem(UUID memberId, UUID entryId, long points, long expectedSeq, String rewardId) {
    return http.post()
        .uri("/ledger/members/{id}/redemptions", memberId)
        .contentType(MediaType.APPLICATION_JSON)
        .body(Map.of("entryId", entryId, "points", points, "expectedSeq", expectedSeq,
            "reason", Map.of("rewardId", rewardId)))
        .exchange((req, res) -> switch (res.getStatusCode().value()) {
          case 200, 201 -> {
            EntryBody b = res.bodyTo(EntryBody.class);
            yield new Outcome.Written(b.entryId(), b.seq(), b.balanceAfter());
          }
          case 409 -> new Outcome.VersionConflict(res.bodyTo(ErrorBody.class).currentSeq());
          case 422 -> new Outcome.Insufficient(res.bodyTo(ErrorBody.class).balance());
          default -> throw new IllegalStateException("ledger answered " + res.getStatusCode());
        });
  }
}
