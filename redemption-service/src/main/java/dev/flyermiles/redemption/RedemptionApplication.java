package dev.flyermiles.redemption;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import java.util.Map;
import java.util.UUID;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
public class RedemptionApplication {

  public static void main(String[] args) {
    SpringApplication.run(RedemptionApplication.class, args);
  }

  public record RedemptionBody(@NotNull UUID memberId, @Positive long points, @NotBlank String rewardId) {}

  @RestController
  @Validated
  static class RedemptionController {
    private final RedemptionService service;

    RedemptionController(RedemptionService service) {
      this.service = service;
    }

    /**
     * The response carries the ledger {@code seq}; pass it to the query-service as {@code ?minSeq=} to
     * read your own write.
     */
    @PostMapping("/redemptions")
    ResponseEntity<Map<String, Object>> redeem(
        @RequestHeader("Idempotency-Key") @NotBlank @Size(max = 200) String key,
        @Valid @RequestBody RedemptionBody body) {
      var res = service.redeem(key, new RedemptionService.Request(body.memberId(), body.points(), body.rewardId()));
      return ResponseEntity.status(res.status()).body(res.body());
    }
  }
}
