package dev.flyermiles.ledger.kafka;

import dev.flyermiles.avro.EntryType;
import dev.flyermiles.avro.PointsEarned;
import dev.flyermiles.avro.Topics;
import dev.flyermiles.ledger.domain.AppendCommand;
import dev.flyermiles.ledger.domain.AppendResult;
import dev.flyermiles.ledger.domain.LedgerService;
import java.time.Instant;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Turns {@code points.earned} into EARNED entries. Reads with {@code isolation.level=read_committed}
 * so only output of committed Kafka Streams transactions is seen. The entryId is the flight's
 * eventId, so a redelivered message is a no-op.
 */
@Component
public class PointsEarnedListener {

  private static final Logger log = LoggerFactory.getLogger(PointsEarnedListener.class);

  private final LedgerService ledger;

  public PointsEarnedListener(LedgerService ledger) {
    this.ledger = ledger;
  }

  @KafkaListener(topics = Topics.POINTS_EARNED, groupId = "ledger-service")
  public void onPointsEarned(PointsEarned event) {
    var reason = """
        {"flightNumber":"%s","fareClass":"%s","distanceMiles":%d,"tier":"%s","flightEventId":"%s"}"""
        .formatted(event.getFlightNumber(), event.getFareClass(), event.getDistanceMiles(),
            event.getTier(), event.getEventId());
    var cmd = new AppendCommand(
        UUID.fromString(event.getMemberId()),
        UUID.fromString(event.getEventId()),
        EntryType.EARNED,
        event.getPoints(),
        reason,
        Instant.now(),
        Instant.ofEpochMilli(event.getFlightCompletedAtMs()),
        null);

    AppendResult result = ledger.appendWithRetry(cmd);
    if (result instanceof AppendResult.Conflict c) {
      // Throwing makes the container's error handler redeliver; nothing is lost.
      throw new IllegalStateException("gave up on contended member " + event.getMemberId() + " at seq " + c.currentSeq());
    }
    if (result instanceof AppendResult.Duplicate) {
      log.debug("duplicate points.earned {} ignored", event.getEventId());
    }
  }
}
