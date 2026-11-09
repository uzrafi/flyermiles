package dev.flyermiles.avro;

/** Kafka topic names shared by every service. */
public final class Topics {
  public static final String FLIGHT_COMPLETED = "flight.completed";
  /** Log-compacted, keyed by memberId; co-partitioned with {@link #FLIGHT_COMPLETED}. */
  public static final String MEMBER_PROFILES = "member.profiles";
  public static final String TIER_CHANGED = "tier.changed";
  public static final String POINTS_EARNED = "points.earned";
  public static final String LEDGER_EVENTS = "ledger.events";

  private Topics() {}
}
