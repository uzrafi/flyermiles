package dev.flyermiles.simulator;

import dev.flyermiles.avro.FlightCompleted;
import java.util.List;
import java.util.UUID;
import java.util.random.RandomGenerator;

/** Plausible flights: real city pairs with great-circle distances, and a skewed fare-class mix. */
public final class FlightGenerator {

  record Route(String origin, String destination, int miles) {}

  static final List<Route> ROUTES = List.of(
      new Route("YYZ", "YUL", 314), new Route("YHZ", "YYZ", 788), new Route("YYC", "YYZ", 1690),
      new Route("YYZ", "YVR", 2080), new Route("YYZ", "LAX", 2172), new Route("YUL", "YVR", 2293),
      new Route("YUL", "CDG", 3446), new Route("YYZ", "LHR", 3544), new Route("YYZ", "FRA", 3965),
      new Route("YVR", "NRT", 4664), new Route("YYZ", "HND", 6420), new Route("YVR", "SYD", 7765));

  /** Y 70%, B 15%, J 12%, F 3%. */
  static String fareClass(RandomGenerator rnd) {
    int r = rnd.nextInt(100);
    if (r < 70) return "Y";
    if (r < 85) return "B";
    if (r < 97) return "J";
    return "F";
  }

  static String cabin(String fareClass) {
    return switch (fareClass) {
      case "F" -> "FIRST";
      case "J" -> "BUSINESS";
      case "B" -> "PREMIUM_ECONOMY";
      default -> "ECONOMY";
    };
  }

  private FlightGenerator() {}

  public static FlightCompleted next(String memberId, RandomGenerator rnd, boolean includeCabin, long nowMs) {
    Route route = ROUTES.get(rnd.nextInt(ROUTES.size()));
    boolean reverse = rnd.nextBoolean();
    String fare = fareClass(rnd);
    return FlightCompleted.newBuilder()
        .setEventId(UUID.randomUUID().toString())
        .setMemberId(memberId)
        .setFlightNumber("FM" + (100 + rnd.nextInt(900)))
        .setOrigin(reverse ? route.destination() : route.origin())
        .setDestination(reverse ? route.origin() : route.destination())
        .setDistanceMiles(route.miles())
        .setFareClass(fare)
        .setCompletedAtMs(nowMs)
        .setCabin(includeCabin ? cabin(fare) : null)
        .build();
  }
}
