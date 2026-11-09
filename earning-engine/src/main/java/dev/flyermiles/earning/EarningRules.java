package dev.flyermiles.earning;

import dev.flyermiles.avro.Tier;
import java.util.Map;

/** points = distance × fare-class multiplier × (1 + tier bonus). Integer maths, rounded down. */
public final class EarningRules {

  /** Percent: Y = 1.0, B = 1.25, J = 1.5, F = 2.0. Unknown classes earn at 1.0. */
  static final Map<String, Integer> FARE_MULTIPLIER_PCT = Map.of("Y", 100, "B", 125, "J", 150, "F", 200);

  static final Map<Tier, Integer> TIER_BONUS_PCT =
      Map.of(Tier.BASE, 0, Tier.SILVER, 25, Tier.GOLD, 50, Tier.PLATINUM, 100);

  public static final long SILVER_MILES = 25_000;
  public static final long GOLD_MILES = 50_000;
  public static final long PLATINUM_MILES = 75_000;

  private EarningRules() {}

  public static long points(int distanceMiles, String fareClass, Tier tier) {
    long fare = FARE_MULTIPLIER_PCT.getOrDefault(fareClass, 100);
    long bonus = TIER_BONUS_PCT.get(tier);
    return Math.max(1, distanceMiles * fare * (100 + bonus) / 10_000);
  }

  public static Tier tierFor(long qualifyingMiles) {
    if (qualifyingMiles >= PLATINUM_MILES) return Tier.PLATINUM;
    if (qualifyingMiles >= GOLD_MILES) return Tier.GOLD;
    if (qualifyingMiles >= SILVER_MILES) return Tier.SILVER;
    return Tier.BASE;
  }
}
