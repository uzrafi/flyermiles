package dev.flyermiles.earning;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.TimeWindow;

/**
 * Tumbling windows aligned to calendar years (Jan 1 00:00 in {@code zone}). The built-in
 * TimeWindows are fixed-size from the epoch, so a "365-day" window would drift off the calendar and
 * leap years would break it. Qualifying miles reset every January 1.
 */
public final class CalendarYearWindows extends Windows<TimeWindow> {

  private static final long MAX_YEAR_MS = Duration.ofDays(366).toMillis();

  private final ZoneId zone;
  private final Duration grace;

  public CalendarYearWindows(ZoneId zone, Duration grace) {
    this.zone = Objects.requireNonNull(zone);
    this.grace = Objects.requireNonNull(grace);
  }

  @Override
  public Map<Long, TimeWindow> windowsFor(long timestamp) {
    ZonedDateTime yearStart = Instant.ofEpochMilli(timestamp).atZone(zone)
        .withDayOfYear(1).truncatedTo(ChronoUnit.DAYS);
    long start = yearStart.toInstant().toEpochMilli();
    long end = yearStart.plusYears(1).toInstant().toEpochMilli();
    return Map.of(start, new TimeWindow(start, end));
  }

  /** Upper bound, used by Streams for store retention and segment sizing. */
  @Override
  public long size() {
    return MAX_YEAR_MS;
  }

  @Override
  public long gracePeriodMs() {
    return grace.toMillis();
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof CalendarYearWindows w && w.zone.equals(zone) && w.grace.equals(grace);
  }

  @Override
  public int hashCode() {
    return Objects.hash(zone, grace);
  }
}
