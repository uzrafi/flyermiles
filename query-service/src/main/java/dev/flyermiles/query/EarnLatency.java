package dev.flyermiles.query;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.springframework.stereotype.Component;

/**
 * Time from a flight completing (stamped by the simulator) to its points being visible in the read
 * model. Recorded twice: a Prometheus histogram for dashboards and SLOs, and an exact HdrHistogram
 * that the benchmark scripts reset and read.
 */
@Component
public class EarnLatency {

  private final Timer timer;
  private final Recorder recorder = new Recorder(3);
  private Histogram accumulated = new Histogram(3);

  public EarnLatency(MeterRegistry meters) {
    this.timer = Timer.builder("flyermiles_earn_latency")
        .description("flight completed -> balance updated")
        .serviceLevelObjectives(
            Duration.ofMillis(100), Duration.ofMillis(250), Duration.ofMillis(500), Duration.ofSeconds(1),
            Duration.ofSeconds(2), Duration.ofSeconds(5), Duration.ofSeconds(10))
        .register(meters);
  }

  public void record(long millis) {
    if (millis < 0) millis = 0;
    timer.record(millis, TimeUnit.MILLISECONDS);
    recorder.recordValue(millis);
  }

  public synchronized void reset() {
    recorder.reset();
    accumulated = new Histogram(3);
  }

  public synchronized Map<String, Object> snapshot() {
    accumulated.add(recorder.getIntervalHistogram());
    Histogram h = accumulated;
    return Map.of(
        "count", h.getTotalCount(),
        "p50Ms", h.getValueAtPercentile(50),
        "p95Ms", h.getValueAtPercentile(95),
        "p99Ms", h.getValueAtPercentile(99),
        "maxMs", h.getMaxValue(),
        "meanMs", Math.round(h.getMean()));
  }
}
