package dev.flyermiles.simulator;

import dev.flyermiles.avro.FlightCompleted;
import dev.flyermiles.avro.Topics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

@Service
public class Simulator {

  private static final Logger log = LoggerFactory.getLogger(Simulator.class);
  private static final long TICK_MS = 10;

  private final KafkaTemplate<String, FlightCompleted> kafka;
  private final RestClient members;
  private final boolean includeCabin;
  private final Counter sentCounter;
  private final AtomicLong sent = new AtomicLong();
  private final ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor();

  private volatile List<String> memberIds = List.of();
  private volatile ScheduledFuture<?> running;
  private volatile double rate;

  public Simulator(
      KafkaTemplate<String, FlightCompleted> kafka,
      RestClient.Builder http,
      @Value("${simulator.member-service-url}") String memberServiceUrl,
      @Value("${simulator.include-cabin:true}") boolean includeCabin,
      MeterRegistry meters) {
    this.kafka = kafka;
    this.members = http.baseUrl(memberServiceUrl).build();
    this.includeCabin = includeCabin;
    this.sentCounter = Counter.builder("simulator_flights_sent_total").register(meters);
  }

  public synchronized int loadMembers(int limit) {
    List<UUID> ids = members.get().uri("/members/ids?limit={l}", limit).retrieve()
        .body(new org.springframework.core.ParameterizedTypeReference<List<UUID>>() {});
    memberIds = ids == null ? List.of() : ids.stream().map(UUID::toString).toList();
    log.info("loaded {} member ids", memberIds.size());
    return memberIds.size();
  }

  /** Steady load: {@code eventsPerSecond} spread evenly over 10 ms ticks. */
  public synchronized void start(double eventsPerSecond) {
    stop();
    ensureMembers();
    rate = eventsPerSecond;
    double perTick = eventsPerSecond * TICK_MS / 1000.0;
    var carry = new double[1];
    running = ticker.scheduleAtFixedRate(() -> {
      try {
        carry[0] += perTick;
        int n = (int) carry[0];
        carry[0] -= n;
        for (int i = 0; i < n; i++) sendOne(randomMember());
      } catch (Exception e) {
        log.warn("tick failed: {}", e.toString());
      }
    }, 0, TICK_MS, TimeUnit.MILLISECONDS);
    log.info("simulating {} flights/s", eventsPerSecond);
  }

  public synchronized void stop() {
    if (running != null) {
      running.cancel(false);
      running = null;
      rate = 0;
      kafka.flush();
    }
  }

  /** As fast as the producer allows. With a memberId, every flight goes to that one member. */
  public long burst(long count, String memberId) {
    if (memberId == null) ensureMembers();
    long t0 = System.nanoTime();
    for (long i = 0; i < count; i++) {
      sendOne(memberId != null ? memberId : randomMember());
    }
    kafka.flush();
    return (System.nanoTime() - t0) / 1_000_000;
  }

  public Status status() {
    return new Status(running != null, rate, sent.get(), memberIds.size());
  }

  public record Status(boolean running, double eventsPerSecond, long sent, int members) {}

  private void sendOne(String memberId) {
    long now = System.currentTimeMillis();
    FlightCompleted flight = FlightGenerator.next(memberId, ThreadLocalRandom.current(), includeCabin, now);
    // Record timestamp = completion time: the earning engine's calendar-year window uses event time.
    kafka.send(new ProducerRecord<>(Topics.FLIGHT_COMPLETED, null, now, memberId, flight));
    sent.incrementAndGet();
    sentCounter.increment();
  }

  private String randomMember() {
    List<String> ids = memberIds;
    return ids.get(ThreadLocalRandom.current().nextInt(ids.size()));
  }

  private void ensureMembers() {
    if (memberIds.isEmpty() && loadMembers(100_000) == 0) {
      throw new IllegalStateException("no members enrolled; POST /members/bulk on member-service first");
    }
  }
}
