package dev.flyermiles.earning;

import static org.assertj.core.api.Assertions.assertThat;

import dev.flyermiles.avro.FlightCompleted;
import dev.flyermiles.avro.MemberProfile;
import dev.flyermiles.avro.PointsEarned;
import dev.flyermiles.avro.Tier;
import dev.flyermiles.avro.TierChanged;
import dev.flyermiles.avro.Topics;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EarningTopologyTest {

  static final ZoneId ZONE = ZoneId.of("America/Toronto");
  static final Instant MARCH_2026 = ZonedDateTime.of(2026, 3, 1, 12, 0, 0, 0, ZONE).toInstant();

  final PlainAvroSerdes avro = new PlainAvroSerdes();
  TopologyTestDriver driver;
  TestInputTopic<String, FlightCompleted> flights;
  TestInputTopic<String, MemberProfile> profiles;
  TestOutputTopic<String, PointsEarned> earned;
  TestOutputTopic<String, TierChanged> tierChanges;

  @BeforeEach
  void setUp() {
    var builder = new StreamsBuilder();
    EarningTopology.build(builder, avro, ZONE, Duration.ofDays(7));
    var props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "earning-test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
    props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
    driver = new TopologyTestDriver(builder.build(), props);

    var keys = Serdes.String();
    flights = driver.createInputTopic(Topics.FLIGHT_COMPLETED, keys.serializer(),
        avro.forValue(FlightCompleted.class).serializer());
    profiles = driver.createInputTopic(Topics.MEMBER_PROFILES, keys.serializer(),
        avro.forValue(MemberProfile.class).serializer());
    earned = driver.createOutputTopic(Topics.POINTS_EARNED, keys.deserializer(),
        avro.forValue(PointsEarned.class).deserializer());
    tierChanges = driver.createOutputTopic(Topics.TIER_CHANGED, keys.deserializer(),
        avro.forValue(TierChanged.class).deserializer());
  }

  @AfterEach
  void tearDown() {
    driver.close();
  }

  @Test
  void pointsAreDistanceTimesFareTimesTierBonus() {
    profile("m1", Tier.GOLD);
    fly("m1", 2000, "J", MARCH_2026);

    PointsEarned p = earned.readValue();
    // 2000 × 1.5 × 1.5 = 4500
    assertThat(p.getPoints()).isEqualTo(4500);
    assertThat(p.getTier()).isEqualTo(Tier.GOLD);
  }

  @Test
  void unknownMemberEarnsAtBaseThroughTheLeftJoin() {
    fly("stranger", 1000, "F", MARCH_2026);
    assertThat(earned.readValue().getPoints()).isEqualTo(2000);
  }

  @Test
  void tierChangeAppliesFromTheNextFlight() {
    profile("m2", Tier.BASE);
    fly("m2", 1000, "Y", MARCH_2026);
    profile("m2", Tier.PLATINUM);
    fly("m2", 1000, "Y", MARCH_2026.plusSeconds(60));

    assertThat(earned.readValuesToList()).extracting(PointsEarned::getPoints).containsExactly(1000L, 2000L);
  }

  @Test
  void crossingAThresholdEmitsTierChangedExactlyOnce() {
    for (int i = 0; i < 12; i++) {
      fly("m3", 2000, "Y", MARCH_2026.plusSeconds(i * 3600L)); // 2k, 4k, ... 24k
    }
    assertThat(tierChanges.isEmpty()).isTrue();

    fly("m3", 2000, "Y", MARCH_2026.plusSeconds(13 * 3600L)); // 26k → SILVER
    fly("m3", 2000, "Y", MARCH_2026.plusSeconds(14 * 3600L)); // 28k, still SILVER

    var changes = tierChanges.readValuesToList();
    assertThat(changes).hasSize(1);
    assertThat(changes.get(0).getPreviousTier()).isEqualTo(Tier.BASE);
    assertThat(changes.get(0).getNewTier()).isEqualTo(Tier.SILVER);
    assertThat(changes.get(0).getQualifyingMiles()).isEqualTo(26_000);
    assertThat(changes.get(0).getYear()).isEqualTo(2026);
  }

  @Test
  void oneLongFlightCanJumpStraightToPlatinum() {
    fly("m4", 80_000, "Y", MARCH_2026);
    var change = tierChanges.readValue();
    assertThat(change.getPreviousTier()).isEqualTo(Tier.BASE);
    assertThat(change.getNewTier()).isEqualTo(Tier.PLATINUM);
  }

  @Test
  void qualifyingMilesResetOnJanuaryFirst() {
    Instant dec31 = ZonedDateTime.of(2026, 12, 31, 22, 0, 0, 0, ZONE).toInstant();
    Instant jan1 = ZonedDateTime.of(2027, 1, 1, 9, 0, 0, 0, ZONE).toInstant();
    fly("m5", 20_000, "Y", dec31);
    fly("m5", 20_000, "Y", jan1); // a new window: 20k in 2027, not 40k

    assertThat(tierChanges.isEmpty()).isTrue();

    fly("m5", 6_000, "Y", jan1.plusSeconds(3600)); // 26k in 2027
    assertThat(tierChanges.readValue().getYear()).isEqualTo(2027);
  }

  @Test
  void calendarYearWindowHandlesLeapYears() {
    var windows = new CalendarYearWindows(ZONE, Duration.ZERO);
    long feb29 = ZonedDateTime.of(2028, 2, 29, 12, 0, 0, 0, ZONE).toInstant().toEpochMilli();
    var w = windows.windowsFor(feb29).values().iterator().next();
    assertThat(Duration.ofMillis(w.end() - w.start())).isEqualTo(Duration.ofDays(366));
  }

  private void profile(String memberId, Tier tier) {
    profiles.pipeInput(memberId, MemberProfile.newBuilder()
        .setMemberId(memberId).setName("Test").setTier(tier).setUpdatedAtMs(0).build());
  }

  private void fly(String memberId, int miles, String fare, Instant when) {
    flights.pipeInput(memberId, FlightCompleted.newBuilder()
        .setEventId(UUID.randomUUID().toString()).setMemberId(memberId)
        .setFlightNumber("FM1").setOrigin("YYZ").setDestination("YVR")
        .setDistanceMiles(miles).setFareClass(fare).setCompletedAtMs(when.toEpochMilli())
        .build(), when);
  }
}
