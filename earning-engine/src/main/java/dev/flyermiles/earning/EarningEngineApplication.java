package dev.flyermiles.earning;

import io.apicurio.registry.serde.avro.AvroKafkaDeserializer;
import io.apicurio.registry.serde.avro.AvroKafkaSerializer;
import java.time.Duration;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

@SpringBootApplication
@EnableKafkaStreams
public class EarningEngineApplication {

  public static void main(String[] args) {
    SpringApplication.run(EarningEngineApplication.class, args);
  }

  /** Apicurio-backed serdes configured from {@code spring.kafka.properties} (registry URL etc.). */
  @Bean
  AvroSerdes avroSerdes(KafkaProperties kafka) {
    Map<String, Object> config = new HashMap<>(kafka.getProperties());
    return new AvroSerdes() {
      @Override
      public <T extends SpecificRecord> Serde<T> forValue(Class<T> type) {
        Serde<T> serde = Serdes.serdeFrom(new AvroKafkaSerializer<T>(), new AvroKafkaDeserializer<T>());
        serde.configure(config, false);
        return serde;
      }
    };
  }

  @Autowired
  void buildTopology(
      StreamsBuilder builder,
      AvroSerdes avroSerdes,
      @Value("${earning.zone:America/Toronto}") String zone,
      @Value("${earning.late-flight-grace:P7D}") Duration grace) {
    EarningTopology.build(builder, avroSerdes, ZoneId.of(zone), grace);
  }

  /** Readiness follows the Streams state: not ready while rebalancing/restoring or after an error. */
  @Bean
  HealthIndicator kafkaStreamsHealthIndicator(StreamsBuilderFactoryBean factory) {
    return () -> {
      KafkaStreams streams = factory.getKafkaStreams();
      if (streams == null) {
        return Health.down().withDetail("state", "NOT_CREATED").build();
      }
      KafkaStreams.State state = streams.state();
      var health = state == KafkaStreams.State.RUNNING || state == KafkaStreams.State.REBALANCING
          ? Health.up() : Health.down();
      return health.withDetail("state", state.name()).build();
    };
  }
}
