package dev.flyermiles.ledger.kafka;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
public class KafkaConfig {

  /** Redeliver with backoff rather than skipping: a skipped earn would be lost points. */
  @Bean
  DefaultErrorHandler errorHandler() {
    var backOff = new ExponentialBackOff(200, 2.0);
    backOff.setMaxInterval(10_000);
    return new DefaultErrorHandler(backOff);
  }
}
