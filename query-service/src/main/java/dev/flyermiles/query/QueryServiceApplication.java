package dev.flyermiles.query;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@SpringBootApplication
public class QueryServiceApplication {

  public static void main(String[] args) {
    SpringApplication.run(QueryServiceApplication.class, args);
  }

  @Bean
  DefaultErrorHandler errorHandler() {
    var backOff = new ExponentialBackOff(200, 2.0);
    backOff.setMaxInterval(10_000);
    return new DefaultErrorHandler(backOff);
  }
}
