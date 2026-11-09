package dev.flyermiles.member;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * Topics are created by the platform (deploy/compose/create-topics.sh, Strimzi KafkaTopic), never by
 * services: a NewTopic bean makes KafkaAdmin silently ADD partitions to an existing topic, which
 * breaks co-partitioning between flight.completed and member.profiles.
 */
@SpringBootApplication
public class MemberServiceApplication {

  public static void main(String[] args) {
    SpringApplication.run(MemberServiceApplication.class, args);
  }

  @Bean
  DefaultErrorHandler errorHandler() {
    var backOff = new ExponentialBackOff(200, 2.0);
    backOff.setMaxInterval(10_000);
    return new DefaultErrorHandler(backOff);
  }
}
