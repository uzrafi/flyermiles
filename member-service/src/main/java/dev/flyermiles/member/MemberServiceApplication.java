package dev.flyermiles.member;

import dev.flyermiles.avro.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@SpringBootApplication
public class MemberServiceApplication {

  public static void main(String[] args) {
    SpringApplication.run(MemberServiceApplication.class, args);
  }

  /** Compacted: the topic is a table (latest profile per member), not a history. */
  @Bean
  NewTopic memberProfiles() {
    return TopicBuilder.name(Topics.MEMBER_PROFILES).partitions(6).compact()
        .config(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "0").build();
  }

  @Bean
  DefaultErrorHandler errorHandler() {
    var backOff = new ExponentialBackOff(200, 2.0);
    backOff.setMaxInterval(10_000);
    return new DefaultErrorHandler(backOff);
  }
}
