package dev.flyermiles.simulator;

import dev.flyermiles.avro.Topics;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
public class SimulatorApplication {

  public static void main(String[] args) {
    SpringApplication.run(SimulatorApplication.class, args);
  }

  /** Same partition count as member.profiles: required for the stream-table join. */
  @Bean
  NewTopic flightCompleted() {
    return TopicBuilder.name(Topics.FLIGHT_COMPLETED).partitions(6).build();
  }

  @RestController
  @RequestMapping("/simulator")
  static class SimulatorController {
    private final Simulator sim;

    SimulatorController(Simulator sim) {
      this.sim = sim;
    }

    @PostMapping("/members/reload")
    Map<String, Integer> reload(@RequestParam(defaultValue = "100000") int limit) {
      return Map.of("members", sim.loadMembers(limit));
    }

    @PostMapping("/start")
    Simulator.Status start(@RequestParam(defaultValue = "100") double rate) {
      sim.start(rate);
      return sim.status();
    }

    @PostMapping("/stop")
    Simulator.Status stop() {
      sim.stop();
      return sim.status();
    }

    @PostMapping("/burst")
    Map<String, Object> burst(@RequestParam long count, @RequestParam(required = false) String memberId) {
      long ms = sim.burst(count, memberId);
      return Map.of("sent", count, "elapsedMs", ms, "eventsPerSecond", ms == 0 ? count : count * 1000 / ms);
    }

    @GetMapping("/status")
    Simulator.Status status() {
      return sim.status();
    }

    @ExceptionHandler(IllegalStateException.class)
    ResponseEntity<Map<String, String>> notReady(IllegalStateException e) {
      return ResponseEntity.status(409).body(Map.of("error", e.getMessage()));
    }
  }
}
