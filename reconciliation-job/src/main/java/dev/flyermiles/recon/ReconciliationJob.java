package dev.flyermiles.recon;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Runs once and exits (a Kubernetes CronJob, or {@code kubectl create job --from=cronjob/...}).
 *
 * <p>The system is eventually consistent, so a single snapshot taken mid-flight would show
 * differences that are only lag. The job re-runs the checks until they pass or {@code settleTimeout}
 * runs out; only a difference that persists is reported. Results go to stdout, to the exit code and
 * to the Prometheus Pushgateway ({@code reconciliation_mismatch_total} drives the critical alert).
 */
@SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
public class ReconciliationJob {

  public static void main(String[] args) {
    System.exit(SpringApplication.exit(SpringApplication.run(ReconciliationJob.class, args)));
  }

  @Component
  static class Runner implements ApplicationRunner, ExitCodeGenerator {
    private static final Logger log = LoggerFactory.getLogger(Runner.class);

    @Value("${recon.ledger-db-url}") String ledgerUrl;
    @Value("${recon.query-db-url}") String queryUrl;
    @Value("${recon.db-user}") String dbUser;
    @Value("${recon.db-password}") String dbPassword;
    @Value("${recon.kafka-bootstrap}") String bootstrap;
    @Value("${recon.pushgateway-url:}") String pushgateway;
    @Value("${recon.settle-timeout:PT2M}") Duration settleTimeout;
    @Value("${recon.settle-interval:PT5S}") Duration settleInterval;

    private int exitCode;

    @Override
    public void run(ApplicationArguments args) throws Exception {
      try (var ledgerDs = ds(ledgerUrl); var queryDs = ds(queryUrl)) {
        var reconciler = new Reconciler(new JdbcTemplate(ledgerDs), new JdbcTemplate(queryDs), new TopicCounter(bootstrap));
        Instant started = Instant.now();
        Instant deadline = started.plus(settleTimeout);
        Reconciler.Result result;
        int passes = 0;
        while (true) {
          passes++;
          result = reconciler.run();
          if (result.mismatches() == 0 || Instant.now().isAfter(deadline)) break;
          log.info("pass {}: {} checks differ, waiting for the pipeline to settle", passes, result.mismatches());
          Thread.sleep(settleInterval.toMillis());
        }

        var report = new LinkedHashMap<String, Object>();
        report.put("ok", result.mismatches() == 0);
        report.put("mismatches", result.mismatches());
        report.put("passes", passes);
        report.put("elapsedMs", Duration.between(started, Instant.now()).toMillis());
        report.put("checks", result.checks());
        report.put("totals", result.totals());
        System.out.println("RECONCILIATION " + new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(report));

        push(result);
        exitCode = result.mismatches() == 0 ? 0 : 2;
      }
    }

    @Override
    public int getExitCode() {
      return exitCode;
    }

    private void push(Reconciler.Result result) {
      if (pushgateway.isBlank()) return;
      var body = new StringBuilder();
      body.append("# TYPE reconciliation_mismatch_total gauge\n");
      body.append("reconciliation_mismatch_total ").append(result.mismatches()).append('\n');
      body.append("# TYPE reconciliation_last_run_timestamp_seconds gauge\n");
      body.append("reconciliation_last_run_timestamp_seconds ").append(Instant.now().getEpochSecond()).append('\n');
      body.append("# TYPE reconciliation_check_ok gauge\n");
      result.checks().values().forEach(c ->
          body.append("reconciliation_check_ok{check=\"").append(c.name()).append("\"} ").append(c.ok() ? 1 : 0).append('\n'));
      body.append("# TYPE reconciliation_total gauge\n");
      result.totals().forEach((k, v) ->
          body.append("reconciliation_total{quantity=\"").append(k).append("\"} ").append(v).append('\n'));
      try {
        var res = HttpClient.newHttpClient().send(
            HttpRequest.newBuilder(URI.create(pushgateway + "/metrics/job/reconciliation"))
                .PUT(HttpRequest.BodyPublishers.ofString(body.toString())).build(),
            HttpResponse.BodyHandlers.discarding());
        log.info("pushed metrics to {} ({})", pushgateway, res.statusCode());
      } catch (Exception e) {
        log.warn("could not push metrics: {}", e.toString());
      }
    }

    private HikariDataSource ds(String url) {
      var ds = new HikariDataSource();
      ds.setJdbcUrl(url);
      ds.setUsername(dbUser);
      ds.setPassword(dbPassword);
      ds.setMaximumPoolSize(2);
      ds.setReadOnly(true);
      return ds;
    }
  }
}
