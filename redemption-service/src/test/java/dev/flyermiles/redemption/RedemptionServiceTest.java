package dev.flyermiles.redemption;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.flyermiles.redemption.LedgerClient.Outcome;
import dev.flyermiles.redemption.LedgerClient.State;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

class RedemptionServiceTest {

  final UUID member = UUID.randomUUID();
  LedgerClient ledger;
  JdbcTemplate jdbc;
  RedemptionService service;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    ledger = mock(LedgerClient.class);
    jdbc = mock(JdbcTemplate.class);
    when(jdbc.query(anyString(), any(RowMapper.class), any())).thenReturn(List.of());
    service = new RedemptionService(ledger, jdbc, new ObjectMapper(), new SimpleMeterRegistry(), 5);
  }

  @Test
  void retriesWithAFreshVersionAfterAConflict() {
    when(ledger.state(member)).thenReturn(new State(7, 1000), new State(8, 900), new State(9, 800));
    when(ledger.redeem(eq(member), any(), eq(100L), anyLong(), eq("lounge")))
        .thenReturn(new Outcome.VersionConflict(8), new Outcome.VersionConflict(9),
            new Outcome.Written(UUID.randomUUID(), 10, 700));

    var res = service.redeem("key-1", new RedemptionService.Request(member, 100, "lounge"));

    assertThat(res.status()).isEqualTo(201);
    assertThat(res.body()).containsEntry("seq", 10L).containsEntry("balanceAfter", 700L);
    verify(ledger).redeem(eq(member), any(), eq(100L), eq(7L), any());
    verify(ledger).redeem(eq(member), any(), eq(100L), eq(8L), any());
    verify(ledger).redeem(eq(member), any(), eq(100L), eq(9L), any());
  }

  @Test
  void rejectsWithoutWritingWhenTheBalanceIsTooLow() {
    when(ledger.state(member)).thenReturn(new State(3, 50));

    var res = service.redeem("key-2", new RedemptionService.Request(member, 100, "lounge"));

    assertThat(res.status()).isEqualTo(422);
    verify(ledger, never()).redeem(any(), any(), anyLong(), anyLong(), any());
  }

  @Test
  void givesUpWithARetryable503UnderSustainedContention() {
    when(ledger.state(member)).thenReturn(new State(1, 1000));
    when(ledger.redeem(any(), any(), anyLong(), anyLong(), any())).thenReturn(new Outcome.VersionConflict(2));

    var res = service.redeem("key-3", new RedemptionService.Request(member, 100, "lounge"));

    assertThat(res.status()).isEqualTo(503);
    verify(ledger, times(5)).redeem(any(), any(), anyLong(), anyLong(), any());
  }

  @Test
  void theSameKeyAlwaysMapsToTheSameLedgerEntry() {
    when(ledger.state(member)).thenReturn(new State(1, 1000));
    var entryIds = new java.util.ArrayList<UUID>();
    when(ledger.redeem(any(), any(), anyLong(), anyLong(), any())).thenAnswer(inv -> {
      entryIds.add(inv.getArgument(1));
      return new Outcome.Written(inv.getArgument(1), 2, 900);
    });

    service.redeem("same-key", new RedemptionService.Request(member, 100, "lounge"));
    service.redeem("same-key", new RedemptionService.Request(member, 100, "lounge"));

    assertThat(entryIds).hasSize(2).allMatch(id -> id.equals(entryIds.get(0)));
  }
}
