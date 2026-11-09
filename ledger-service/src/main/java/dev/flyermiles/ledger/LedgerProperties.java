package dev.flyermiles.ledger;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param snapshotEvery write a balance snapshot every N entries per member
 * @param relayBatchSize max entries published to ledger.events per relay transaction
 * @param maxAppendAttempts retries for internal appends (earn, reversal) that lose an optimistic race
 */
@ConfigurationProperties("ledger")
public record LedgerProperties(int snapshotEvery, int relayBatchSize, int maxAppendAttempts) {
  public LedgerProperties {
    if (snapshotEvery <= 0) snapshotEvery = 100;
    if (relayBatchSize <= 0) relayBatchSize = 500;
    if (maxAppendAttempts <= 0) maxAppendAttempts = 20;
  }
}
