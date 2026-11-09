package dev.flyermiles.ledger.domain;

public sealed interface AppendResult {

  /** The entry was written. */
  record Appended(LedgerEntry entry) implements AppendResult {}

  /** An entry with this entryId already exists: the request was a retry, nothing new was written. */
  record Duplicate(LedgerEntry entry) implements AppendResult {}

  /** Another write got there first: {@code currentSeq} is no longer the caller's expected version. */
  record Conflict(long currentSeq) implements AppendResult {}

  /** A redemption would take the balance below zero. */
  record InsufficientBalance(long balance, long currentSeq) implements AppendResult {}
}
