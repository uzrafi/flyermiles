package dev.flyermiles.ledger.domain;

import dev.flyermiles.avro.EntryType;
import java.time.Instant;
import java.util.UUID;

public record LedgerEntry(
    long globalSeq,
    UUID memberId,
    long seq,
    UUID entryId,
    EntryType type,
    long points,
    long balanceAfter,
    String reasonJson,
    Instant occurredAt,
    Instant sourceTimestamp) {}
