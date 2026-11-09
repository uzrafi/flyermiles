package dev.flyermiles.ledger.domain;

import dev.flyermiles.avro.EntryType;
import java.time.Instant;
import java.util.UUID;

/**
 * @param points signed amount (positive for EARNED, negative otherwise)
 * @param expectedSeq the version the caller based its decision on; {@code null} means "whatever is
 *     current" (used by internal appends that retry on their own)
 */
public record AppendCommand(
    UUID memberId,
    UUID entryId,
    EntryType type,
    long points,
    String reasonJson,
    Instant occurredAt,
    Instant sourceTimestamp,
    Long expectedSeq) {}
