package dev.flyermiles.ledger.domain;

/** A member's current version ({@code seq} of their latest entry) and balance. */
public record MemberState(long seq, long balance) {}
