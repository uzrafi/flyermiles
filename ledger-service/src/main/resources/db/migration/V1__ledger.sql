-- The event store. The balance is never stored as mutable state: it is the sum of a member's entries.
CREATE TABLE ledger_entries (
  global_seq    BIGINT      GENERATED ALWAYS AS IDENTITY UNIQUE, -- publication order
  member_id     UUID        NOT NULL,
  seq           BIGINT      NOT NULL CHECK (seq >= 1),          -- per-member version: 1, 2, 3...
  entry_id      UUID        NOT NULL UNIQUE,                    -- = source eventId (idempotency)
  type          TEXT        NOT NULL CHECK (type IN ('EARNED','REDEEMED','REVERSED')),
  points        BIGINT      NOT NULL,                           -- signed
  balance_after BIGINT      NOT NULL,
  reason        JSONB       NOT NULL,                           -- flight number, fare class, reward id...
  occurred_at   TIMESTAMPTZ NOT NULL,
  source_ts     TIMESTAMPTZ,                                    -- when the triggering business event happened
  recorded_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (member_id, seq),                                 -- optimistic concurrency
  CHECK ((type = 'EARNED'   AND points > 0)
      OR (type = 'REDEEMED' AND points < 0)
      OR (type = 'REVERSED' AND points < 0)),
  -- Backstop for the application check: a redemption can never leave a negative balance.
  -- A reversal may (points already spent before the flight was cancelled).
  CHECK (balance_after >= 0 OR type = 'REVERSED')
);

-- No updates and no deletes, ever.
CREATE FUNCTION ledger_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'ledger_entries is append-only (% rejected)', TG_OP;
END $$;

CREATE TRIGGER ledger_entries_no_update_delete
  BEFORE UPDATE OR DELETE ON ledger_entries
  FOR EACH ROW EXECUTE FUNCTION ledger_append_only();

CREATE TRIGGER ledger_entries_no_truncate
  BEFORE TRUNCATE ON ledger_entries
  FOR EACH STATEMENT EXECUTE FUNCTION ledger_append_only();

-- Balance snapshot every N entries so rebuilding a member's state doesn't replay their whole history.
CREATE TABLE ledger_snapshots (
  member_id  UUID   NOT NULL,
  seq        BIGINT NOT NULL,
  balance    BIGINT NOT NULL,
  PRIMARY KEY (member_id, seq)
);

-- Entries committed but not yet published to the ledger.events topic. Filled by a trigger in the same
-- transaction as the append, drained by LedgerEventRelay: no dual write between Postgres and Kafka.
CREATE TABLE ledger_publish_queue (
  global_seq BIGINT PRIMARY KEY
);

CREATE FUNCTION ledger_enqueue_publish() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO ledger_publish_queue (global_seq) VALUES (NEW.global_seq);
  RETURN NULL;
END $$;

CREATE TRIGGER ledger_entries_enqueue
  AFTER INSERT ON ledger_entries
  FOR EACH ROW EXECUTE FUNCTION ledger_enqueue_publish();
