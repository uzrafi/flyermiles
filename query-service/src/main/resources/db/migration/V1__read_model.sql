-- Read model, rebuilt at will from ledger.events. Never the source of truth.
CREATE TABLE member_balances (
  member_id  UUID        PRIMARY KEY,
  balance    BIGINT      NOT NULL,
  last_seq   BIGINT      NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE statement_lines (
  member_id     UUID        NOT NULL,
  seq           BIGINT      NOT NULL,
  entry_id      UUID        NOT NULL,
  type          TEXT        NOT NULL,
  points        BIGINT      NOT NULL,
  balance_after BIGINT      NOT NULL,
  occurred_at   TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (member_id, seq)  -- also makes the projection idempotent
);
