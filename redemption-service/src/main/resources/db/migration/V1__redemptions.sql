-- Final outcome per Idempotency-Key. Retryable failures (gave up on contention) are not stored,
-- so the client can retry with the same key.
CREATE TABLE redemptions (
  idempotency_key TEXT        PRIMARY KEY,
  request_hash    TEXT        NOT NULL,
  member_id       UUID        NOT NULL,
  points          BIGINT      NOT NULL,
  reward_id       TEXT        NOT NULL,
  http_status     INT         NOT NULL,
  response        JSONB       NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
