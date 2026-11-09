CREATE TABLE members (
  member_id  UUID        PRIMARY KEY,
  name       TEXT        NOT NULL,
  email      TEXT        NOT NULL,
  tier       TEXT        NOT NULL DEFAULT 'BASE' CHECK (tier IN ('BASE','SILVER','GOLD','PLATINUM')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
