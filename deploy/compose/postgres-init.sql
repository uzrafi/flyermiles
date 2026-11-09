-- One Postgres instance, one database per service (no service reads another's tables,
-- except the reconciliation job, which only reads).
CREATE DATABASE members;
CREATE DATABASE ledger;
CREATE DATABASE redemptions;
CREATE DATABASE query;
