-- Serves the keyset statement query from the index alone (Index Only Scan): the ORDER BY matches the
-- index order and every selected column is either a key or INCLUDEd. bench/statement-bench.sh drops and
-- recreates this index to measure the before/after.
CREATE INDEX statement_lines_keyset_idx
  ON statement_lines (member_id, occurred_at DESC, seq DESC)
  INCLUDE (type, points, balance_after);
