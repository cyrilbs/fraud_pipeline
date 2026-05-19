-- Schema for fraud_streaming.py (key_space=mykeyspace, cass_table_name=fraud).
-- Columns match the BLOCK decision rows written by Spark (partition/offset dropped).

CREATE KEYSPACE IF NOT EXISTS mykeyspace
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS mykeyspace.fraud (
  transaction_id text PRIMARY KEY,
  fraud_probability text,
  decision text,
  timestamp timestamp
);
