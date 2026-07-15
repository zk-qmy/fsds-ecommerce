-- Registers the Bronze and Silver Delta Lake tables (written by ingest_bronze.py /
-- transform_silver.py to MinIO) into Trino's Hive Metastore, so they're browsable
-- in DBeaver alongside the Gold PostgreSQL connection.
--
-- Prerequisite: infra/trino/catalog/delta.properties must have
--   delta.register-table-procedure.enabled=true
-- (added alongside the pre-existing delta.legacy-create-table-with-existing-location.enabled
-- flag — that older flag alone was not enough; CREATE TABLE ... WITH (location=...) errors
-- with a parser error on this connector version. register_table is the supported path.)
-- Restart the trino container after editing that file for it to take effect.
--
-- Prerequisite: Bronze must be populated (uv run python3
-- b_schema_pipelines/pipelines/bronze/ingest_bronze.py) and Silver must be populated
-- (uv run python3 b_schema_pipelines/pipelines/silver/transform_silver.py --mode optimized)
-- before running this — Trino reads each table's existing Delta transaction log to infer
-- schema, it does not create anything new.
--
-- Run once (idempotent — CALL register_table errors if already registered, which is fine to
-- ignore on a re-run):
--   docker exec fsds-trino trino -f /path/to/this/file.sql
-- or paste into DBeaver's SQL editor once connected to the Trino connection (see below).

CREATE SCHEMA IF NOT EXISTS delta.bronze WITH (location = 's3a://bronze-data/bronze/');
CREATE SCHEMA IF NOT EXISTS delta.silver WITH (location = 's3a://silver-data/silver/');

CALL delta.system.register_table(schema_name => 'bronze', table_name => 'customers',   table_location => 's3a://bronze-data/bronze/customers');
CALL delta.system.register_table(schema_name => 'bronze', table_name => 'products',    table_location => 's3a://bronze-data/bronze/products');
CALL delta.system.register_table(schema_name => 'bronze', table_name => 'orders',      table_location => 's3a://bronze-data/bronze/orders');
CALL delta.system.register_table(schema_name => 'bronze', table_name => 'order_items', table_location => 's3a://bronze-data/bronze/order_items');
CALL delta.system.register_table(schema_name => 'bronze', table_name => 'payments',    table_location => 's3a://bronze-data/bronze/payments');
CALL delta.system.register_table(schema_name => 'bronze', table_name => 'events',      table_location => 's3a://bronze-data/bronze/events');

CALL delta.system.register_table(schema_name => 'silver', table_name => 'customers',   table_location => 's3a://silver-data/silver/customers');
CALL delta.system.register_table(schema_name => 'silver', table_name => 'products',    table_location => 's3a://silver-data/silver/products');
CALL delta.system.register_table(schema_name => 'silver', table_name => 'orders',      table_location => 's3a://silver-data/silver/orders');
CALL delta.system.register_table(schema_name => 'silver', table_name => 'order_items', table_location => 's3a://silver-data/silver/order_items');
CALL delta.system.register_table(schema_name => 'silver', table_name => 'payments',    table_location => 's3a://silver-data/silver/payments');

-- Verify:
-- SHOW TABLES FROM delta.bronze;
-- SHOW TABLES FROM delta.silver;
-- SELECT COUNT(*) FROM delta.bronze.customers;
