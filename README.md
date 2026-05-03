## `transactions`

**Airflow DAG (`transactions.py`)**
- Schedule is **Asset-driven**: triggered automatically when the upstream `glue_crawler` DAG emits the `raw_ingestion_complete` Asset, so it runs the moment new raw data is registered in the catalog.
- Reads the most recent Asset event in the `metadata` task to extract the `ingested_date` and the list of `tables` that were just landed.
- A **branch task** (`trigger_upsert`) dynamically chooses which per-table upload tasks to execute based on the asset's `tables` list — only the tables present in the latest ingestion get promoted, even though all four tables are statically defined in the DAG.
- Iterates over `TABLES` (a config list with `partition_keys` and `upsert_keys` per table) to build a parallel branch per table — capped at 2 concurrent tasks via `max_active_tasks=2`.
- For each table, runs two tasks in sequence after the branch:
  - **`<table>_upload_sql`** — reads the per-table SQL file from the local `sql/` directory, renders it through Airflow's context aware templating engine, and uploads the rendered query to `s3://<bucket>/artifacts/transactions/sql/<run_id>/<table>.sql`. Run-ID-scoped paths prevent concurrent DAG runs from clobbering each other's templates.
  - **`promote_<table>`** — submits a Glue job (`transactions_promote_<table>`) running the shared `glue_script.py`, passing a `--config` JSON containing the table name, S3 path to the rendered SQL, partition keys, and upsert keys.
- Every Glue job is configured with the standard Iceberg-on-Glue settings (SQL extensions, dedicated `iceberg` catalog backed by Glue, S3FileIO, warehouse path) and `--enable-glue-datacatalog` so Spark's default `spark_catalog` can read the Hive-style `raw` tables.

**Glue script (`glue_script.py`)**
- Generic SQL runner: pulls the rendered SQL from the `--config` S3 path, deduplicates on upsert keys, and either creates the Iceberg table on first run (with the configured partitioning) or runs `MERGE INTO` for upsert on subsequent runs.

**SQL templates directory (`sql/<table>.sql`)**
- One file per table containing an explicit column list and inline validation logic — joins to dimension tables for referential checks, range/tolerance filters for corrupt records — so bad data is filtered out before it reaches `transactions`.
