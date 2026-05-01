"""
DAG 2 — Raw Auto Ingester  (Stage 3 — with outlet)
====================================================
Same as Stage 2 with one addition: notify_complete emits the
RAW_INGESTION_COMPLETE asset so DAG 3 (transactions) is triggered
automatically after each successful ingestion run.

The asset event carries:
    data_interval_start  — the interval that was just ingested
    data_interval_end    — used by DAG 3 to scope its MERGE
    tables               — which tables were updated this interval

DAG 3 uses the tables list to only promote entities that have new data,
avoiding unnecessary Glue cluster startups.

No schema evolution at this stage — schema_mode remains explicit.
"""

import pendulum
from pathlib import Path

from airflow.sdk import DAG, Asset, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.glue_crawler import GlueCrawlerHook

S3_BUCKET      = "l4-lakehouse-dev-753900908173"
LANDING_PREFIX = "landing/"
GLUE_DB        = "raw"
ICEBERG_SCRIPT       = (Path(__file__).parent / "glue_scripts" / "ingest_to_raw.py").as_posix()
WAREHOUSE_PATH = f"s3://{S3_BUCKET}/iceberg-warehouse/"
CRAWLER_NAME   = "ecommerce_raw_iceberg_crawler"
GLUE_ROLE_ARN  = "dev-lakehouse-glue-role"
GLUE_ROLE_NAME = "dev-lakehouse-glue-role"
AWS_CONN_ID    = "aws_default"

REGION          = "us-east-1"
RAW_INGESTION_COMPLETE = Asset("s3://l4-lakehouse-dev-753900908173/iceberg-warehouse/raw/")


with DAG(
    dag_id="dag_02_raw_ingester",
    description="Auto-ingest landing files into raw Iceberg — triggers DAG 3 via asset",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    end_date=pendulum.datetime(2026, 1, 2, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    tags=["lakehouse", "raw", "ingestion", "iceberg", "stage-3"],
) as dag:

    @task
    def capture_landing_keys(**context) -> list[str]:
        """
        Discover which schema/table pairs have files in landing for this interval.
        Returns a lightweight list of "schema/table" strings so XCom stays small.
        build_ingest_tasks re-derives the S3 paths from these names directly.
        """
        interval_start = context["ds"]
        s3             = S3Hook(aws_conn_id=AWS_CONN_ID)
        found          = []

        for schema_prefix in s3.list_prefixes(
            bucket_name=S3_BUCKET,
            prefix=f"{LANDING_PREFIX}{interval_start}/",
            delimiter="/"
        ) or []:
            schema = schema_prefix.rstrip("/").split("/")[-1]
            for table_prefix in s3.list_prefixes(
                bucket_name=S3_BUCKET, prefix=schema_prefix, delimiter="/"
            ) or []:
                table   = table_prefix.rstrip("/").split("/")[-1]
                matches = s3.list_keys(bucket_name=S3_BUCKET, prefix=table_prefix) or []
                if matches:
                    found.append(f"{schema}/{table}")
                    print(f"  [{interval_start}] {schema}/{table}: {len(matches)} file(s)")

        return found


    def _upload_script(local_path: str, s3_key: str) -> None:
        """Upload a local Glue script to S3, replacing any existing version."""
        S3Hook(aws_conn_id=AWS_CONN_ID).load_file(
            filename    = local_path,
            key         = s3_key,
            bucket_name = S3_BUCKET,
            replace     = True,
        )
        print(f"  Uploaded {local_path} → s3://{S3_BUCKET}/{s3_key}")

    @task
    def build_ingest_tasks(schema_tables: list[str], **context) -> list[str]:
        from airflow.providers.amazon.aws.hooks.glue import GlueJobHook

        # Upload latest local script to S3 before creating/running jobs
        _upload_script(ICEBERG_SCRIPT, "artifacts/glue-scripts/ingest_to_raw.py")

        interval_start   = context["data_interval_start"].strftime("%Y-%m-%d")
        interval_end     = context["data_interval_end"].strftime("%Y-%m-%d")
        iceberg_prefixes = []

        for schema_table in schema_tables:
            interval_dt    = context["ds"]
            landing_path   = f"s3://{S3_BUCKET}/{LANDING_PREFIX}{interval_dt}/{schema_table}/"
            schema, table = schema_table.split("/", 1)
            hook   = GlueJobHook(
                region_name       = REGION,
                job_name          = f"ecommerce_ingest_raw_{table}",
                s3_bucket         = S3_BUCKET,
                iam_role_name     = GLUE_ROLE_NAME,
                script_location   = f"s3://{S3_BUCKET}/artifacts/glue-scripts/ingest_to_raw.py",
                create_job_kwargs = {"GlueVersion": "4.0", "NumberOfWorkers": 2, "WorkerType": "G.1X", "Description": "Lakehouse demo job"},
                aws_conn_id       = AWS_CONN_ID,
            )
            response = hook.initialize_job(script_arguments={
                "--datalake-formats" : "iceberg",
                "--table"               : table,
                "--raw_db"              : "raw",
                "--warehouse_path"      : WAREHOUSE_PATH,
                "--schema_mode"         : "explicit",
                    "--auto_create"         : "true",
                "--landing_path"        : landing_path,
                "--data_interval_start" : interval_start,
                "--data_interval_end"   : interval_end,
            }, run_kwargs={})
            run_id = response["JobRunId"]
            state  = hook.job_completion(hook.job_name, run_id)['JobRunState']
            if state not in ("SUCCEEDED",):
                raise RuntimeError(f"Ingest job for '{table}' ended: {state}")
            iceberg_prefixes.append(f"{WAREHOUSE_PATH}raw/{table}/")

        return iceberg_prefixes

    @task
    def upsert_crawler(iceberg_prefixes: list[str]) -> None:
        if not iceberg_prefixes:
            return
        hook    = GlueCrawlerHook(aws_conn_id=AWS_CONN_ID, region_name='us-east-1')
        glue    = hook.get_conn()
        targets = [{"Path": p} for p in iceberg_prefixes]
        # Database is pre-created by CloudFormation — no boto3 call needed.
        # With Lake Formation enabled, creating databases from a Glue job
        # requires catalog-level CREATE_DATABASE permission which is not
        # granted to the Glue service role. The database is provisioned
        # in the CF stack instead.
        if hook.has_crawler(CRAWLER_NAME):
            existing = {t["Path"] for t in hook.get_crawler(CRAWLER_NAME)["Targets"].get("S3Targets", [])}
            new      = [t for t in targets if t["Path"] not in existing]
            if new:
                hook.update_crawler(**{
                    "Name"   : CRAWLER_NAME,
                    "Targets": {"S3Targets": list(existing) + new},
                })
        else:
            hook.create_crawler(
                **{
                    "Name"        : CRAWLER_NAME,
                    "Role"        : "dev-lakehouse-glue-role",
                    "DatabaseName": GLUE_DB,
                    "Targets"     : {"S3Targets": targets},
                }
            )

    @task
    def run_crawler(iceberg_prefixes: list[str]) -> None:
        if not iceberg_prefixes:
            return
        hook = GlueCrawlerHook(aws_conn_id=AWS_CONN_ID, region_name=REGION)
        if hook.get_crawler(CRAWLER_NAME)["State"] != "RUNNING":
            hook.start_crawler(CRAWLER_NAME)

    @task
    def wait_for_crawler(iceberg_prefixes: list[str]) -> None:
        if not iceberg_prefixes:
            return
        GlueCrawlerHook(aws_conn_id=AWS_CONN_ID, region_name=REGION).wait_for_crawler_completion(CRAWLER_NAME)

    @task(outlets=[RAW_INGESTION_COMPLETE])
    def notify_complete(schema_tables: list[str], *, outlet_events, **context) -> None:
        """
        Emit RAW_INGESTION_COMPLETE after successful ingestion.
        DAG 3 is scheduled on this asset and reads the interval and
        table list from the event extras to scope its work.
        """
        tables = [st.split("/", 1)[1] for st in schema_tables]
        outlet_events[RAW_INGESTION_COMPLETE].extra = {
            "data_interval_start": context["ds"],
            "data_interval_end"  : context["data_interval_end"].strftime("%Y-%m-%d"),
            "tables"             : tables,
        }
        print(f"Asset emitted | tables={tables}")
    # ──────────────────────────────

    schema_tables = capture_landing_keys()
    prefixes      = build_ingest_tasks(schema_tables)
    crawl    = upsert_crawler(prefixes)
    run      = run_crawler(prefixes)
    wait     = wait_for_crawler(prefixes)

    schema_tables >> prefixes >> crawl >> run >> wait >> notify_complete(schema_tables)