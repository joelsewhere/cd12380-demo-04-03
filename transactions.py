import json
from pathlib import Path
from airflow.sdk import DAG, Asset, task
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

CWD                    = Path(__file__).parent
S3_BUCKET              = "{{ var.value.s3_bucket }}"
S3_SQL                 = 'artifacts/transactions/sql/{{ dag_run.run_id }}'
RAW_DB                 = "raw"
TRANSACTIONS_DB        = "transactions"
WAREHOUSE_PATH         = f"s3://{S3_BUCKET}/iceberg-warehouse/transactions/"
PROMOTE_SCRIPT         = CWD / "glue_script.py"
GLUE_ROLE_NAME         = "dev-lakehouse-glue-role"
AWS_CONN_ID            = "aws_default"
REGION                 = 'us-east-1'
SQL_DIR                = Path(__file__).parent / 'sql'
RAW_INGESTION_COMPLETE = Asset(f"raw_ingestion_complete")

TABLES = [
    {
        "table": "customers",
        "partition_keys": [],
        "upsert_keys": ["customer_id"],
    },
    {
        "table": "products",
        "partition_keys": [],
        "upsert_keys": ["product_id"],
    },
    {
        "table": "orders",
        "partition_keys": ["order_date"],
        "upsert_keys": ["order_id"],
    },
    {
        "table": "order_items",
        "partition_keys": ["order_date"],
        "upsert_keys": ["item_id"],
    },
]


with DAG(
    dag_id="transactions",
    schedule=RAW_INGESTION_COMPLETE, # Trigger when raw ingestion is complete
    max_active_tasks=2,
    max_active_runs=1,
) as dag:

    # Pull metadata from triggering asset event
    @task(inlets=[RAW_INGESTION_COMPLETE])
    def metadata(*, inlet_events, **context) -> dict:
        events = inlet_events[RAW_INGESTION_COMPLETE]
        if not events:
            raise ValueError("No asset events found.")
        latest = events[-1]
        result = {
            "ingested_date" : latest.extra["ingested_date"],
            "tables"        : latest.extra["tables"],
        }
        return result

    # Trigger transaction promotions
    # for tables with newly ingested records
    @task.branch
    def trigger_upsert(metadata):

        return [f'{table}_upload_sql' for table in metadata['tables']]

    branch = trigger_upsert(metadata())

    # Create promotion tasks for each table
    for table in TABLES:

        table_name = table['table']
        SQL = SQL_DIR / f'{table_name}.sql'
        SQL_KEY = S3_SQL + f'{table_name}.sql'

        # Render the table's SQL and push the
        # rendered query to S3. 
        @task(task_id=f"{table_name}_upload_sql")
        def upload(file, s3_bucket, s3_key, **context):
            
            ti = context['ti']
            sql_template = file.read_text()
            sql_rendered = ti.task.render_template(sql_template, context)

            hook = S3Hook()

            hook.load_string(
                string_data=sql_rendered,
                key=s3_key,
                bucket_name=s3_bucket,
                replace=True
                )

        # Set up table specific configurations
        payload = {
            "sql": f"s3://{S3_BUCKET}/{SQL_KEY}",
            }
        
        payload.update(table)

        # Execute glue script
        promote = GlueJobOperator(
            task_id             = f"promote_{table['table']}",
            job_name            = f"transactions_promote_{table['table']}",
            script_location     = PROMOTE_SCRIPT.as_posix(),
            s3_bucket           = S3_BUCKET,
            iam_role_name       = GLUE_ROLE_NAME,
            replace_script_file = True,
            verbose             = True,
            region_name         ='us-east-1',
            script_args         = {
                "--config": json.dumps(payload),
                "--datalake-formats": "iceberg",
                "--enable-glue-datacatalog": "",
                "--conf": (
                    "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
                    "--conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog "
                    "--conf spark.sql.catalog.iceberg.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog "
                    "--conf spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO "
                    f"--conf spark.sql.catalog.iceberg.warehouse=s3://{S3_BUCKET}/iceberg-warehouse/"
                ),
                "--warehouse_path": f"s3://{S3_BUCKET}/iceberg-warehouse/"
                },
            create_job_kwargs   = {
                "GlueVersion": "5.0",
                },
            wait_for_completion = True,
            aws_conn_id         = AWS_CONN_ID,
            )

        branch >> upload(SQL, S3_BUCKET, SQL_KEY) >> promote


