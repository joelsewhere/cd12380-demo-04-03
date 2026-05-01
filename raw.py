import pendulum
from airflow.sdk import DAG, Asset, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.glue_crawler import GlueCrawlerHook

S3_BUCKET      = "l4-lakehouse-dev-753900908173"
LANDING_PREFIX = "landing/"
GLUE_DB        = "raw"
WAREHOUSE_PATH = f"s3://{S3_BUCKET}/iceberg-warehouse/"
CRAWLER_NAME   = "raw"
GLUE_ROLE_NAME = "dev-lakehouse-glue-role"
AWS_CONN_ID    = "aws_default"
REGION         = "us-east-1"

RAW_INGESTION_COMPLETE = Asset("s3://l4-lakehouse-dev-753900908173/iceberg-warehouse/raw/")

with DAG(
    dag_id="glue_crawler",
    max_active_runs=1,
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    end_date=pendulum.datetime(2026, 1, 2, tz="UTC"),
    catchup=True,
) as dag:

    @task
    def capture_landing_keys(ds) -> list[str]:
        
        s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
        
        found_prefixes = set()

        prefixes = s3.list_prefixes(
            bucket_name=S3_BUCKET,
            prefix=LANDING_PREFIX,
            delimiter='/'
            )

        for prefix in prefixes:
            # Assuming structure: landing/table_name/ingested_date=YYYY-MM-DD/file
            ingestion_prefix = f"{prefix}ingested_date={ds}/"
            print(ingestion_prefix)
            for key in s3.list_keys(bucket_name=S3_BUCKET, prefix=ingestion_prefix):
                parts = key.split('/')
                # Point the crawler landing/table_name/
                schema_table_path = f"s3://{S3_BUCKET}/{parts[0]}/{parts[1]}/"
                found_prefixes.add(schema_table_path)
            
        return list(found_prefixes)

    @task.short_circuit
    def upsert_crawler(landing_prefixes: list[str]) -> None:
        if not landing_prefixes:
            return False
        
        hook = GlueCrawlerHook(aws_conn_id=AWS_CONN_ID, region_name=REGION)
        targets = [{"Path": p} for p in landing_prefixes]

        schema_policy = {
                    "UpdateBehavior": "UPDATE_IN_DATABASE",    # Auto evolve schema
                    "DeleteBehavior": "DEPRECATE_IN_DATABASE"  # or "LOG" to keep it safe
                    }
        
        recrawl_policy = {"RecrawlBehavior": "CRAWL_EVERYTHING"} # Required for updating schemas
        
        
        if hook.has_crawler(CRAWLER_NAME):

            hook.update_crawler(
                Name=CRAWLER_NAME,
                Targets={"S3Targets": targets}, # Point the crawler to updated list of tables
                SchemaChangePolicy=schema_policy,
                RecrawlPolicy=recrawl_policy,
                )
        else:
            hook.create_crawler(
                Name=CRAWLER_NAME,
                Role=GLUE_ROLE_NAME,
                DatabaseName=GLUE_DB, # Point crawler to "raw" namespace
                Targets={"S3Targets": targets},
                SchemaChangePolicy=schema_policy,
                RecrawlPolicy=recrawl_policy,
                )
        
        return True

    @task
    def run_crawler() -> None:
        hook = GlueCrawlerHook(aws_conn_id=AWS_CONN_ID, region_name=REGION)
        hook.start_crawler(CRAWLER_NAME)
        hook.wait_for_crawler_completion(CRAWLER_NAME)

    @task(outlets=[RAW_INGESTION_COMPLETE])
    def notify_complete(prefixes: list[str], **context) -> None:
        
        # Logic to extract table names from prefixes for the downstream asset
        tables = [p.rstrip('/').split('/')[-1] for p in prefixes]
        context['outlet_events'][RAW_INGESTION_COMPLETE].extra = {
            "ingested_date": context["ds"],
            "tables": tables,
        }

    prefixes = capture_landing_keys()
    upsert = upsert_crawler(prefixes)
    run = run_crawler()
    
    prefixes >> upsert >> run >> notify_complete(prefixes)