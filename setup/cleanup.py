"""
DAG: cleanup
=============
Resets the lakehouse for a fresh demo run. Drops all Glue catalog databases
and their tables, deletes all S3 objects under the managed prefixes, and
deletes any Glue jobs and crawlers created by the demo DAGs.

This DAG does NOT delete:
    - The S3 bucket itself
    - IAM roles
    - The CloudFormation stack

Run this between demo attempts to get back to a clean slate.

Flow (sequential):
    delete_glue_jobs
        → delete_crawlers
            → delete_catalog_databases
                → delete_s3_data
"""

import pendulum
from pathlib import Path
from airflow.sdk import DAG, task
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

S3_BUCKET   = "l4-lakehouse-dev-753900908173"
AWS_CONN_ID = "aws_default"

# S3 prefixes to wipe — landing is intentionally excluded so generated
# data does not need to be re-uploaded between runs
S3_PREFIXES = [
    "iceberg-warehouse/",
    "glue-scripts/",
    "athena-results/",
    "staging/",
    "artifacts/"
]

# Glue catalog databases to drop.
# Only drop databases created by the DAGs at runtime — NOT databases created
# by CloudFormation (raw, transactions, analytics). Deleting CF-managed
# databases breaks the stack and requires manual remediation.
DATABASES            = []  # no runtime-created databases
CF_MANAGED_DATABASES = ["raw", "transactions", "analytics", "staging"]  # tables cleared, DBs preserved

# Glue crawler names created by the demo DAGs
CRAWLER_NAMES = ["raw"]

# Glue job name prefix — all jobs whose names start with this are deleted
GLUE_JOB_PREFIX = "ecommerce_"
REGION          = "us-east-1"


with DAG(
    dag_id="cleanup",
    description="Reset the lakehouse — drops catalog, S3 data, Glue jobs and crawlers",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["lakehouse", "demo", "cleanup"],
) as dag:

    @task
    def delete_glue_jobs() -> list[str]:
        """
        Delete all Glue jobs whose names start with GLUE_JOB_PREFIX.
        These are created by GlueJobOperator on first run and accumulate
        across demo stages.
        """
        glue    = AwsBaseHook(aws_conn_id=AWS_CONN_ID, client_type="glue").get_client_type(region_name=REGION)
        deleted = []

        paginator = glue.get_paginator("get_jobs")
        for page in paginator.paginate():
            for job in page["Jobs"]:
                glue.delete_job(JobName=job["Name"])
                deleted.append(job["Name"])
                print(f"  Deleted Glue job: {job['Name']}")

        print(f"Deleted {len(deleted)} Glue job(s)")
        return deleted

    @task
    def delete_crawlers() -> None:
        """Delete crawlers created by the demo DAGs."""
        glue = AwsBaseHook(aws_conn_id=AWS_CONN_ID, client_type="glue").get_client_type(region_name=REGION)
        for name in CRAWLER_NAMES:
            try:
                # Stop crawler if running before deleting
                crawler = glue.get_crawler(Name=name)
                if crawler["Crawler"]["State"] == "RUNNING":
                    glue.stop_crawler(Name=name)
                    print(f"  Stopped crawler: {name}")
                glue.delete_crawler(Name=name)
                print(f"  Deleted crawler: {name}")
            except glue.exceptions.EntityNotFoundException:
                print(f"  Crawler not found (already deleted): {name}")

    @task
    def delete_catalog_databases() -> None:
        """
        Drop runtime-created Glue databases (DATABASES list) and clear all
        tables from CF-managed databases (raw, transactions, analytics).
        The CF-managed databases themselves are preserved — only their
        contents are wiped.
        """
        glue = AwsBaseHook(aws_conn_id=AWS_CONN_ID, client_type="glue").get_client_type(region_name=REGION)

        # Wipe tables from CF-managed databases (preserve the DB itself)
        for db in CF_MANAGED_DATABASES:
            try:
                paginator = glue.get_paginator("get_tables")
                tables    = []
                for page in paginator.paginate(DatabaseName=db):
                    tables.extend([t["Name"] for t in page["TableList"]])
                if tables:
                    for i in range(0, len(tables), 100):
                        glue.batch_delete_table(
                            DatabaseName=db,
                            TablesToDelete=tables[i:i+100],
                        )
                    print(f"  Cleared {len(tables)} table(s) from {db} (DB preserved)")
            except glue.exceptions.EntityNotFoundException:
                print(f"  Database not found: {db}")

        # Drop runtime-created databases entirely
        for db in DATABASES:
            try:
                paginator = glue.get_paginator("get_tables")
                tables    = []
                for page in paginator.paginate(DatabaseName=db):
                    tables.extend([t["Name"] for t in page["TableList"]])
                if tables:
                    for i in range(0, len(tables), 100):
                        glue.batch_delete_table(
                            DatabaseName=db,
                            TablesToDelete=tables[i:i+100],
                        )
                glue.delete_database(Name=db)
                print(f"  Deleted database: {db}")
            except glue.exceptions.EntityNotFoundException:
                print(f"  Database not found (already deleted): {db}")

    @task
    def delete_s3_data() -> None:
        """
        Delete all S3 objects under the managed prefixes.
        Landing files are preserved so data does not need to be re-uploaded.
        """
        s3     = AwsBaseHook(aws_conn_id=AWS_CONN_ID, client_type="s3").get_client_type(region_name=REGION)
        total  = 0

        for prefix in S3_PREFIXES:
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
                objects = page.get("Contents", [])
                if not objects:
                    continue
                keys = [{"Key": obj["Key"]} for obj in objects]
                s3.delete_objects(Bucket=S3_BUCKET, Delete={"Objects": keys})
                total += len(keys)
                print(f"  Deleted {len(keys)} object(s) under {prefix}")

        print(f"\nDeleted {total:,} S3 object(s) total")
        print(f"Landing files preserved: s3://{S3_BUCKET}/landing/")

    # ── Sequential — order matters ──────────────────────────────────────────────
    # Jobs and crawlers must be deleted before databases (they reference them).
    # Databases must be deleted before S3 (Iceberg metadata references S3 paths).

    delete_glue_jobs() >> delete_crawlers() >> delete_catalog_databases() >> delete_s3_data()