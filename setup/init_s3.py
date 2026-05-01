"""
DAG: upload_landing_to_s3
==========================
Uploads pre-generated landing files from the local filesystem to S3.

Run the generator locally first:
    python generate_ecommerce_data.py
    python generate_ecommerce_data.py --schema-drift

Then trigger this DAG once to push everything to S3.

The local directory structure mirrors the S3 key structure exactly:
    local : {LOCAL_LANDING_PATH}/2026-01-01/customers/customers.csv
    s3    : s3://bucket/landing/2026-01-01/customers/customers.csv

LOCAL_LANDING_PATH should point to the landing/ folder produced by the
generator. Adjust it to match where you ran the script.
"""

import os
import pendulum
from pathlib import Path

from airflow.sdk import DAG, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

S3_BUCKET          = "l4-lakehouse-dev-753900908173"
S3_LANDING_PREFIX  = "landing/"
AWS_CONN_ID        = "aws_default"

REGION          = "us-east-1"
# Path to the landing/ directory produced by generate_ecommerce_data.py.
# Adjust this to match where you ran the generator.
LOCAL_LANDING_PATH = os.path.join(os.path.dirname(__file__), "..", "landing")


with DAG(
    dag_id="upload_landing_to_s3",
    description="Upload locally generated landing files to S3 — run after generate_ecommerce_data.py",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["lakehouse", "data-generation", "demo"],
) as dag:

    @task
    def upload_landing_files() -> None:
        """
        Walk the local landing/ directory and upload every file to S3,
        preserving the relative path as the S3 key.

        local:  landing/2026-01-01/orders/orders.json
        s3 key: landing/2026-01-01/orders/orders.json
        """
        landing_path = os.path.abspath(LOCAL_LANDING_PATH)

        if not os.path.exists(landing_path):
            raise FileNotFoundError(
                f"Landing directory not found: {landing_path}\n"
                f"Run generate_ecommerce_data.py first."
            )

        hook      = S3Hook(aws_conn_id=AWS_CONN_ID)
        uploaded  = 0

        for root, _, files in os.walk(landing_path):
            for filename in files:
                if filename.startswith("."):
                    continue

                local_file = os.path.join(root, filename)

                # Build the S3 key from the relative path within landing/
                relative   = os.path.relpath(local_file, os.path.dirname(landing_path))
                s3_key     = relative.replace(os.sep, "/")

                hook.load_file(
                    filename    = local_file,
                    key         = s3_key,
                    bucket_name = S3_BUCKET,
                    replace     = True,
                )
                print(f"  ✔  {s3_key}")
                uploaded += 1

        print(f"\nUploaded {uploaded} file(s) to s3://{S3_BUCKET}/landing/")

    upload_landing_files()