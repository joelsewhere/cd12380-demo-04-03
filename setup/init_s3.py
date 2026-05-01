import os
import pendulum
from pathlib import Path

from airflow.sdk import DAG, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

S3_BUCKET          = "l4-lakehouse-dev-753900908173"
S3_LANDING_PREFIX  = "landing/"
AWS_CONN_ID        = "aws_default"
REGION             = "us-east-1"
LOCAL_LANDING_PATH = os.path.join(os.path.dirname(__file__), "landing")


with DAG(
    dag_id="init_s3",
    description="Upload locally generated landing files to S3",
) as dag:

    @task
    def upload_landing_files() -> None:

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