import sys
import json
import boto3
import uuid
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext

# 1. Context & Config
args = getResolvedOptions(sys.argv, ['config'])
config = json.loads(args['config'])

table_name = config['table']
sql_s3_path = config['sql']
evolve_schema = config.get('evolve_schema', False)
upsert_keys = config['upsert_keys']
partition_keys = config.get('partition_keys', [])
ignore_columns = config.get('ignore_columns', [])

sc = SparkContext()

glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Apply UUID to staging table name for parallel safety
unique_stg = f"stg_{table_name}_{str(uuid.uuid4())[:8]}"

# 2. Fetch SQL from S3
s3 = boto3.client('s3')
bucket, key = sql_s3_path.replace("s3://", "").split("/", 1)
sql_query = s3.get_object(Bucket=bucket, Key=key)['Body'].read().decode('utf-8')

# 3. Create Staging & Filter
raw_df = spark.sql(sql_query)
# Filter ignored columns
stg_df = raw_df.select([c for c in raw_df.columns if c not in ignore_columns])

# Drop duplicates on upsert keys
stg_df = stg_df.dropDuplicates(upsert_keys)

if partition_keys:

    # Ensure partitions are sorted to avoid iceberg IO errors
    stg_df = stg_df.sortWithinPartitions(*partition_keys)

# Create staging table
stg_df.createOrReplaceTempView(unique_stg)

target_table = f"iceberg.transactions.{table_name}"

table_check = spark.sql(f"SHOW TABLES IN iceberg.transactions LIKE '{table_name}'")

# 4. Create table
if table_check.count() == 0:

    # Initialize partitioned Iceberg table
    writer = stg_df.writeTo(target_table).using("iceberg")
    
    if partition_keys:
        writer = writer.partitionedBy(*partition_keys)
    
    writer.create()
else:
    # 5. Iceberg-Optimized MERGE
    # Inclusion of partition keys as upsert keys improved write efficiency! (partition pruning)
    on_clause = " AND ".join([f"target_table.{k} = source_table.{k}" for k in upsert_keys])
    
    spark.sql(f"""
        MERGE INTO {target_table} target_table
        USING {unique_stg} source_table
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

print(f"Completed Iceberg merge for {table_name}")
