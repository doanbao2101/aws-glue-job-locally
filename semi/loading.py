import sys
import boto3
import logging
from typing import List
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame

# -------------------------------------
# Logging setup
# -------------------------------------
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# -------------------------------------
# Glue job arguments
# -------------------------------------
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'GLUE_DATABASE',
    'S3_SILVER_BUCKET',
    'S3_SILVER_PREFIX',
    'DATA_DESTINATION'
])

JOB_NAME = args['JOB_NAME']
GLUE_DATABASE = args['GLUE_DATABASE']
S3_SILVER_BUCKET = args['S3_SILVER_BUCKET']
S3_SILVER_PREFIX = args['S3_SILVER_PREFIX']
DATA_DESTINATION = args['DATA_DESTINATION'].lower()

# -------------------------------------
# Initialize Spark / Glue context
# -------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

# -------------------------------------
# Get list of tables from Glue Catalog
# -------------------------------------


def get_table_list(database_name: str) -> List[str]:
    try:
        glue_client = boto3.client('glue')
        table_list = []
        paginator = glue_client.get_paginator('get_tables')
        for page in paginator.paginate(DatabaseName=database_name):
            table_list.extend(table['Name'] for table in page['TableList'])
        logging.info(f"Found tables in Glue Catalog: {table_list}")
        return table_list
    except Exception as e:
        logging.error(f"Failed to retrieve tables from Glue Catalog: {e}")
        return []

# -------------------------------------
# Load to Redshift
# -------------------------------------


def write_to_redshift(frame: DynamicFrame, table_name: str):
    try:
        redshift_tmp_dir = f"s3://{S3_SILVER_BUCKET}/temp/redshift/"
        glueContext.write_dynamic_frame.from_jdbc_conf(
            frame=frame,
            catalog_connection="redshift-connection",
            connection_options={
                "dbtable": table_name,
                "database": "your_redshift_db",
                "preactions": f"TRUNCATE TABLE {table_name}"
            },
            redshift_tmp_dir=redshift_tmp_dir,
            transformation_ctx="redshift_writer"
        )
        logging.info(f"✅ Wrote table '{table_name}' to Redshift.")
    except Exception as e:
        logging.error(f"❌ Redshift load failed for '{table_name}': {e}")

# -------------------------------------
# Load to DynamoDB
# -------------------------------------


def write_to_dynamodb(frame: DynamicFrame, table_name: str):
    try:
        glueContext.write_dynamic_frame.from_options(
            frame=frame,
            connection_type="dynamodb",
            connection_options={
                "dynamodb.output.tableName": table_name,
                "dynamodb.throughput.write.percent": "1.0"
            },
            transformation_ctx="dynamodb_writer"
        )
        logging.info(f"✅ Wrote table '{table_name}' to DynamoDB.")
    except Exception as e:
        logging.error(f"❌ DynamoDB load failed for '{table_name}': {e}")

# -------------------------------------
# Load to RDS (PostgreSQL)
# -------------------------------------


def write_to_postgres(frame: DynamicFrame, table_name: str):
    try:
        glueContext.write_dynamic_frame.from_jdbc_conf(
            frame=frame,
            catalog_connection="postgres-connection",
            connection_options={
                "dbtable": table_name,
                "database": "your_postgres_db",
                "preactions": f"TRUNCATE TABLE {table_name}"
            },
            transformation_ctx="postgres_writer"
        )
        logging.info(f"✅ Wrote table '{table_name}' to PostgreSQL.")
    except Exception as e:
        logging.error(f"❌ PostgreSQL load failed for '{table_name}': {e}")

# -------------------------------------
# Process a single table
# -------------------------------------


def process_table(table_name: str):
    try:
        logging.info(f"📥 Reading S3 parquet for table: {table_name}")

        input_path = f"s3://{S3_SILVER_BUCKET}/{S3_SILVER_PREFIX}/{table_name}"
        datasource = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [input_path]},
            format="parquet",
            transformation_ctx="datasource"
        )

        if datasource.count() == 0:
            logging.warning(f"⚠️ No data in '{table_name}', skipping.")
            return
        logging.info(f"📊 Loaded {datasource.count()} records from '{table_name}'.")
        
        # Uncomment the following lines to enable writing to the desired destination
        # if DATA_DESTINATION == "redshift":
        #     write_to_redshift(datasource, table_name)
        # elif DATA_DESTINATION == "dynamodb":
        #     write_to_dynamodb(datasource, table_name)
        # elif DATA_DESTINATION == "rds" or DATA_DESTINATION == "postgres":
        #     write_to_postgres(datasource, table_name)
        # else:
        #     logging.error(f"❌ Unknown DATA_DESTINATION: {DATA_DESTINATION}")
    except Exception as e:
        logging.error(f"❌ Failed to process '{table_name}': {e}")

# -------------------------------------
# Main job logic
# -------------------------------------


def main():
    tables = get_table_list(GLUE_DATABASE)
    logging.info(
        f"🚚 Starting load to '{DATA_DESTINATION}' for {len(tables)} tables")

    for table_name in tables:
        process_table(table_name)

    job.commit()
    logging.info("🏁 Data load job completed successfully.")

# -------------------------------------
# Entry point
# -------------------------------------
if __name__ == "__main__":
    main()
