import sys
import boto3
import logging
from typing import List
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import DropNullFields

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
    'S3_SILVER_PREFIX'
])

JOB_NAME = args['JOB_NAME']
GLUE_DATABASE = args['GLUE_DATABASE']
S3_SILVER_BUCKET = args['S3_SILVER_BUCKET']
S3_SILVER_PREFIX = args['S3_SILVER_PREFIX']

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
# Data transformation logic
# -------------------------------------


def transform(datasource: DynamicFrame) -> DynamicFrame:
    cleaned = DropNullFields.apply(
        frame=datasource,
        transformation_ctx="dropnullfields"
    )
    return cleaned

# -------------------------------------
# Data validation logic
# -------------------------------------


def validate_data(frame: DynamicFrame, table_name: str) -> bool:
    """
    Add validation rules here. Return True if data is valid, False otherwise.
    You can extend this function to check:
    - Required columns exist
    - No empty critical fields
    - Value range, data types, etc.
    """
    try:
        df = frame.toDF()
        record_count = df.count()

        if record_count == 0:
            logging.warning(
                f"❌ Validation failed: no records in '{table_name}'")
            return False

        # Example: Validate a required column exists
        # customize this list per project
        required_columns = ["id", "timestamp"]
        for col in required_columns:
            if col not in df.columns:
                logging.warning(
                    f"❌ Validation failed: missing column '{col}' in '{table_name}'")
                return False

        # Example: Check if "id" column has any nulls
        if df.filter("id IS NULL").count() > 0:
            logging.warning(
                f"❌ Validation failed: null 'id' values in '{table_name}'")
            return False

        # Add other rules here as needed...

        return True

    except Exception as e:
        logging.error(
            f"❌ Exception during validation for table '{table_name}': {e}")
        return False

# -------------------------------------
# Process a single table
# -------------------------------------


def process_table(table_name: str):
    try:
        logging.info(f"📥 Processing table: {table_name}")

        datasource = glueContext.create_dynamic_frame.from_catalog(
            database=GLUE_DATABASE,
            table_name=table_name,
            transformation_ctx="datasource"
        )

        if datasource.count() == 0:
            logging.warning(f"⚠️ No data in table '{table_name}', skipping.")
            return

        transformed = transform(datasource)
        logging.info(f"🧹 Transformed data for table '{table_name}'")

        if not validate_data(transformed, table_name):
            logging.warning(
                f"🚫 Skipping write: data validation failed for '{table_name}'")
            # return

         # 👉 Cleaned table key name (remove "_json" suffix if present)
        table_key = table_name[:-
                               5] if table_name.endswith('_json') else table_name
        output_path = f"s3://{S3_SILVER_BUCKET}/{S3_SILVER_PREFIX}/{table_key}"
        logging.info(f"📤 Writing to: {output_path}")

        glueContext.purge_s3_path(output_path, {"retentionPeriod": 0})
        logging.info(f"🗑️ Purged existing data at: {output_path}")

        glueContext.write_dynamic_frame.from_options(
            frame=transformed,
            connection_type="s3",
            connection_options={
                "path": output_path,
                "partitionKeys": []
            },
            format="parquet",
            transformation_ctx="datasink"
        )

        logging.info(f"✅ Successfully overwrote data for table '{table_name}'")

    except Exception as e:
        logging.error(f"❌ Failed to process table '{table_name}': {e}")

# -------------------------------------
# Main job logic
# -------------------------------------


def main():
    tables = get_table_list(GLUE_DATABASE)
    logging.info(
        f"🔍 Starting processing for {len(tables)} tables in database '{GLUE_DATABASE}'")

    for table_name in tables:
        process_table(table_name)

    job.commit()
    logging.info("🏁 Glue job completed successfully.")


# -------------------------------------
# Entry point
# -------------------------------------
if __name__ == "__main__":
    main()
