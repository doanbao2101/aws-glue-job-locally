import sys
import os
import re
import boto3
import logging
import pandas as pd
from io import BytesIO
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext

# -------------------------------------------------------
# Setup logging with emojis
# -------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

# -------------------------------------------------------
# Read ENV variables
# -------------------------------------------------------
args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "BRONZE_BUCKET", "SILVER_BUCKET", "FILES"])
# e.g. lanhm-dev-datalake-raw-bucket-us-west-2-.../galaxy/
BRONZE_BUCKET = args["BRONZE_BUCKET"]
# e.g. lanhm-dev-datalake-stage-bucket-us-west-2-.../galaxy/
SILVER_BUCKET = args["SILVER_BUCKET"]
FILES = args["FILES"]
JOB_NAME = args["JOB_NAME"]

logger.info(f"🚀 Starting Glue Job: {JOB_NAME}")
logger.info(f"📂 BRONZE_BUCKET: {BRONZE_BUCKET}")
logger.info(f"📂 SILVER_BUCKET: {SILVER_BUCKET}")
logger.info(f"📝 FILES param: {FILES}")

# -------------------------------------------------------
# Initialize Spark & Glue Context
# -------------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# -------------------------------------------------------
# Helpers
# -------------------------------------------------------


def to_snake_case(name: str) -> str:
    # replace spaces & special chars with "-"
    name = re.sub(r"[^0-9a-zA-Z]+", "-", name)
    name = re.sub(r"-+", "-", name)  # collapse multiple "-"
    return name.strip("-").lower()


def get_file_list(bucket: str, prefix: str, files_param: str):
    s3 = boto3.client("s3")
    if files_param.upper() == "ALL":
        logger.info("🔄 Scanning bucket for Excel files...")
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        file_keys = [c["Key"] for c in resp.get(
            "Contents", []) if c["Key"].endswith(".xlsx")]
        logger.info(f"✅ Found {len(file_keys)} Excel files in bucket.")
        return file_keys
    else:
        file_names = [f.strip() for f in files_param.split(",")]
        return [f"{prefix}{name}" for name in file_names]


# -------------------------------------------------------
# Main processing
# -------------------------------------------------------
s3 = boto3.client("s3")

bucket, prefix = BRONZE_BUCKET.split("/", 1)
file_keys = get_file_list(bucket, prefix, FILES)

for file_key in file_keys:
    file_name = os.path.basename(file_key).replace(".xlsx", "")
    logger.info(f"📥 Processing file: {file_name} (s3://{bucket}/{file_key})")

    obj = s3.get_object(Bucket=bucket, Key=file_key)
    excel_bytes = obj["Body"].read()
    xls = pd.ExcelFile(BytesIO(excel_bytes))
    sheet_names = xls.sheet_names
    logger.info(f"📑 Found sheets: {sheet_names}")

    # Special case: final-fiscal-sales.xlsx
    if file_name == "final-fiscal-sales":
        logger.info("🔄 Special handling for final-fiscal-sales.xlsx")
        combined_df = None

        for sheet in sheet_names:
            logger.info(f"🔎 Reading fiscal sheet: {sheet}")
            pdf = pd.read_excel(BytesIO(excel_bytes), sheet_name=sheet)
            pdf.columns = [to_snake_case(c) for c in pdf.columns]
            pdf["fiscal_year"] = sheet  # add column with sheet name

            sdf = spark.createDataFrame(pdf)
            combined_df = sdf if combined_df is None else combined_df.unionByName(
                sdf)

        record_count = combined_df.count()
        output_path = f"s3://{SILVER_BUCKET}/galaxy/final-fiscal-year/"
        logger.info(f"💾 Writing {record_count} records → {output_path}")
        combined_df.write.mode("overwrite").parquet(output_path)

    else:
        # Default behavior: each sheet → separate parquet
        for sheet in sheet_names:
            logger.info(f"🔎 Reading sheet: {sheet}")
            pdf = pd.read_excel(BytesIO(excel_bytes), sheet_name=sheet)
            pdf.columns = [to_snake_case(c) for c in pdf.columns]

            sdf = spark.createDataFrame(pdf)
            record_count = sdf.count()

            sheet_path = to_snake_case(sheet)
            output_path = f"s3://{SILVER_BUCKET}/galaxy/{file_name}/{sheet_path}/"

            logger.info(f"💾 Writing {record_count} records → {output_path}")
            sdf.write.mode("overwrite").parquet(output_path)


logger.info("🎉 Job completed successfully!")
