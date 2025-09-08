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
from awsglue.job import Job
from datetime import datetime

# -------------------------------------------------------
# Setup logging with emojis
# -------------------------------------------------------
logger = logging.getLogger("my_glue_job")
logger.setLevel(logging.INFO)

if logger.hasHandlers():
    logger.handlers.clear()

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(console_handler)
logger.propagate = False  # prevent duplicate logs

# -------------------------------------------------------
# Explicit field mapping (for fields not handled well by snake_case)
# -------------------------------------------------------
FIELD_MAPPING = {
    "eventno": "event_no",
    "eventname": "event_name",
    "accesscode": "access_code_id",
    "Access Code": "access_code_name",
    "discno": "discount_no",
    "discamt": "discount_amount",
    "descr": "description",
    "price": "price",
    "tkt_qty": "ticket_qty",
    "TK Status": "ticket_status",
    "category": "category_id",
    "subcat": "sub_category_id",
    "Title 1?": "title",
    "charter?": "charter",
    "Street1": "street_1",
    "StartDateTime": "start_datetime",
    "OrderNo": "order_no",
    "PromotionCode": "promotion_code",
    "FirstName": "first_name",
    "LastName": "last_name",
    "Usedate": "use_date"
}

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
sc.setLogLevel("WARN")  # Silence Spark INFO/DEBUG logs

glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -------------------------------------------------------
# Helpers: Write logs to file
# -------------------------------------------------------


def to_kebab_case(name: str) -> str:
    """
    Convert a string into kebab-case format.

    - Replace spaces & special characters with hyphens.
    - Collapse multiple hyphens into one.
    - Strip leading/trailing hyphens.
    - Lowercase the result.

    Example:
        "Hello World!!  Test" -> "hello-world-test"
    """
    # Replace non-alphanumeric characters with hyphen
    name = re.sub(r"[^0-9a-zA-Z]+", "-", name)
    # Collapse multiple hyphens
    name = re.sub(r"-+", "-", name)
    # Strip leading/trailing hyphens & lowercase
    return name.strip("-").lower()


def to_snake_case(name: str) -> str:
    """
    Convert a string into snake_case format.

    - Replace spaces & special characters with underscores.
    - Collapse multiple underscores into one.
    - Strip leading/trailing underscores.
    - Lowercase the result.

    Example:
        "Hello World!!  Test" -> "hello_world_test"
    """
    # Replace non-alphanumeric characters with underscore
    name = re.sub(r"[^0-9a-zA-Z]+", "_", name)
    # Collapse multiple underscores
    name = re.sub(r"_+", "_", name)
    # Strip leading/trailing underscores & lowercase
    return name.strip("_").lower()


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


def process_attendance_or_membership(file_name: str, excel_bytes: bytes, sheet_names: list, silver_bucket: str):
    """
    Special handler for files: final-attendance, final-membership.

    - Group sheets into 3 categories: FY, Donation, Gift.
    - Each category will be combined and saved as a separate parquet table.
    """
    categories = {
        "fy": [s for s in sheet_names if s.lower().startswith("fy")],
        "donation": [s for s in sheet_names if "donation" in s.lower()],
        "gift": [s for s in sheet_names if "gift" in s.lower()]
    }

    for category, sheets in categories.items():
        if not sheets:
            logger.info(f"⚠️ No sheets found for category: {category}")
            continue

        combined_df = None
        for sheet in sheets:
            logger.info(f"🔎 Reading {category} sheet: {sheet}")
            pdf = pd.read_excel(BytesIO(excel_bytes), sheet_name=sheet)

            # Apply mapping + snake_case fallback
            pdf.columns = [FIELD_MAPPING.get(
                c, to_snake_case(c)) for c in pdf.columns]
            pdf["fiscal_year"] = sheet  # add traceability column

            sdf = spark.createDataFrame(pdf)
            combined_df = sdf if combined_df is None else combined_df.unionByName(
                sdf)

        record_count = combined_df.count()
        output_path = f"s3://{silver_bucket}/galaxy/{file_name}/{category}/"
        logger.info(f"💾 Writing {record_count} records → {output_path}")
        combined_df.write.mode("overwrite").parquet(output_path)


# -------------------------------------------------------
# Main processing
# -------------------------------------------------------
s3 = boto3.client("s3")

bucket, prefix = BRONZE_BUCKET.split("/", 1)
file_keys = get_file_list(bucket, prefix, FILES)

for file_key in file_keys:
    file_name = os.path.basename(file_key).replace(".xlsx", "")
    file_name = to_kebab_case(file_name)
    logger.info(f"📥 Processing file: {file_name} (s3://{bucket}/{file_key})")

    obj = s3.get_object(Bucket=bucket, Key=file_key)
    excel_bytes = obj["Body"].read()
    xls = pd.ExcelFile(BytesIO(excel_bytes))
    sheet_names = xls.sheet_names
    logger.info(f"📑 Found sheets: {sheet_names}")

    # Special case: final-fiscal-sales.xlsx
    if file_name in ("final-fiscal-sales", "final-event-sales"):
        logger.info(f"🔄 Special handling for {file_name}.xlsx")
        combined_df = None

        for sheet in sheet_names:
            logger.info(f"🔎 Reading fiscal sheet: {sheet}")

            pdf = pd.read_excel(BytesIO(excel_bytes), sheet_name=sheet)
            pdf.columns = [
                # ưu tiên mapping, fallback snake_case
                FIELD_MAPPING.get(c, to_snake_case(c))
                for c in pdf.columns
            ]
            pdf["fiscal_year"] = sheet  # add column with sheet name

            sdf = spark.createDataFrame(pdf)
            combined_df = sdf if combined_df is None else combined_df.unionByName(
                sdf)
        record_count = combined_df.count()
        output_path = f"s3://{SILVER_BUCKET}/galaxy/{file_name}/"
        logger.info(f"💾 Writing {record_count} records → {output_path}")
        combined_df.write.mode("overwrite").parquet(output_path)
    elif file_name in ("final-attendance", "final-membership"):
        logger.info(f"🔄 Special handling for {file_name}.xlsx")
        process_attendance_or_membership(
            file_name, excel_bytes, sheet_names, SILVER_BUCKET)
    else:
        # Default behavior: each sheet → separate parquet
        for sheet in sheet_names:
            logger.info(f"🔎 Reading sheet: {sheet}")
            pdf = pd.read_excel(BytesIO(excel_bytes), sheet_name=sheet)
            pdf.columns = [
                FIELD_MAPPING.get(c, to_snake_case(c))
                for c in pdf.columns
            ]

            sdf = spark.createDataFrame(pdf)
            record_count = sdf.count()
            sheet = to_kebab_case(sheet)
            output_path = f"s3://{SILVER_BUCKET}/galaxy/{file_name}/{sheet}/"
            logger.info(f"💾 Writing {record_count} records → {output_path}")

            sdf.write.mode("overwrite").parquet(output_path)

job.commit()
logger.info("🎉 Job completed successfully!")
