import sys
import os
import re
import boto3
import logging
import pandas as pd
from io import BytesIO
from collections import defaultdict
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F
from datetime import datetime
import openpyxl
from pyspark.sql import DataFrame
import psycopg2
import numpy as np

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
    "Usedate": "use_date",
    "GiftMemStatus": "gift_membership_status",
    "DiscAmt": "discount_amount",
    "Rev": "revenue",
    "usedate": "use_date",
    "eventdate": "event_date",
    "eventid": "event_id",
    "facilityname": "facility_name",
    "nodenumber": "node_number"
}
PIPELINE_STAGE = ["Ingestion", "Transformation", "Loading"]

# -------------------------------------------------------
# Read ENV variables
# -------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "BRONZE_BUCKET",
        "SILVER_BUCKET",
        "FILES",
        "JDBC_URL",
        "DB_USER",
        "DB_PASSWORD",
        "DE_USER"
    ],
)


JDBC_URL = args["JDBC_URL"]
DB_USER = args["DB_USER"]
DB_PASSWORD = args["DB_PASSWORD"]
DE_USER = args["DE_USER"]

# e.g. lanhm-dev-datalake-raw-bucket-us-west-2-.../galaxy/
BRONZE_BUCKET = args["BRONZE_BUCKET"]
# e.g. lanhm-dev-datalake-stage-bucket-us-west-2-.../galaxy/
SILVER_BUCKET = args["SILVER_BUCKET"]
FILES = args["FILES"]
JOB_NAME = args["JOB_NAME"]
S3 = boto3.client("s3")
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
# Helpers
# -------------------------------------------------------


def jdbc_update_data_checkpoint(group_name: str, code: str, user: str = "Bao Doan"):
    clean_url = JDBC_URL.replace("jdbc:postgresql://", "")
    host_port, dbname = clean_url.split("/")
    host, port = host_port.split(":")
    port = int(port)
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cur = conn.cursor()
    formated_timestamp = "to_char(now() + interval '7 hours', 'DD-MM-YYYY HH24:MI:SS')::timestamp"
    cur.execute(
        f"""
        INSERT INTO control.data_checkpoints (
            group_name, name, code, data_upto_ts, note, created_at, updated_at, created_by, updated_by
        )
        VALUES ('{group_name}', NULL, '{group_name.lower()}.{code}', {formated_timestamp},
                '', {formated_timestamp}, {formated_timestamp}, '{user}', '{user}')
        ON CONFLICT (group_name,code)
        DO UPDATE
        SET
            data_upto_ts = EXCLUDED.data_upto_ts,
            updated_at   = EXCLUDED.updated_at,
            updated_by   = EXCLUDED.updated_by;
    """)

    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"📌 Updated Checkpoint: {code}")


def get_sheet_names_from_s3(bucket, key):
    obj = S3.get_object(Bucket=bucket, Key=key)
    wb = openpyxl.load_workbook(BytesIO(obj["Body"].read()), read_only=True)
    return wb.sheetnames


def read_sheet_to_sdf_spark(s3_path: str, sheet: str, extra_cols=None) -> DataFrame:
    """
    Read Excel sheet directly into Spark DataFrame using spark-excel.
    - Avoids loading entire file into pandas.
    """
    try:
        sdf = (
            sc.read
            .format("com.crealytics.spark.excel")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("treatEmptyValuesAsNulls", "true")
            .option("addColorColumns", "false")
            .option("dataAddress", f"'{sheet}'!A1")   # Start from A1
            .load(s3_path)
        )

        # Standardize column names
        for old_col in sdf.columns:
            new_col = FIELD_MAPPING.get(old_col, to_snake_case(old_col))
            sdf = sdf.withColumnRenamed(old_col, new_col)

        # Add extra columns if any
        if extra_cols:
            for col, val in extra_cols.items():
                sdf = sdf.withColumn(col, F.lit(val))

        return sdf

    except Exception as e:
        logger.error(
            f"❌ Error reading sheet '{sheet}' from {s3_path}: {e}", exc_info=True)
        return None


def extract_fy(text: str) -> str:
    """
    Extract the fiscal year code (e.g., FY17, FY18, FY20)
    from a given string.
    """
    match = re.search(r'FY\d{2}', text, re.IGNORECASE)
    return match.group(0) if match else ""


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


def detect_categories(sheet_names):
    """
    Detect categories from sheet names based on prefixes before FY codes.
    """
    categories = defaultdict(list)

    for s in sheet_names:
        match = re.match(r"^([A-Za-z]+)?\s*(FY\d+)$", s.strip(), re.IGNORECASE)
        if match:
            prefix, fy = match.groups()
            if prefix:
                cat = prefix.lower()
            else:
                cat = "FY"
            categories[cat].append(s)
        else:
            categories["OTHERS"].append(s)
    logger.info(f"🗂️ Detected categories in file: {dict(categories)}")
    return dict(categories)


def optimize_coalesce(df, record_count, target_file_size_mb=128, max_partitions=200):
    """
    [NEED TO DO LATER]
    Optimize number of partitions before writing parquet.
    """
    try:
        # size_in_bytes = df._jdf.logicalPlan().stats().sizeInBytes()
        # if size_in_bytes <= 0:
        #     # fallback: giả định 1 KB mỗi row
        #     size_in_bytes = record_count * 1024

        # target_bytes = target_file_size_mb * 1024 * 1024
        # est_partitions = max(1, int(size_in_bytes / target_bytes))

        # est_partitions = min(est_partitions, max_partitions)

        # logger.info(f"📦 Estimated data size: {size_in_bytes/1024/1024:.2f} MB")
        # logger.info(f"🎯 Target file size: {target_file_size_mb} MB")
        # logger.info(f"️🔁 Suggested partitions: {est_partitions}")

        # # return df.coalesce(est_partitions)
        return df.coalesce(10)

    except Exception as e:
        logger.warning(
            f"⚠️ Could not estimate size, fallback coalesce(10): {e}")
        return df.coalesce(10)


def build_code_checkpoint(data_source: str, key: str, sub_key: str) -> str:
    """
    Build a code checkpoint in the format
    Output: "data_source.key.sub_key" in snake_case
    """
    result = f"{data_source}.{to_snake_case(key)}"
    if sub_key:
        result += f".{to_snake_case(sub_key)}"
    return result


# -------------------------------------------------------
# Transformation functions for excel files
# -------------------------------------------------------

def transform_excel_to_sdf(excel_bytes, sheet, extra_cols=None):
    """
    Read an Excel sheet into Spark DataFrame with standardized columns.
    - Apply FIELD_MAPPING with fallback to snake_case.
    - Add any extra columns provided in `extra_cols`.
    """
    try:
        pdf = pd.read_excel(BytesIO(excel_bytes), sheet_name=sheet)
        # Standardize column names:
        # pdf.columns = [FIELD_MAPPING.get(c, to_snake_case(c)) ]

        new_columns = []
        for old_col in pdf.columns:
            # Standardize column name
            new_col = FIELD_MAPPING.get(old_col, to_snake_case(old_col))
            new_columns.append(new_col)

            # If column is datetime, convert it
            if np.issubdtype(pdf[old_col].dtype, np.datetime64):
                pdf[old_col] = pd.to_datetime(
                    pdf[old_col]).dt.strftime("%Y-%m-%d")

        pdf.columns = new_columns

        # Add extra columns if any
        if extra_cols:
            for col, val in extra_cols.items():
                pdf[col] = val

        # Convert to Spark DataFrame
        target_partitions = max(10, len(pdf) // 50000)

        return spark.createDataFrame(pdf).repartition(target_partitions)

    except Exception as e:
        logger.error(f"❌ Error reading sheet '{sheet}': {e}", exc_info=True)
        return None   # return None to indicate failure


def transform_all_fiscal_sheets_to_single_parquet(file_name, excel_bytes, sheet_names):
    """
        File with multiple fiscal year sheets (e.g., FY17, FY18, FY19). Union sheets into one Parquet output.
    """

    logger.info(f"🔄 Special handling for {file_name}.xlsx")
    combined_df = None

    for sheet in sheet_names:
        try:
            if file_name == "final-attendance":
                extra_cols = {}
                extra_cols["museum_code"] = sheet.split()[0]  # e.g. Hart FY20
                extra_cols["fiscal_year"] = sheet.split()[1]
                sdf = transform_excel_to_sdf(excel_bytes, sheet, extra_cols)
                if "event_date" in sdf.columns:
                    sdf = sdf.withColumn(
                        "event_date",
                        sdf["event_date"].cast("timestamp")
                    )
            else:
                sdf = transform_excel_to_sdf(excel_bytes, sheet, {
                    "fiscal_year": sheet})

            record_count = sdf.count()
            logger.info(
                f"\n🔎 Reading fiscal sheet: {sheet} → {record_count} records")
            combined_df = sdf if combined_df is None else combined_df.unionByName(
                sdf)

        except Exception as e:
            logger.error(
                f"❌ Error processing sheet '{sheet}' in file {file_name}: {e}", exc_info=True)
            continue  # skip this sheet, keep going

    if combined_df:
        write_silver_and_load_dw(
            combined_df=combined_df,
            file_name=file_name,
            category=None,
            silver_bucket=SILVER_BUCKET,
            target_schema="dw",
            mode="overwrite"
        )

def transform_fiscal_sheets_by_category_to_parquet(file_name, excel_bytes, sheet_names):
    """
        File with multiple fiscal year sheets (e.g., FY17, FY18, FY19).
        Sheets may be categorized into "donation", "gift", or "fy" types.
        Union sheets of same category into one Parquet output.
    """
    logger.info(f"🔄 Special handling for {file_name}.xlsx")
    categories = detect_categories(sheet_names)
    for category, sheets in categories.items():
        if not sheets:
            logger.info(f"⚠️ No sheets found for category: {category}")
            continue

        combined_df = None
        for sheet in sheets:
            try:
                sdf = transform_excel_to_sdf(excel_bytes, sheet, {
                    "fiscal_year": extract_fy(sheet)})
                record_count = sdf.count()
                logger.info(
                    f"\n🔎 Reading fiscal sheet: {sheet} → {record_count} records")
                combined_df = sdf if combined_df is None else combined_df.unionByName(
                    sdf)

            except Exception as e:
                logger.error(
                    f"❌ Error processing sheet '{sheet}' in {file_name}/{category}: {e}", exc_info=True)
                continue

        if combined_df:
            write_silver_and_load_dw(
                combined_df=combined_df,
                file_name=file_name,
                category=category,
                silver_bucket=SILVER_BUCKET,
                target_schema="dw",
                mode="overwrite"
            )


def transform_each_sheet_to_parquet(file_name, excel_bytes, sheet_names):
    """
        Default transformation: each sheet becomes one Parquet output.
    """
    for sheet in sheet_names:
        try:
            sdf = transform_excel_to_sdf(excel_bytes, sheet)

            # sdf = read_sheet_to_sdf_spark(s3_path, sheet)
            record_count = sdf.count()
            logger.info(
                f"\n🔎 Reading fiscal sheet: {sheet} → {record_count} records")

            write_silver_and_load_dw(
                combined_df=sdf,
                file_name=file_name,
                category=to_kebab_case(sheet),
                silver_bucket=SILVER_BUCKET,
                target_schema="dw",
                mode="overwrite"
            )

        except Exception as e:
            logger.error(
                f"❌ Error processing sheet '{sheet}' in file {file_name}: {e}", exc_info=True)
            continue

# -------------------------------------------------------
# Loading data to Data Warehouses (Postgres)
# -------------------------------------------------------


def write_silver_and_load_dw(
    combined_df,
    file_name: str,
    category: str | None,
    silver_bucket: str,
    target_schema: str = "dw",
    mode: str = "overwrite"
) -> None:
    """
    Process a Spark DataFrame and load it into both S3 (Silver layer) and Postgres (Data Warehouse).

    Steps:
    1. Count records in the DataFrame.
    2. Build output path in Silver S3 bucket.
    3. Generate checkpoint code for tracking.
    4. Write DataFrame as Parquet to S3.
    5. Load DataFrame into Postgres target schema.

    Args:
        combined_df: Spark DataFrame to be processed.
        file_name (str): The name of the source file.
        category (str): Data category / sub-key.
        silver_bucket (str): Target Silver S3 bucket name.
        target_schema (str, optional): Postgres schema to load into. Default = "dw".
        mode (str, optional): Load mode for Postgres. Default = "overwrite".

    Returns:
        None
    """
    # Step 1: Count records
    record_count = combined_df.count()

    # Step 2: Write S3 output path
    base_path = f"s3://{silver_bucket}/galaxy/{file_name}"
    output_path = f"{base_path}/{category}/" if category else f"{base_path}/"

    # Step 3: Generate checkpoint code
    checkpoint_code = build_code_checkpoint(
        data_source="galaxy",
        key=file_name,
        sub_key=category
    )

    # Step 4: Write DataFrame to Parquet (Silver layer)
    write_parquet_to_s3(
        df=combined_df,
        output_path=output_path,
        record_count=record_count,
        code=checkpoint_code
    )

    # Step 5: Load DataFrame into Postgres (Data Warehouse)
    load_parquet_to_postgres(
        df=combined_df,
        file_key=f"{file_name}_{category}" if category else file_name,
        target_schema=target_schema,
        code=checkpoint_code,
        mode=mode
    )


def write_parquet_to_s3(df: DataFrame, output_path: str, record_count: int, code: str, mode="overwrite"):
    """
    Write a Spark DataFrame to Parquet with logging.
    """
    df = optimize_coalesce(df, record_count, target_file_size_mb=128)
    logger.info(f"️️🔁 Writing {record_count} records → {output_path}")
    df.write.mode(mode).parquet(output_path)
    logger.info(
        f"🥈 Successfully written: {record_count} records → {output_path}")

    # Update checkpoint after transformation
    jdbc_update_data_checkpoint(
        group_name=PIPELINE_STAGE[1],
        code=code,
        user=DE_USER)


def load_parquet_to_postgres(
    df: DataFrame,
    file_key: str,
    target_schema: str,
    code: str,
    mode: str = "overwrite"
):
    """
    Read parquet data from Silver S3 bucket and write to RDS Postgres.

    Args:
        spark: SparkSession
        silver_bucket (str): S3 bucket Silver layer
        file_name (str): folder name inside galaxy (kebab-case)
        jdbc_url (str): JDBC URL of target Postgres
        target_schema (str): Postgres schema name
        table_name (str): Postgres table name
        db_user (str): DB username
        db_password (str): DB password
        mode (str): "overwrite" or "append"
    """
    try:
        table_name = to_snake_case(file_key)
        record_count = df.count()
        logger.info(
            f"🔁 Loading {record_count} records → galaxy_{table_name}...")

        df.write.format("jdbc") \
            .mode(mode) \
            .option("url", JDBC_URL) \
            .option("dbtable", f'"{target_schema}"."galaxy_{table_name}"') \
            .option("user", DB_USER) \
            .option("password", DB_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .option("batchsize", "5000") \
            .option("numPartitions", "8") \
            .save()
        logger.info(
            f"🥇 Successfully loaded {record_count} records to {target_schema}.{table_name}")

        jdbc_update_data_checkpoint(
            group_name=PIPELINE_STAGE[2],
            code=code,
            user=DE_USER)

    except Exception as e:
        logger.error(
            f"❌ Error writing parquet {table_name} to Postgres: {e}", exc_info=True)


def read_and_load_parquet_to_postgres(
    spark,
    silver_bucket: str,
    file_key: str,
    jdbc_url: str,
    target_schema: str,
    db_user: str,
    db_password: str,
    mode: str = "overwrite"
):
    """
    Read parquet data from Silver S3 bucket and write to RDS Postgres.

    Args:
        spark: SparkSession
        silver_bucket (str): S3 bucket Silver layer
        file_name (str): folder name inside galaxy (kebab-case)
        jdbc_url (str): JDBC URL of target Postgres
        target_schema (str): Postgres schema name
        table_name (str): Postgres table name
        db_user (str): DB username
        db_password (str): DB password
        mode (str): "overwrite" or "append"
    """
    try:
        s3_path = f"s3://{silver_bucket}/galaxy/{file_key}/"
        logger.info(f"📥 Loading parquet from {s3_path}")
        df: DataFrame = spark.read.parquet(s3_path)
        table_name = to_snake_case(file_key)
        record_count = df.count()
        logger.info(f"️🔁 Loaded {record_count} records from parquet")

        df.write.format("jdbc") \
            .mode(mode) \
            .option("url", jdbc_url) \
            .option("dbtable", f'"{target_schema}"."galaxy_{table_name}"') \
            .option("user", db_user) \
            .option("password", db_password) \
            .option("driver", "org.postgresql.Driver") \
            .option("batchsize", "5000") \
            .option("numPartitions", "8") \
            .save()

        logger.info(
            f"🥇 Successfully loaded {record_count} records to {target_schema}.{table_name}")

    except Exception as e:
        logger.error(
            f"❌ Error writing parquet {table_name} to Postgres: {e}", exc_info=True)

# -------------------------------------------------------
# Main processing
# -------------------------------------------------------


def main():
    bucket, prefix = BRONZE_BUCKET.split("/", 1)
    file_keys = get_file_list(bucket, prefix, FILES)

    for file_key in file_keys:
        try:
            file_name = os.path.basename(file_key).replace(".xlsx", "")
            file_name = to_kebab_case(file_name)
            logger.info(
                f"\n📥 Processing file: {file_name} (s3://{bucket}/{file_key})")

            obj = S3.get_object(Bucket=bucket, Key=file_key)
            excel_bytes = obj["Body"].read()
            xls = pd.ExcelFile(BytesIO(excel_bytes))
            sheet_names = xls.sheet_names
            # s3_path = f"s3a:/{bucket}/{file_key}"
            # s3_path = s3_path.replace("/", "//")
            # sheet_names = get_sheet_names_from_s3(bucket, file_key)
            logger.info(f"📑 Found sheets: {sheet_names}")

            if file_name in ("final-fiscal-sales", "final-event-sales", "final-attendance"):
                transform_all_fiscal_sheets_to_single_parquet(
                    file_name, excel_bytes, sheet_names)
            elif file_name in ("final-membership"):
                transform_fiscal_sheets_by_category_to_parquet(
                    file_name, excel_bytes, sheet_names)
            else:
                transform_each_sheet_to_parquet(
                    file_name, excel_bytes, sheet_names)

        except Exception as e:
            logger.error(
                f"❌ Fatal error processing file {file_key}: {e}", exc_info=True)
            continue
    job.commit()


if __name__ == "__main__":
    main()
    # tables = file_names = [f.replace(".xlsx", "") for f in FILES.split(",")]
    # for t in tables:
    #     continue
    # read_and_load_parquet_to_postgres(
    #     spark,
    #     SILVER_BUCKET,
    #     t,
    #     JDBC_URL,
    #     "dw",
    #     DB_USER,
    #     DB_PASSWORD,
    #     mode="overwrite"
    # )
    # jdbc_update_data_checkpoint(
    #     PIPELINE_STAGE[2], ""f"galaxy/{to_kebab_case(t)}/", DE_USER)


################# NEED TO DO ####################

# Predefine: data config file
# Update script: for this data control structure
# Update workflow

##################################################
