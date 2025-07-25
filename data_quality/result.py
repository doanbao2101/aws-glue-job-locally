import sys
import logging
import re
import boto3
from typing import List, Dict, Any
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count, when
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from concurrent.futures import ThreadPoolExecutor
import argparse
import time
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, LongType
from functools import reduce
from pyspark.sql import DataFrame

"""
Glue Job: Data Quality Checker from Bronze (S3) to RDS/Postgres
- Reads data from Bronze layer in S3 and performs data quality checks
- Saves quality metrics to RDS/Postgres in data_quality schema
- Uses Glue connection and from_options for RDS/Postgres access
- Supports batch processing and parallel execution
"""

# Required Glue Job Parameters:
#   JOB_NAME
#   S3_BRONZE_BUCKET
#   S3_BRONZE_PREFIX
#   DATABASE_NAME (for Glue Catalog)
#   JDBC_URL
#   DB_USERNAME
#   DB_PASSWORD
#   DB_NAME
#   TEMP_S3_DIR
#   S3_SILVER_BUCKET (for Silver layer)
#   S3_SILVER_PREFIX (for Silver layer)
# =====================

# JDBC Connection Configuration
JDBC_BATCH_SIZE = 1000
JDBC_ISOLATION_LEVEL = "READ_COMMITTED"
JDBC_TIMEOUT = 300  # seconds
JDBC_FETCH_SIZE = 1000

# Data Quality Schema Configuration
DATA_QUALITY_SCHEMA = "data_quality"  # Target schema in RDS PostgreSQL

# Category keywords for table classification
category_keywords = {
    'Finance & Accounting': [
        'finance', 'accounting', 'revenue', 'payment', 'billing', 'invoice',
        'account', 'ledger', 'transaction', 'txn', 'trans', 'charge', 'fee',
        'cost', 'expense', 'budget', 'financial', 'money', 'cash', 'credit',
        'debit', 'balance', 'collection', 'phic', 'insurance', 'fin'
    ],
    'Patient Management': [
        'patient', 'pat', 'pt', 'admission', 'discharge', 'register',
        'demographic', 'contact', 'address', 'guardian', 'emergency', 'visitor'
    ],
    'User & Authentication': [
        'user', 'login', 'password', 'auth', 'permission', 'role', 'access',
        'security', 'credential', 'session', 'token'
    ],
    'Medical/Clinical': [
        'medical', 'clinical', 'diagnosis', 'treatment', 'procedure', 'service',
        'icd', 'cpt', 'doctor', 'physician', 'nurse', 'practitioner', 'specialty'
    ],
    'Inventory & Supplies': [
        'inventory', 'inv', 'stock', 'item', 'supply', 'warehouse', 'storage',
        'product', 'material', 'equipment', 'asset', 'requisition'
    ],
    'Reports & Analytics': [
        'report', 'analytics', 'summary', 'statistics', 'metric', 'kpi',
        'dashboard', 'chart', 'graph', 'analysis'
    ],
    'System Configuration': [
        'config', 'setting', 'parameter', 'system', 'admin', 'setup',
        'preference', 'option', 'application', 'app'
    ],
    'Pharmacy': [
        'pharmacy', 'drug', 'medicine', 'medication', 'prescription', 'dose',
        'pharma', 'rx', 'pharmaceutical'
    ],
    'Human Resources': [
        'employee', 'emp', 'staff', 'personnel', 'hr', 'payroll', 'department',
        'position', 'job', 'work', 'schedule', 'shift'
    ],
    'Laboratory': [
        'lab', 'laboratory', 'test', 'specimen', 'sample', 'result',
        'pathology', 'microbiology', 'chemistry', 'hematology'
    ],
    'Radiology': [
        'radiology', 'imaging', 'xray', 'rad', 'ct', 'mri', 'ultrasound',
        'scan', 'radiological', 'dicom'
    ],
    'Scheduling & Appointments': [
        'schedule', 'appointment', 'appt', 'booking', 'calendar', 'cal',
        'slot', 'availability', 'queue', 'waiting'
    ],
    'Billing & Insurance': [
        'bill', 'invoice', 'claim', 'insurance', 'coverage', 'copay',
        'deductible', 'reimbursement', 'adjudication'
    ],
    'Asset Management': [
        'asset', 'equipment', 'maintenance', 'calibration', 'service',
        'warranty', 'vendor', 'contract'
    ],
    'Audit & Logging': [
        'audit', 'log', 'history', 'tracking', 'trail', 'event',
        'activity', 'monitor', 'compliance'
    ],
    'Master Data': [
        'master', 'lookup', 'reference', 'code', 'type', 'category',
        'classification', 'taxonomy', 'standard'
    ]
}


def get_table_category(table_name: str) -> str:
    """Determine the category of a table based on its name using category_keywords."""
    name = table_name.lower()
    for category, keywords in category_keywords.items():
        for kw in keywords:
            if kw in name:
                return category
    return 'Other'


def safe_column_name(column_name: str) -> str:
    """Safely handle column names with spaces and special characters"""
    if not column_name:
        return ""
    # Remove any existing backticks and add them back
    clean_name = column_name.strip().replace("`", "")
    return f"`{clean_name}`"


def log_column_info(table_name: str, columns: List[str]):
    """Log column information for debugging"""
    if columns:
        logging.info(f"[{table_name}]→ Found {len(columns)} columns")
        # Log first few columns as examples
        sample_cols = columns[:5]
        logging.info(f"[{table_name}]→ Sample columns: {sample_cols}")
        if len(columns) > 5:
            logging.info(
                f"[{table_name}]→ ... and {len(columns) - 5} more columns")

        # Check for problematic column names
        problematic_cols = [col for col in columns if any(char in col for char in [
                                                          ' ', '.', '/', '-', '(', ')', '[', ']', '{', '}', '&', '|', '!', '@', '#', '$', '%', '^', '*', '+', '='])]
        if problematic_cols:
            logging.warning(
                f"[{table_name}]→ Found {len(problematic_cols)} columns with special characters: {problematic_cols}")
    else:
        logging.warning(f"[{table_name}]→ No columns found in DataFrame")


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "S3_BRONZE_BUCKET",
    "S3_BRONZE_PREFIX",
    "DATABASE_NAME",
    "JDBC_URL",
    "DB_USERNAME",
    "DB_PASSWORD",
    "DB_NAME",
    "TEMP_S3_DIR",
    # Silver layer params (optional)
    "S3_SILVER_BUCKET",
    "S3_SILVER_PREFIX"
])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
S3_CLIENT = boto3.client('s3')

S3_BRONZE_BUCKET = args["S3_BRONZE_BUCKET"]
S3_BRONZE_PREFIX = args["S3_BRONZE_PREFIX"].rstrip("/")
S3_BRONZE_PATH = f"s3://{S3_BRONZE_BUCKET}/{S3_BRONZE_PREFIX}"
DATABASE_NAME = args["DATABASE_NAME"]
JDBC_URL = args["JDBC_URL"]
DB_USERNAME = args["DB_USERNAME"]
DB_PASSWORD = args["DB_PASSWORD"]
DB_NAME = args["DB_NAME"]
TEMP_S3_DIR = args["TEMP_S3_DIR"]

# Optional parameters
TABLE_PREFIX = ""
BATCH_SIZE = 0
BATCH_NUMBER = 0
CREATE_EMPTY_TABLES = True  # Default: create empty tables instead of skipping
for i, arg in enumerate(sys.argv):
    if arg == "--TABLE_PREFIX" and i + 1 < len(sys.argv):
        TABLE_PREFIX = sys.argv[i + 1]
    elif arg == "--BATCH_SIZE" and i + 1 < len(sys.argv):
        try:
            BATCH_SIZE = int(sys.argv[i + 1])
        except ValueError:
            logging.warning("Invalid BATCH_SIZE value, using 0")
    elif arg == "--BATCH_NUMBER" and i + 1 < len(sys.argv):
        try:
            BATCH_NUMBER = int(sys.argv[i + 1])
        except ValueError:
            logging.warning("Invalid BATCH_NUMBER value, using 0")
    elif arg == "--SKIP_EMPTY_TABLES":
        CREATE_EMPTY_TABLES = False
        logging.info("Empty tables will be skipped (not created in RDS)")
    elif arg == "--DATA_QUALITY_SCHEMA" and i + 1 < len(sys.argv):
        DATA_QUALITY_SCHEMA = sys.argv[i + 1]
        logging.info(
            f"Using custom data quality schema: {DATA_QUALITY_SCHEMA}")

USE_BATCHING = BATCH_SIZE > 0
MAX_THREADS = 10
processed_tables = []

# Add Silver and Gold layer config
S3_SILVER_BUCKET = args.get("S3_SILVER_BUCKET", None)
S3_SILVER_PREFIX = args.get("S3_SILVER_PREFIX", None)
GOLD_SCHEMA = 'new_raw_data'  # For Gold layer (RDS schema)


def validate_jdbc_connection():
    """Validate JDBC connection parameters before processing tables"""
    try:
        logging.info(f"Validating JDBC connection parameters...")

        # Validate JDBC URL format
        if not JDBC_URL or not JDBC_URL.strip():
            logging.error("❌ JDBC_URL is required")
            return False

        if not JDBC_URL.startswith("jdbc:postgresql://"):
            logging.error("❌ JDBC_URL must start with 'jdbc:postgresql://'")
            return False

        # Validate credentials
        if not DB_USERNAME or not DB_USERNAME.strip():
            logging.error("❌ DB_USERNAME is required")
            return False

        if not DB_PASSWORD or not DB_PASSWORD.strip():
            logging.error("❌ DB_PASSWORD is required")
            return False

        # Validate database name
        if not DB_NAME or not DB_NAME.strip():
            logging.error("❌ DB_NAME is required")
            return False

        logging.info("✅ JDBC connection validation successful")
        logging.info(f"   URL: {JDBC_URL}")
        logging.info(f"   Username: {DB_USERNAME}")
        logging.info(f"   Database: {DB_NAME}")
        return True

    except Exception as e:
        logging.error(f"❌ JDBC connection validation failed: {e}")
        logging.error("Please check:")
        logging.error(
            "1. JDBC_URL is properly formatted (jdbc:postgresql://host:port/database)")
        logging.error("2. DB_USERNAME and DB_PASSWORD are provided")
        logging.error("3. DB_NAME is specified")
        logging.error("4. Network connectivity to RDS instance")
        return False


def ensure_data_quality_schema_exists():
    """Ensure the data quality schema exists in RDS PostgreSQL"""
    try:
        logging.info(
            f"Ensuring data quality schema '{DATA_QUALITY_SCHEMA}' exists in RDS PostgreSQL...")

        # Create a dummy DataFrame to execute schema creation SQL
        dummy_df = spark.createDataFrame([("dummy",)], ["dummy_col"])
        dummy_dyf = DynamicFrame.fromDF(dummy_df, glueContext, "dummy_dyf")

        # Create schema if it doesn't exist
        create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {DATA_QUALITY_SCHEMA};"

        glueContext.write_dynamic_frame.from_options(
            frame=dummy_dyf.toDF().limit(0),  # Convert to DataFrame and limit to 0 rows
            connection_type="jdbc",
            connection_options={
                "url": JDBC_URL,
                "user": DB_USERNAME,
                "password": DB_PASSWORD,
                "driver": "org.postgresql.Driver",
                "dbtable": f"{DATA_QUALITY_SCHEMA}.dummy_table",
                "batchsize": "1",
                "preactions": create_schema_sql
            }
        )

        logging.info(
            f"✅ Data quality schema '{DATA_QUALITY_SCHEMA}' is ready for use")
        return True

    except Exception as e:
        logging.error(
            f"❌ Failed to create data quality schema '{DATA_QUALITY_SCHEMA}': {e}")
        logging.error(
            "Please ensure the database user has CREATE SCHEMA permissions")
        return False


def get_quality_table_name(layer: str) -> str:
    """Return the data quality results table name for the given layer."""
    return f"data_quality_results_{layer}"


def create_data_quality_table(layer: str):
    """Create the data quality results table for the given layer in RDS PostgreSQL"""
    try:
        table_name = get_quality_table_name(layer)
        logging.info(
            f"Creating data quality results table '{table_name}' in schema '{DATA_QUALITY_SCHEMA}'...")
        # Define the schema for data quality results using StructType
        quality_schema = StructType([
            StructField("table_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("ingestion_timestamp", StringType(), True),
            StructField("check_timestamp", StringType(), True),
            StructField("processing_time_seconds", DoubleType(), True),
            StructField("is_empty", BooleanType(), True),
            StructField("total_null_record", LongType(), True),
            StructField("percentage_of_null_values", DoubleType(), True),
            StructField("total_duplicated_records", LongType(), True),
            StructField("number_of_records", LongType(), True),
            StructField("table_size_mb", DoubleType(), True),
            StructField("completeness_score", DoubleType(), True),
            StructField("uniqueness_score", DoubleType(), True),
            StructField("data_score", DoubleType(), True),
            StructField("processing_status", StringType(), True),
            StructField("error_message", StringType(), True),
            StructField("total_cell_nulls", LongType(), True)
        ])
        # Create empty DataFrame with the schema
        empty_df = spark.createDataFrame([], quality_schema)
        empty_dyf = DynamicFrame.fromDF(empty_df, glueContext, "empty_dyf")
        # Create the table
        glueContext.write_dynamic_frame.from_options(
            frame=empty_dyf,
            connection_type="jdbc",
            connection_options={
                "url": JDBC_URL,
                "user": DB_USERNAME,
                "password": DB_PASSWORD,
                "dbtable": f"{DATA_QUALITY_SCHEMA}.{table_name}",
                "driver": "org.postgresql.Driver",
                "batchsize": "1",
                "preactions": f"DROP TABLE IF EXISTS {DATA_QUALITY_SCHEMA}.{table_name};"
            }
        )
        logging.info(
            f"✅ Data quality results table created: {DATA_QUALITY_SCHEMA}.{table_name}")
        return True
    except Exception as e:
        logging.error(f"❌ Failed to create data quality results table: {e}")
        return False


def perform_data_quality_check(df, table_name: str, processing_time_seconds: float = 0.0) -> Dict[str, Any]:
    """Perform comprehensive data quality checks on a DataFrame"""
    try:
        logging.info(f"[{table_name}]→ Performing data quality checks...")

        # Number of records
        num_records = df.count()
        logging.info(f"[{table_name}]→ Total records: {num_records}")

        # Check if table is empty
        is_empty = num_records == 0
        logging.info(f"[{table_name}]→ Is empty: {is_empty}")

        # Calculate % of nulls across all columns - FIXED: Properly handle column names with spaces/special chars
        if df.columns:
            # Create safe column references for null counting
            null_expressions = []
            for c in df.columns:
                # Wrap column name in backticks to handle spaces and special characters
                safe_col_name = safe_column_name(c)
                null_expressions.append(
                    count(when(col(safe_col_name).isNull(), safe_col_name)).alias(c))

            null_counts = df.select(null_expressions).collect()[0].asDict()
            total_nulls = sum(null_counts.values())
            total_cells = num_records * len(df.columns)
            null_percentage = round(
                (total_nulls / total_cells) * 100, 2) if total_cells else 0.0
        else:
            null_percentage = 0.0
            total_nulls = 0
            total_cells = 0
        logging.info(f"[{table_name}]→ Null percentage: {null_percentage}%")

        # Count total null records (rows where all columns are null)
        if df.columns:
            # Always wrap column names in backticks to handle spaces, dots, and special characters
            null_conditions = [col(safe_column_name(c)).isNull()
                               for c in df.columns]
            total_null_record = df.filter(
                reduce(lambda x, y: x & y, null_conditions)).count()
        else:
            total_null_record = 0
        logging.info(
            f"[{table_name}]→ Total null records (all columns null): {total_null_record}")

        # Count duplicate rows - FIXED: Properly handle column names with spaces/special chars
        if df.columns:
            # Create safe column references for groupBy
            safe_columns = [col(safe_column_name(c)) for c in df.columns]
            dup_count_df = df.groupBy(safe_columns).count().filter(
                "count > 1").selectExpr("sum(count - 1) as dup").collect()
            total_duplicates = int(
                dup_count_df[0]['dup']) if dup_count_df[0]['dup'] is not None else 0
        else:
            total_duplicates = 0
        logging.info(f"[{table_name}]→ Total duplicates: {total_duplicates}")

        # Estimate table size in MB (based on in-memory size)
        table_size_bytes = df.rdd.map(lambda row: len(str(row))).sum()
        table_size_mb = round(table_size_bytes / (1024 * 1024), 2)
        logging.info(f"[{table_name}]→ Table size: {table_size_mb} MB")

        # Completeness Score
        completeness_score = round(1 - (null_percentage / 100), 4)

        # Uniqueness Score
        uniqueness_score = round(
            1 - (total_duplicates / num_records), 4) if num_records > 0 else 1.0

        # Data Score
        data_score = round(
            (uniqueness_score * 0.5 + completeness_score * 0.5) * 100, 2)

        quality_metrics = {
            "table_name": table_name,
            "category": get_table_category(table_name),
            "processing_time_seconds": round(processing_time_seconds, 2),
            "is_empty": is_empty,
            "total_null_record": total_null_record,
            "percentage_of_null_values": null_percentage,
            "total_duplicated_records": total_duplicates,
            "number_of_records": num_records,
            "table_size_mb": table_size_mb,
            "completeness_score": completeness_score,
            "uniqueness_score": uniqueness_score,
            "data_score": data_score,
            "total_cell_nulls": total_nulls
        }

        logging.info(
            f"[{table_name}]→ Data quality scores - Completeness: {completeness_score}, Uniqueness: {uniqueness_score}, Overall: {data_score}")
        logging.info(
            f"[{table_name}]→ Processing time: {processing_time_seconds:.2f} seconds")
        return quality_metrics

    except Exception as e:
        logging.error(f"[{table_name}] ❌ Error during data quality check: {e}")
        raise


def get_table_list(database_name: str) -> List[str]:
    """Get list of tables from Glue Catalog (limit to 10 tables)"""
    try:
        glue_client = boto3.client('glue')
        table_list = []
        paginator = glue_client.get_paginator('get_tables')

        for page in paginator.paginate(DatabaseName=database_name):
            for table in page['TableList']:
                # Filter out null or empty table names
                if table['Name'] and table['Name'].strip():
                    table_list.append(table['Name'].strip())

                    # Stop once 10 tables are found
                    if len(table_list) >= 2:
                        break
            if len(table_list) >= 2:
                break

        logging.info(
            f"Found {len(table_list)} valid tables in Glue Catalog: {table_list}")
        return table_list

    except Exception as e:
        logging.error(f"Failed to retrieve tables from Glue Catalog: {e}")
        return []


def get_gold_table_list(jdbc_url: str, db_user: str, db_pass: str, schema: str = 'raw_data') -> list:
    """Get list of tables from RDS/Postgres raw_data schema (Gold layer)"""
    try:
        query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}' AND table_type = 'BASE TABLE'"
        df = spark.read.format("jdbc").option("url", jdbc_url).option("user", db_user).option(
            "password", db_pass).option("dbtable", f"({query}) as t").option("driver", "org.postgresql.Driver").load()
        # table_list = [row['table_name'] for row in df.collect()]
        # Get the first 10 tables
        table_list = [row['table_name'] for row in df.collect()][:2]
        logging.info(
            f"Found {len(table_list)} tables in Gold schema '{schema}': {table_list}")
        return table_list
    except Exception as e:
        logging.error(
            f"Failed to retrieve tables from Gold schema '{schema}': {e}")
        return []


def get_latest_ingestion_ts(s3_bucket: str, table_name: str) -> str:
    """Get the latest ingestion timestamp for a table from S3"""
    table_name_clean = table_name.replace(TABLE_PREFIX, "")
    table_prefix = f"{S3_BRONZE_PREFIX}/{table_name_clean}/"
    paginator = S3_CLIENT.get_paginator('list_objects_v2')
    result = paginator.paginate(
        Bucket=s3_bucket, Prefix=table_prefix, Delimiter='/')
    partitions = []
    for page in result:
        for prefix in page.get('CommonPrefixes', []):
            m = re.search(r'ingestion_ts=(.+)/$', prefix['Prefix'])
            if m:
                partitions.append(m.group(1))
    if partitions:
        return sorted(partitions)[-1]
    return None


def get_quality_schema():
    return StructType([
        StructField("table_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("ingestion_timestamp", StringType(), True),
        StructField("check_timestamp", StringType(), True),
        StructField("processing_time_seconds", DoubleType(), True),
        StructField("is_empty", BooleanType(), True),
        StructField("total_null_record", LongType(), True),
        StructField("percentage_of_null_values", DoubleType(), True),
        StructField("total_duplicated_records", LongType(), True),
        StructField("number_of_records", LongType(), True),
        StructField("table_size_mb", DoubleType(), True),
        StructField("completeness_score", DoubleType(), True),
        StructField("uniqueness_score", DoubleType(), True),
        StructField("data_score", DoubleType(), True),
        StructField("processing_status", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("total_cell_nulls", LongType(), True)
    ])


def sanitize_quality_metrics(metrics: dict) -> dict:
    # Ensure no None values and correct types for all fields
    return {
        "table_name": str(metrics.get("table_name", "")),
        "category": str(metrics.get("category", "")),
        "ingestion_timestamp": str(metrics.get("ingestion_timestamp", "")),
        "check_timestamp": str(metrics.get("check_timestamp", "")),
        "processing_time_seconds": float(metrics.get("processing_time_seconds", 0.0)),
        "is_empty": bool(metrics.get("is_empty", False)),
        "total_null_record": int(metrics.get("total_null_record", 0)),
        "percentage_of_null_values": float(metrics.get("percentage_of_null_values", 0.0)),
        "total_duplicated_records": int(metrics.get("total_duplicated_records", 0)),
        "number_of_records": int(metrics.get("number_of_records", 0)),
        "table_size_mb": float(metrics.get("table_size_mb", 0.0)),
        "completeness_score": float(metrics.get("completeness_score", 0.0)),
        "uniqueness_score": float(metrics.get("uniqueness_score", 0.0)),
        "data_score": float(metrics.get("data_score", 0.0)),
        "processing_status": str(metrics.get("processing_status", "")),
        "error_message": str(metrics.get("error_message", "")),
        "total_cell_nulls": int(metrics.get("total_cell_nulls", 0)),
    }


def save_quality_results(quality_metrics: Dict[str, Any], ingestion_ts: str, processing_status: str = "SUCCESS", error_message: str = "", layer: str = None):
    """Save data quality results to the correct RDS table for the layer (no layer_prefix column)"""
    try:
        # Add metadata
        quality_metrics["ingestion_timestamp"] = ingestion_ts
        quality_metrics["check_timestamp"] = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S")
        quality_metrics["processing_status"] = processing_status
        quality_metrics["error_message"] = error_message
        # Sanitize metrics to ensure no None values and correct types
        sanitized_metrics = sanitize_quality_metrics(quality_metrics)
        table_name = get_quality_table_name(layer or "unknown")
        # Create DataFrame from quality metrics with explicit schema
        quality_df = spark.createDataFrame(
            [sanitized_metrics], schema=get_quality_schema())
        quality_dyf = DynamicFrame.fromDF(
            quality_df, glueContext, "quality_dyf")
        # Save to RDS
        glueContext.write_dynamic_frame.from_options(
            frame=quality_dyf,
            connection_type="jdbc",
            connection_options={
                "url": JDBC_URL,
                "user": DB_USERNAME,
                "password": DB_PASSWORD,
                "dbtable": f"{DATA_QUALITY_SCHEMA}.{table_name}",
                "driver": "org.postgresql.Driver",
                "batchsize": str(JDBC_BATCH_SIZE)
            }
        )
        logging.info(
            f"[{sanitized_metrics['table_name']}]✅ Quality results saved to RDS table {DATA_QUALITY_SCHEMA}.{table_name}")
    except Exception as e:
        logging.error(
            f"[{quality_metrics.get('table_name', 'Unknown')}] ❌ Failed to save quality results: {e}")


def save_quality_results_to_s3(quality_metrics: Dict[str, Any], ingestion_ts: str, processing_status: str = "SUCCESS", error_message: str = "", layer: str = None):
    """Save data quality results to S3 in Parquet format"""

    try:
        # Add metadata
        quality_metrics["ingestion_timestamp"] = ingestion_ts
        quality_metrics["check_timestamp"] = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S")
        quality_metrics["processing_status"] = processing_status
        quality_metrics["error_message"] = error_message
        # Sanitize metrics to ensure no None values and correct types
        sanitized_metrics = sanitize_quality_metrics(quality_metrics)
        table_name = get_quality_table_name(layer or "unknown")
        # Create DataFrame from quality metrics with explicit schema
        quality_df = spark.createDataFrame(
            [sanitized_metrics], schema=get_quality_schema())
        s3_output_path = f"s3://mghi-dev-data-quality-bucket-us-west-2-154983253388/{table_name}"

        # Save to S3 in Parquet format if the S3 output path is provided
        if s3_output_path:
            quality_df.write.parquet(s3_output_path, mode='overwrite')
            logging.info(
                f"[{quality_metrics['table_name']}] ✅ Quality results saved to S3 at {s3_output_path}")

    except Exception as e:
        logging.error(
            f"[{quality_metrics.get('table_name', 'Unknown')}] ❌ Failed to save quality results: {e}")


def read_table_data(table_name: str, layer: str, ingestion_ts: str = None, rds_schema: str = None) -> DataFrame:
    """Read table data from the specified layer (bronze, silver, gold)"""
    if layer == 'bronze':
        s3_path = f"s3://{S3_BRONZE_BUCKET}/{S3_BRONZE_PREFIX}/{table_name}/ingestion_ts={ingestion_ts}/" if ingestion_ts else f"s3://{S3_BRONZE_BUCKET}/{S3_BRONZE_PREFIX}/{table_name}/"
        logging.info(f"[BRONZE] Reading data from {s3_path}")
        return spark.read.parquet(s3_path)
    elif layer == 'silver':
        if not S3_SILVER_BUCKET or not S3_SILVER_PREFIX:
            raise ValueError(
                "S3_SILVER_BUCKET and S3_SILVER_PREFIX must be set for Silver layer.")
        s3_path = f"s3://{S3_SILVER_BUCKET}/{S3_SILVER_PREFIX}/{table_name}/ingestion_ts={ingestion_ts}/" if ingestion_ts else f"s3://{S3_SILVER_BUCKET}/{S3_SILVER_PREFIX}/{table_name}/"
        logging.info(f"[SILVER] Reading data from {s3_path}")
        return spark.read.parquet(s3_path)
    elif layer == 'gold':
        # Read from RDS/Postgres raw_data schema
        dbtable = f'{rds_schema}."{table_name}"'
        logging.info(f"[GOLD] Reading data from RDS table {dbtable}")
        return spark.read.format("jdbc").option("url", JDBC_URL).option("user", DB_USERNAME).option("password", DB_PASSWORD).option("dbtable", dbtable).option("driver", "org.postgresql.Driver").load()
    else:
        raise ValueError(f"Unknown layer: {layer}")


def process_table_quality(table_name: str, ingestion_ts: str, layer: str, rds_schema: str):
    """Process data quality checks for a single table and layer"""
    max_retries = 3
    retry_count = 0

    while retry_count < max_retries:
        try:
            start_time = time.time()
            table_name_clean = table_name.replace(TABLE_PREFIX, "")

            df = read_table_data(table_name_clean, layer,
                                 ingestion_ts, rds_schema)
            row_count = df.count()
            logging.info(
                f"[{table_name}]→ Read {row_count} rows from {layer} layer")

            # Log column information for debugging
            log_column_info(table_name, df.columns)

            if row_count == 0:
                logging.warning(
                    f"[{table_name}]→ No data to process, skipping table")
                processing_time = time.time() - start_time
                # Save empty quality results
                empty_metrics = {
                    "table_name": table_name_clean,
                    "category": get_table_category(table_name_clean),
                    "processing_time_seconds": 0.0,
                    "is_empty": True,
                    "total_null_record": 0,
                    "percentage_of_null_values": 0.0,
                    "total_duplicated_records": 0,
                    "number_of_records": 0,
                    "table_size_mb": 0.0,
                    "completeness_score": 1.0,
                    "uniqueness_score": 1.0,
                    "data_score": 100.0,
                    "total_cell_nulls": 0
                }
                save_quality_results_to_s3(
                    empty_metrics, ingestion_ts, "EMPTY_TABLE", "No data found in layer", layer)
                processed_tables.append(table_name)
                break

            # Perform data quality checks
            processing_time = time.time() - start_time
            quality_metrics = perform_data_quality_check(
                df, table_name_clean, processing_time)

            # Save quality results to RDS
            save_quality_results_to_s3(
                quality_metrics, ingestion_ts, "SUCCESS", "", layer)

            processed_tables.append(table_name)
            logging.info(
                f"[{table_name}]✅ Data quality check completed successfully")
            break  # Success, exit retry loop

        except Exception as e:
            retry_count += 1
            logging.error(
                f"[{table_name}] ❌ Attempt {retry_count}/{max_retries} failed: {e}")

            if retry_count < max_retries:
                logging.info(f"[{table_name}]→ Retrying in 5 seconds...")
                time.sleep(5)
            else:
                logging.error(
                    f"[{table_name}] ❌ All {max_retries} attempts failed. Table processing failed.")
                processing_time = time.time() - start_time if 'start_time' in locals() else 0.0
                # Save error results
                error_metrics = {
                    "table_name": table_name.replace(TABLE_PREFIX, ""),
                    "category": get_table_category(table_name.replace(TABLE_PREFIX, "")),
                    "processing_time_seconds": round(processing_time, 2),
                    "is_empty": False,
                    "total_null_record": 0,
                    "percentage_of_null_values": 0.0,
                    "total_duplicated_records": 0,
                    "number_of_records": 0,
                    "table_size_mb": 0.0,
                    "completeness_score": 0.0,
                    "uniqueness_score": 0.0,
                    "data_score": 0.0,
                    "total_cell_nulls": 0
                }
                save_quality_results_to_s3(
                    error_metrics, ingestion_ts, "ERROR", str(e), layer)
                processed_tables.append(table_name)


def main():
    parser = argparse.ArgumentParser(
        description='Perform data quality checks on tables from S3 BRONZE layer and save results to RDS Postgres')
    parser.add_argument('--ingestion-timestamp', required=False,
                        help='Ingestion timestamp to process (format: YYYY-MM-DD_HH-MM-SS). If not provided, use latest available for each table.')
    parser.add_argument('--layer', required=False, default='bronze', choices=[
                        'bronze', 'silver', 'gold', 'all'], help='Data layer to process: bronze (default), silver, gold, or all')
    parser.add_argument('--rds-schema', required=False, default='raw_data',
                        help='RDS schema to use for gold layer (default: raw_data)')
    args_cli, unknown = parser.parse_known_args()
    layer = args_cli.layer
    rds_schema = args_cli.rds_schema

    # Validate JDBC connection first
    if not validate_jdbc_connection():
        logging.error("JDBC connection validation failed. Exiting.")
        sys.exit(1)

    # Ensure data quality schema exists
    # if not ensure_data_quality_schema_exists():
    #     logging.error("Data quality schema creation failed. Exiting.")
    #     sys.exit(1)

    logging.info(f"Getting tables from Glue Catalog: {DATABASE_NAME}")
    logging.info(f"Using JDBC URL: {JDBC_URL}")
    logging.info(f"Target database: {DB_NAME}")
    logging.info(f"Data quality schema: {DATA_QUALITY_SCHEMA}")

    def process_layer(selected_layer, rds_schema):
        # Create the data quality results table for this layer
        # if not create_data_quality_table(selected_layer):
        #     logging.error(
        #         f"Data quality table creation failed for layer {selected_layer}. Exiting.")
        #     sys.exit(1)

        if selected_layer == 'bronze' or selected_layer == 'silver':
            tables = get_table_list(DATABASE_NAME)
        elif selected_layer == 'gold':
            tables = get_gold_table_list(
                JDBC_URL, DB_USERNAME, DB_PASSWORD, rds_schema)
        else:
            logging.error(f"Unknown layer: {selected_layer}")
            return

        if not tables:
            logging.error("No tables found for the selected layer. Exiting.")
            return
        valid_tables = [table for table in tables if table and table.strip()]
        if len(valid_tables) != len(tables):
            logging.warning(
                f"Filtered out {len(tables) - len(valid_tables)} invalid table names")
        logging.info(f"Found {len(valid_tables)} valid tables: {valid_tables}")
        if USE_BATCHING:
            start_idx = BATCH_NUMBER * BATCH_SIZE
            end_idx = start_idx + BATCH_SIZE
            batch_tables = valid_tables[start_idx:end_idx]
            logging.info(
                f"BATCH MODE: Processing batch {BATCH_NUMBER}: tables {start_idx} to {end_idx-1} (total: {len(batch_tables)} tables)")
            logging.info(f"Batch tables: {batch_tables}")
            if not batch_tables:
                logging.warning(f"No tables in batch {BATCH_NUMBER}")
                return
            tables_to_process = batch_tables
        else:
            logging.info(
                f"NORMAL MODE: Processing all {len(valid_tables)} tables")
            tables_to_process = valid_tables
        table_ingestion_map = {}
        tables_without_data = []
        for table in tables_to_process:
            table_name_clean = table.replace(TABLE_PREFIX, "")
            if selected_layer == 'bronze':
                bucket = S3_BRONZE_BUCKET
                if args_cli.ingestion_timestamp:
                    partition_prefix = f"{S3_BRONZE_PREFIX}/{table_name_clean}/ingestion_ts={args_cli.ingestion_timestamp}/"
                    response = S3_CLIENT.list_objects_v2(
                        Bucket=bucket, Prefix=partition_prefix, MaxKeys=1)
                    if 'Contents' in response and response['Contents']:
                        table_ingestion_map[table_name_clean] = args_cli.ingestion_timestamp
                    else:
                        if CREATE_EMPTY_TABLES:
                            logging.info(
                                f"Table {table_name_clean} does not have data for ingestion_ts={args_cli.ingestion_timestamp}, will create empty quality record.")
                            tables_without_data.append(table)
                        else:
                            logging.warning(
                                f"Table {table_name_clean} does not have data for ingestion_ts={args_cli.ingestion_timestamp}, skipping.")
                else:
                    latest_ts = get_latest_ingestion_ts(
                        bucket, table_name_clean)
                    if latest_ts:
                        table_ingestion_map[table_name_clean] = latest_ts
                    else:
                        if CREATE_EMPTY_TABLES:
                            logging.info(
                                f"Table {table_name_clean} has no ingestion_ts partitions in BRONZE, will create empty quality record.")
                            tables_without_data.append(table)
                        else:
                            logging.warning(
                                f"Table {table_name_clean} has no ingestion_ts partitions in BRONZE, skipping.")
            elif selected_layer == 'silver':
                # Silver is not partitioned by ingestion_ts; always add the table
                table_ingestion_map[table_name_clean] = None
            elif selected_layer == 'gold':
                # For gold, ingestion_ts is not used; just process the table
                table_ingestion_map[table_name_clean] = None

        # Process tables with data
        if table_ingestion_map:
            logging.info(f"Tables with data to process: {table_ingestion_map}")
            with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
                executor.map(lambda t: process_table_quality(
                    t[0], t[1], selected_layer if selected_layer != 'gold' else 'gold', rds_schema), table_ingestion_map.items())
        # Create empty quality records for tables without data
        if tables_without_data:
            logging.info(
                f"Creating empty quality records for: {tables_without_data}")
            for table in tables_without_data:
                table_name_clean = table.replace(TABLE_PREFIX, "")
                empty_metrics = {
                    "table_name": table_name_clean,
                    "category": get_table_category(table_name_clean),
                    "processing_time_seconds": 0.0,
                    "is_empty": True,
                    "total_null_record": 0,
                    "percentage_of_null_values": 0.0,
                    "total_duplicated_records": 0,
                    "number_of_records": 0,
                    "table_size_mb": 0.0,
                    "completeness_score": 1.0,
                    "uniqueness_score": 1.0,
                    "data_score": 100.0,
                    "total_cell_nulls": 0
                }
                save_quality_results_to_s3(
                    empty_metrics, "N/A", "NO_DATA", f"No data found in {selected_layer.upper()} layer", selected_layer)
                processed_tables.append(table)
        if not table_ingestion_map and not tables_without_data:
            logging.error("No tables to process. Exiting.")
            return
    # If layer is 'all', loop through all layers
    if layer == 'all':
        for lyr in ['bronze', 'silver', 'gold']:
            logging.info(f"\n===== Processing {lyr.upper()} Layer =====")
            process_layer(lyr, rds_schema)
    else:
        process_layer(layer, rds_schema)

    logging.info("✅ All data quality checks completed successfully!")
    logging.info(f"📊 Tables with data processed: {len(processed_tables)}")
    logging.info(f"📊 Total processed tables: {processed_tables}")

    # Print summary
    logging.info("=" * 80)
    logging.info("🎉 DATA QUALITY JOB SUMMARY")
    logging.info("=" * 80)
    logging.info(f"📊 Total tables processed: {len(processed_tables)}")
    # This line was changed to reflect the total processed tables
    logging.info(f"✅ Tables with data: {len(processed_tables)}")
    logging.info(f"🔗 JDBC URL: {JDBC_URL}")
    logging.info(f"🗄️  Target Database: {DB_NAME}")
    logging.info(f"📋 Data Quality Schema: {DATA_QUALITY_SCHEMA}")
    logging.info(f"📦 Source: S3 BRONZE Layer ({S3_BRONZE_PATH})")
    logging.info(
        f"⚙️  Batch Mode: {'Enabled' if USE_BATCHING else 'Disabled'}")
    if USE_BATCHING:
        logging.info(f"   Batch Number: {BATCH_NUMBER}")
        logging.info(f"   Batch Size: {BATCH_SIZE}")
    logging.info(f"🔄 Parallel Processing: {MAX_THREADS} threads")
    logging.info(f"⚡ JDBC Batch Size: {JDBC_BATCH_SIZE}")
    logging.info("=" * 80)
    job.commit()
    logging.info("🎉 Data Quality Glue Spark Job completed.")


if __name__ == "__main__":
    main()
