import sys
import logging
import re
import boto3
from typing import List, Dict, Any
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count, when, expr
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from concurrent.futures import ThreadPoolExecutor
import argparse
import time
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

"""
Glue Job: Data Quality Rules Checker from Bronze (S3) to RDS/Postgres
- Runs custom data quality rules on specific tables
- Saves rule validation results to RDS/Postgres in data_quality schema
- Supports table-specific rule configurations
- Uses Glue connection and from_options for RDS/Postgres access
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
#   RULES_CONFIG_S3_BUCKET (optional - for S3-based rules config)
#   RULES_CONFIG_S3_KEY (optional - for S3-based rules config)
#   RULES_CONFIG_S3_REGION (optional - for S3-based rules config)
# =====================

# JDBC Connection Configuration
JDBC_BATCH_SIZE = 1000
JDBC_ISOLATION_LEVEL = "READ_COMMITTED"
JDBC_TIMEOUT = 300  # seconds
JDBC_FETCH_SIZE = 1000

# Data Quality Schema Configuration
DATA_QUALITY_SCHEMA = "data_quality"  # Target schema in RDS PostgreSQL

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
    "TEMP_S3_DIR"
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
RULES_CONFIG_S3_BUCKET = ""
RULES_CONFIG_S3_KEY = ""
RULES_CONFIG_S3_REGION = ""


# Optional parameters
TABLE_PREFIX = ""
BATCH_SIZE = 0
BATCH_NUMBER = 0
RULES_CONFIG_FILE = ""
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
    elif arg == "--RULES_CONFIG_FILE" and i + 1 < len(sys.argv):
        RULES_CONFIG_FILE = sys.argv[i + 1]
        logging.info(f"Using rules configuration file: {RULES_CONFIG_FILE}")
    elif arg == "--RULES_CONFIG_S3_BUCKET" and i + 1 < len(sys.argv):
        RULES_CONFIG_S3_BUCKET = sys.argv[i + 1]
        logging.info(
            f"Using S3 bucket for rules config: {RULES_CONFIG_S3_BUCKET}")
    elif arg == "--RULES_CONFIG_S3_KEY" and i + 1 < len(sys.argv):
        RULES_CONFIG_S3_KEY = sys.argv[i + 1]
        logging.info(f"Using S3 key for rules config: {RULES_CONFIG_S3_KEY}")
    elif arg == "--RULES_CONFIG_S3_REGION" and i + 1 < len(sys.argv):
        RULES_CONFIG_S3_REGION = sys.argv[i + 1]
        logging.info(
            f"Using S3 region for rules config: {RULES_CONFIG_S3_REGION}")
    elif arg == "--DATA_QUALITY_SCHEMA" and i + 1 < len(sys.argv):
        DATA_QUALITY_SCHEMA = sys.argv[i + 1]
        logging.info(
            f"Using custom data quality schema: {DATA_QUALITY_SCHEMA}")

USE_BATCHING = BATCH_SIZE > 0
MAX_THREADS = 10
processed_rules = []

# Default rules configuration (can be overridden by config file)
DEFAULT_RULES_CONFIG = {
    "customer_table": [
        {
            "rule_name": "email_format_check",
            "rule_description": "Check if email column contains valid email format",
            "severity": "HIGH",
            "sql_condition": "email IS NOT NULL AND email NOT REGEXP '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'"
        },
        {
            "rule_name": "customer_id_not_null",
            "rule_description": "Check if customer_id is not null",
            "severity": "CRITICAL",
            "sql_condition": "customer_id IS NULL"
        }
    ],
    "order_table": [
        {
            "rule_name": "order_amount_positive",
            "rule_description": "Check if order amount is positive",
            "severity": "MEDIUM",
            "sql_condition": "order_amount <= 0"
        },
        {
            "rule_name": "order_date_valid",
            "rule_description": "Check if order date is not in future",
            "severity": "HIGH",
            "sql_condition": "order_date > CURRENT_DATE"
        }
    ],
    "product_table": [
        {
            "rule_name": "product_price_positive",
            "rule_description": "Check if product price is positive",
            "severity": "MEDIUM",
            "sql_condition": "price <= 0"
        }
    ]
}


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


def validate_s3_rules_config():
    """Validate S3 rules configuration parameters"""
    try:
        logging.info(f"Validating S3 rules configuration parameters...")

        # Check if S3 configuration is provided
        if RULES_CONFIG_S3_BUCKET or RULES_CONFIG_S3_KEY:
            # Both bucket and key must be provided together
            if not RULES_CONFIG_S3_BUCKET or not RULES_CONFIG_S3_KEY:
                logging.error(
                    "❌ Both RULES_CONFIG_S3_BUCKET and RULES_CONFIG_S3_KEY must be provided together")
                return False

            # Validate bucket name format
            if not re.match(r'^[a-z0-9][a-z0-9.-]*[a-z0-9]$', RULES_CONFIG_S3_BUCKET):
                logging.error("❌ Invalid S3 bucket name format")
                return False

            # Validate key is not empty
            if not RULES_CONFIG_S3_KEY.strip():
                logging.error("❌ RULES_CONFIG_S3_KEY cannot be empty")
                return False

            # Validate region if provided
            if RULES_CONFIG_S3_REGION and not re.match(r'^[a-z0-9-]+$', RULES_CONFIG_S3_REGION):
                logging.error("❌ Invalid AWS region format")
                return False

            logging.info("✅ S3 rules configuration validation successful")
            logging.info(f"   Bucket: {RULES_CONFIG_S3_BUCKET}")
            logging.info(f"   Key: {RULES_CONFIG_S3_KEY}")
            if RULES_CONFIG_S3_REGION:
                logging.info(f"   Region: {RULES_CONFIG_S3_REGION}")
            return True
        else:
            logging.info(
                "✅ No S3 rules configuration provided, will use default or file-based config")
            return True

    except Exception as e:
        logging.error(f"❌ S3 rules configuration validation failed: {e}")
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


def create_data_quality_rules_table():
    """Create the data quality rules results table in RDS PostgreSQL"""
    try:
        logging.info(
            f"Creating data quality rules results table in schema '{DATA_QUALITY_SCHEMA}'...")

        # Use StructType for schema definition
        rules_schema = StructType([
            StructField("table_name", StringType(), True),
            StructField("rule_name", StringType(), True),
            StructField("rule_description", StringType(), True),
            StructField("severity", StringType(), True),
            StructField("validated_time", StringType(), True),
            StructField("ingestion_time", StringType(), True),
            StructField("rule_status", StringType(), True),
            StructField("failed_count", LongType(), True),
            StructField("total_count", LongType(), True),
            StructField("failure_rate", DoubleType(), True),
            StructField("validity_score", DoubleType(), True),
            StructField("error_message", StringType(), True)
        ])

        # Create empty DataFrame with the schema
        empty_df = spark.createDataFrame([], rules_schema)
        empty_dyf = DynamicFrame.fromDF(empty_df, glueContext, "empty_dyf")

        # Create the table by writing the empty DataFrame
        glueContext.write_dynamic_frame.from_options(
            frame=empty_dyf,
            connection_type="jdbc",
            connection_options={
                "url": JDBC_URL,
                "user": DB_USERNAME,
                "password": DB_PASSWORD,
                "dbtable": f"{DATA_QUALITY_SCHEMA}.data_quality_rules",
                "driver": "org.postgresql.Driver",
                "batchsize": "1",
                "preactions": f"DROP TABLE IF EXISTS {DATA_QUALITY_SCHEMA}.data_quality_rules;"
            }
        )

        logging.info(
            f"✅ Data quality rules table created: {DATA_QUALITY_SCHEMA}.data_quality_rules")
        return True

    except Exception as e:
        logging.error(f"❌ Failed to create data quality rules table: {e}")
        return False


def load_rules_configuration():
    """Load rules configuration from S3 or use default"""
    try:
        # Check if S3-based configuration is provided
        if RULES_CONFIG_S3_BUCKET and RULES_CONFIG_S3_KEY:
            logging.info(
                f"Loading rules configuration from S3: s3://{RULES_CONFIG_S3_BUCKET}/{RULES_CONFIG_S3_KEY}")

            # Use specified region if provided, otherwise use default
            s3_client = boto3.client(
                's3', region_name=RULES_CONFIG_S3_REGION) if RULES_CONFIG_S3_REGION else S3_CLIENT

            try:
                response = s3_client.get_object(
                    Bucket=RULES_CONFIG_S3_BUCKET, Key=RULES_CONFIG_S3_KEY)
                config_content = response['Body'].read().decode('utf-8')
                import json
                rules_array = json.loads(config_content)
                logging.info(
                    f"Successfully loaded {len(rules_array)} rules from S3 configuration")
            except Exception as s3_error:
                logging.error(
                    f"❌ Failed to load rules configuration from S3: {s3_error}")
                logging.info("Falling back to default configuration")
                return DEFAULT_RULES_CONFIG

        elif RULES_CONFIG_FILE:
            logging.info(
                f"Loading rules configuration from: {RULES_CONFIG_FILE}")
            # Read configuration from S3 or local file
            if RULES_CONFIG_FILE.startswith("s3://"):
                # Parse S3 path
                bucket = RULES_CONFIG_FILE.split("/")[2]
                key = "/".join(RULES_CONFIG_FILE.split("/")[3:])
                response = S3_CLIENT.get_object(Bucket=bucket, Key=key)
                config_content = response['Body'].read().decode('utf-8')
                import json
                rules_array = json.loads(config_content)
            else:
                # Local file
                import json
                with open(RULES_CONFIG_FILE, 'r') as f:
                    rules_array = json.load(f)
        else:
            # Use default bronze bucket configuration
            default_config_key = "configs/rules_config.json"
            logging.info(
                f"No rules configuration provided, trying default S3 location: s3://{S3_BRONZE_BUCKET}/{default_config_key}")

            try:
                response = S3_CLIENT.get_object(
                    Bucket=S3_BRONZE_BUCKET, Key=default_config_key)
                config_content = response['Body'].read().decode('utf-8')
                import json
                rules_array = json.loads(config_content)
                logging.info(
                    f"Successfully loaded {len(rules_array)} rules from default S3 configuration")
            except Exception as s3_error:
                logging.warning(
                    f"❌ Failed to load rules configuration from default S3 location: {s3_error}")
                logging.info("Falling back to default configuration")
                return DEFAULT_RULES_CONFIG

        # Convert array format to table-based format for processing
        table_rules = {}
        for rule_config in rules_array:
            rule_name = rule_config.get("rule", "Unknown Rule")
            rule_description = rule_config.get("rule_description", "")
            # Handle both "severity" and "serverity" (typo) for backward compatibility
            severity = rule_config.get(
                "severity", rule_config.get("serverity", "MEDIUM"))
            sql_conditions = rule_config.get("sql_condition", [])

            # Process each table condition
            for table_condition in sql_conditions:
                for table_name, condition in table_condition.items():
                    if table_name not in table_rules:
                        table_rules[table_name] = []

                    table_rules[table_name].append({
                        "rule_name": rule_name,
                        "rule_description": rule_description,
                        "severity": severity,
                        "sql_condition": condition
                    })

        logging.info(
            f"Converted {len(rules_array)} rules to {len(table_rules)} table configurations")
        return table_rules

    except Exception as e:
        logging.error(f"❌ Failed to load rules configuration: {e}")
        logging.info("Falling back to default configuration")
        return DEFAULT_RULES_CONFIG


def validate_rule(df, table_name: str, rule_config: Dict[str, Any], ingestion_ts: str) -> Dict[str, Any]:
    """Validate a single rule on a DataFrame"""
    try:
        rule_name = rule_config["rule_name"]
        rule_description = rule_config["rule_description"]
        severity = rule_config["severity"]
        sql_condition = rule_config["sql_condition"]

        logging.info(f"[{table_name}]→ Validating rule: {rule_name}")
        logging.info(f"[{table_name}]→ Rule description: {rule_description}")
        logging.info(f"[{table_name}]→ SQL condition: {sql_condition}")

        # Get total count
        total_count = df.count()

        if total_count == 0:
            # Table is empty, rule passes
            return {
                "table_name": table_name,
                "rule_name": rule_name,
                "rule_description": rule_description,
                "severity": severity,
                "validated_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "ingestion_time": ingestion_ts,
                "rule_status": "PASSED",
                "failed_count": 0,
                "total_count": 0,
                "failure_rate": 0.0,
                "validity_score": 100.0,
                "error_message": "Table is empty"
            }

        # Apply the rule condition
        try:
            failed_df = df.filter(sql_condition)
            failed_count = failed_df.count()
        except Exception as sql_error:
            logging.error(
                f"[{table_name}] ❌ SQL condition error for rule '{rule_name}': {sql_error}")
            return {
                "table_name": table_name,
                "rule_name": rule_name,
                "rule_description": rule_description,
                "severity": severity,
                "validated_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "ingestion_time": ingestion_ts,
                "rule_status": "ERROR",
                "failed_count": 0,
                "total_count": total_count,
                "failure_rate": 0.0,
                "validity_score": 100.0,
                "error_message": f"SQL condition error: {str(sql_error)}"
            }

        # Calculate failure rate
        failure_rate = round((failed_count / total_count)
                             * 100, 2) if total_count > 0 else 0.0
        # Calculate validity score
        validity_score = round(
            (1 - (failed_count / total_count)) * 100, 2) if total_count > 0 else 100.0

        # Determine rule status
        if failed_count == 0:
            rule_status = "PASSED"
            error_message = ""
        else:
            rule_status = "FAILED"
            error_message = f"Rule failed: {failed_count} records out of {total_count} failed validation"

        rule_result = {
            "table_name": table_name,
            "rule_name": rule_name,
            "rule_description": rule_description,
            "severity": severity,
            "validated_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "ingestion_time": ingestion_ts,
            "rule_status": rule_status,
            "failed_count": failed_count,
            "total_count": total_count,
            "failure_rate": failure_rate,
            "validity_score": validity_score,
            "error_message": error_message
        }

        logging.info(
            f"[{table_name}]→ Rule {rule_name}: {rule_status} (Failed: {failed_count}/{total_count}, Rate: {failure_rate}%)")
        return rule_result

    except Exception as e:
        logging.error(
            f"[{table_name}] ❌ Error validating rule {rule_config.get('rule_name', 'Unknown')}: {e}")
        return {
            "table_name": table_name,
            "rule_name": rule_config.get("rule_name", "Unknown"),
            "rule_description": rule_config.get("rule_description", ""),
            "severity": rule_config.get("severity", "UNKNOWN"),
            "validated_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "ingestion_time": ingestion_ts,
            "rule_status": "ERROR",
            "failed_count": 0,
            "total_count": 0,
            "failure_rate": 0.0,
            "validity_score": 100.0,
            "error_message": str(e)
        }


def save_rule_results(rule_results: List[Dict[str, Any]]):
    """Save data quality rule results to RDS PostgreSQL"""
    try:
        if not rule_results:
            return

        # Create DataFrame from rule results
        rule_df = spark.createDataFrame(rule_results)
        rule_dyf = DynamicFrame.fromDF(rule_df, glueContext, "rule_dyf")

        # Save to RDS
        glueContext.write_dynamic_frame.from_options(
            frame=rule_dyf,
            connection_type="jdbc",
            connection_options={
                "url": JDBC_URL,
                "user": DB_USERNAME,
                "password": DB_PASSWORD,
                "dbtable": f"{DATA_QUALITY_SCHEMA}.data_quality_rules",
                "driver": "org.postgresql.Driver",
                "batchsize": str(JDBC_BATCH_SIZE)
            }
        )

        logging.info(f"✅ {len(rule_results)} rule results saved to RDS")

    except Exception as e:
        logging.error(f"❌ Failed to save rule results: {e}")

# bao-doan


def save_rule_results_to_s3(rule_results: List[Dict[str, Any]], s3_output_path: str):
    """Save data quality rule results to S3 in Parquet format"""
    s3_output_path = "s3://mghi-dev-data-quality-bucket-us-west-2-154983253388/data_quality_rules"
    try:
        if not rule_results:
            return

        # Create DataFrame from rule results
        rule_df = spark.createDataFrame(rule_results)

        # Save DataFrame to S3 in Parquet format
        rule_df.write.parquet(s3_output_path, mode='overwrite')

        logging.info(
            f"✅ {len(rule_results)} rule results saved to S3 at {s3_output_path}")

    except Exception as e:
        logging.error(f"❌ Failed to save rule results to S3: {e}")
######


def get_table_list(database_name: str) -> List[str]:
    """Get list of tables from Glue Catalog"""
    try:
        glue_client = boto3.client('glue')
        table_list = []
        paginator = glue_client.get_paginator('get_tables')
        for page in paginator.paginate(DatabaseName=database_name):
            for table in page['TableList']:
                # Filter out null or empty table names
                if table['Name'] and table['Name'].strip():
                    table_list.append(table['Name'].strip())
                else:
                    logging.warning(
                        f"Skipping table with null or empty name: {table}")
        logging.info(
            f"Found {len(table_list)} valid tables in Glue Catalog: {table_list}")
        return table_list
    except Exception as e:
        logging.error(f"Failed to retrieve tables from Glue Catalog: {e}")
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


def process_table_rules(table_name: str, ingestion_ts: str, rules_config: Dict[str, Any]):
    """Process data quality rules for a single table"""
    max_retries = 3
    retry_count = 0

    while retry_count < max_retries:
        try:
            table_name_clean = table_name.replace(TABLE_PREFIX, "")
            s3_path = f"{S3_BRONZE_PATH}/{table_name_clean}/ingestion_ts={ingestion_ts}/"

            logging.info(f"[{table_name}]→ Reading data from {s3_path}")
            bronze_df = spark.read.parquet(s3_path)
            row_count = bronze_df.count()
            logging.info(
                f"[{table_name}]→ Read {row_count} rows from {s3_path}")

            # Get rules for this table
            table_rules = rules_config.get(table_name_clean, [])
            if not table_rules:
                logging.info(
                    f"[{table_name}]→ No rules configured for this table, skipping")
                processed_rules.append(table_name)
                break

            logging.info(
                f"[{table_name}]→ Found {len(table_rules)} rules to validate")

            # Validate each rule
            rule_results = []
            for rule_config in table_rules:
                rule_result = validate_rule(
                    bronze_df, table_name_clean, rule_config, ingestion_ts)
                rule_results.append(rule_result)

            # Save rule results
            # save_rule_results(rule_results)
            save_rule_results_to_s3(rule_results, '')

            processed_rules.append(table_name)
            logging.info(
                f"[{table_name}]✅ Data quality rules validation completed successfully")
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
                # Save error result
                error_result = {
                    "table_name": table_name.replace(TABLE_PREFIX, ""),
                    "rule_name": "table_processing_error",
                    "rule_description": "Error occurred while processing table",
                    "severity": "CRITICAL",
                    "validated_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "ingestion_time": ingestion_ts,
                    "rule_status": "ERROR",
                    "failed_count": 0,
                    "total_count": 0,
                    "failure_rate": 0.0,
                    "validity_score": 100.0,
                    "error_message": str(e)
                }
                save_rule_results([error_result])
                processed_rules.append(table_name)


def main():
    parser = argparse.ArgumentParser(
        description='Perform data quality rules validation on tables from S3 Bronze layer and save results to RDS Postgres')
    parser.add_argument('--ingestion-timestamp', required=False,
                        help='Ingestion timestamp to process (format: YYYY-MM-DD_HH-MM-SS). If not provided, use latest available for each table.')
    args_cli, unknown = parser.parse_known_args()

    # Validate JDBC connection first
    if not validate_jdbc_connection():
        logging.error("JDBC connection validation failed. Exiting.")
        sys.exit(1)

    # Validate S3 rules configuration
    if not validate_s3_rules_config():
        logging.error("S3 rules configuration validation failed. Exiting.")
        sys.exit(1)

    # Ensure data quality schema exists
    # if not ensure_data_quality_schema_exists():
    #     logging.error("Data quality schema creation failed. Exiting.")
    #     sys.exit(1)

    # Create data quality rules table
    if not create_data_quality_rules_table():
        logging.error("Data quality rules table creation failed. Exiting.")
        sys.exit(1)

    # Load rules configuration
    rules_config = load_rules_configuration()
    logging.info(f"Loaded rules configuration for {len(rules_config)} tables")

    # Log S3 configuration details
    if RULES_CONFIG_S3_BUCKET and RULES_CONFIG_S3_KEY:
        logging.info(
            f"📋 Rules Config: S3 (s3://{RULES_CONFIG_S3_BUCKET}/{RULES_CONFIG_S3_KEY})")
        if RULES_CONFIG_S3_REGION:
            logging.info(f"🌍 Rules Config Region: {RULES_CONFIG_S3_REGION}")
    elif RULES_CONFIG_FILE:
        logging.info(f"📋 Rules Config: {RULES_CONFIG_FILE}")
    else:
        logging.info(
            f"📋 Rules Config: Default S3 location (s3://{S3_BRONZE_BUCKET}/config/data-quality-rules.json)")

    # Log detailed rules information
    for table_name, rules in rules_config.items():
        logging.info(f"Table '{table_name}' has {len(rules)} rules:")
        for rule in rules:
            logging.info(
                f"  - {rule['rule_name']} ({rule['severity']}): {rule['rule_description']}")

    logging.info(f"Getting tables from Glue Catalog: {DATABASE_NAME}")
    logging.info(f"Using JDBC URL: {JDBC_URL}")
    logging.info(f"Target database: {DB_NAME}")
    logging.info(f"Data quality schema: {DATA_QUALITY_SCHEMA}")

    tables = get_table_list(DATABASE_NAME)
    if not tables:
        logging.error("No tables found in Glue Catalog. Exiting.")
        return

    # Filter tables that have rules configured
    # Get table names from rules_config keys (which are the table names from sql_condition)
    logging.info(f"Table prefix: {TABLE_PREFIX}")
    tables_with_rules = [
        table for table in tables
        if table.replace(TABLE_PREFIX, "").strip().lower() in [k.strip().lower() for k in rules_config.keys()]
    ]
    logging.info(
        f"Found {len(tables_with_rules)} tables with rules configured: {tables_with_rules}")

    if not tables_with_rules:
        logging.warning("No tables have rules configured. Exiting.")
        logging.info("Available tables in Glue Catalog: " + ", ".join(tables))
        logging.info("Tables with rules configured: " +
                     ", ".join(rules_config.keys()))
        return

    # Filter out null or empty table names
    valid_tables = [
        table for table in tables_with_rules if table and table.strip()]
    if len(valid_tables) != len(tables_with_rules):
        logging.warning(
            f"Filtered out {len(tables_with_rules) - len(valid_tables)} invalid table names")

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
            f"NORMAL MODE: Processing all {len(valid_tables)} tables with rules")
        tables_to_process = valid_tables

    table_ingestion_map = {}

    for table in tables_to_process:
        table_name_clean = table.replace(TABLE_PREFIX, "")
        if args_cli.ingestion_timestamp:
            partition_prefix = f"{S3_BRONZE_PREFIX}/{table_name_clean}/ingestion_ts={args_cli.ingestion_timestamp}/"
            response = S3_CLIENT.list_objects_v2(
                Bucket=S3_BRONZE_BUCKET, Prefix=partition_prefix, MaxKeys=1)
            if 'Contents' in response and response['Contents']:
                table_ingestion_map[table_name_clean] = args_cli.ingestion_timestamp
            else:
                logging.warning(
                    f"Table {table_name_clean} does not have data for ingestion_ts={args_cli.ingestion_timestamp}, skipping.")
        else:
            latest_ts = get_latest_ingestion_ts(
                S3_BRONZE_BUCKET, table_name_clean)
            if latest_ts:
                table_ingestion_map[table_name_clean] = latest_ts
            else:
                logging.warning(
                    f"Table {table_name_clean} has no ingestion_ts partitions in Bronze, skipping.")

    if not table_ingestion_map:
        logging.error("No tables to process. Exiting.")
        return

    # Process tables with rules
    logging.info(f"Tables with rules to process: {table_ingestion_map}")
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        executor.map(lambda t: process_table_rules(
            t[0], t[1], rules_config), table_ingestion_map.items())

    logging.info("✅ All data quality rules validation completed successfully!")
    logging.info(f"📊 Tables with rules processed: {len(table_ingestion_map)}")
    logging.info(f"📊 Total processed tables: {processed_rules}")

    # Print summary
    logging.info("=" * 80)
    logging.info("🎉 DATA QUALITY RULES JOB SUMMARY")
    logging.info("=" * 80)
    logging.info(f"📊 Total tables processed: {len(processed_rules)}")
    logging.info(f"✅ Tables with rules: {len(table_ingestion_map)}")
    logging.info(f"🔗 JDBC URL: {JDBC_URL}")
    logging.info(f"🗄️  Target Database: {DB_NAME}")
    logging.info(f"📋 Data Quality Schema: {DATA_QUALITY_SCHEMA}")
    logging.info(f"📦 Source: S3 Bronze Layer ({S3_BRONZE_PATH})")
    if RULES_CONFIG_S3_BUCKET and RULES_CONFIG_S3_KEY:
        logging.info(
            f"📋 Rules Config: S3 (s3://{RULES_CONFIG_S3_BUCKET}/{RULES_CONFIG_S3_KEY})")
        if RULES_CONFIG_S3_REGION:
            logging.info(f"🌍 Rules Config Region: {RULES_CONFIG_S3_REGION}")
    elif RULES_CONFIG_FILE:
        logging.info(f"📋 Rules Config: {RULES_CONFIG_FILE}")
    else:
        logging.info(
            f"📋 Rules Config: Default S3 location (s3://{S3_BRONZE_BUCKET}/configs/rules_config.json)")
    logging.info(
        f"⚙️  Batch Mode: {'Enabled' if USE_BATCHING else 'Disabled'}")
    if USE_BATCHING:
        logging.info(f"   Batch Number: {BATCH_NUMBER}")
        logging.info(f"   Batch Size: {BATCH_SIZE}")
    logging.info(f"🔄 Parallel Processing: {MAX_THREADS} threads")
    logging.info(f"⚡ JDBC Batch Size: {JDBC_BATCH_SIZE}")
    logging.info(
        f"📋 Rules Configuration: {len(rules_config)} tables configured")
    logging.info("=" * 80)
    job.commit()
    logging.info("🎉 Data Quality Rules Glue Spark Job completed.")


if __name__ == "__main__":
    main()
