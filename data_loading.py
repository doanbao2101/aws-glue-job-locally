import sys
import logging
import json
import datetime
import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from sqlalchemy import create_engine, text
import psycopg2

# Logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Glue init
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
S3_CLIENT = boto3.client('s3')

# Params
S3_GOLD_BUCKET = "datalake-analytics-bucket-154983253388-us-west-2"
S3_GOLD_PATH = f"s3://{S3_GOLD_BUCKET}/sqlserver"
TABLE_LIST = [
    'repfaglfsis_financemgr',
    'repglperbook_financemanager',
    'repglperbook_financesv'
]

changed_tables = []

# PostgreSQL via ngrok
JDBC_URL = "jdbc:postgresql://0.tcp.ap.ngrok.io:13636/live_demo_new"
PG_PROPERTIES = {
    "user": "postgres",
    "password": "1",
    "driver": "org.postgresql.Driver"
}

# SQLAlchemy engine for DELETE and UPDATE
SQLALCHEMY_ENGINE = create_engine(
    "postgresql+psycopg2://postgres:1@0.tcp.ap.ngrok.io:13636/live_demo_new")


def write_to_s3(data, path):
    try:
        S3_CLIENT.put_object(
            Bucket=S3_GOLD_BUCKET,
            Key=path,
            Body=data
        )
        logging.info(f"🖊 Wrote data to S3: {path}")
    except Exception as e:
        logging.error(f"✗ Can't write data to s3 {path}: {e}")


def check_exist_and_schema_mismatch(table_name, gold_df):
    overwrite_required = False
    try:
        postgres_df = spark.read.jdbc(
            url=JDBC_URL,
            table=table_name,
            properties=PG_PROPERTIES
        )
        if set(gold_df.columns) != set(postgres_df.columns):
            logging.warning(f"[{table_name}]→ Schema mismatch detected.")
            overwrite_required = True
    except Exception as e:
        if "does not exist" in str(e).lower():
            logging.warning(f"[{table_name}]→ Table not found in PostgreSQL.")
            overwrite_required = True
        else:
            raise

    if overwrite_required:
        gold_df.write.jdbc(
            url=JDBC_URL,
            table=table_name,
            mode="overwrite",
            properties=PG_PROPERTIES
        )
        logging.info(f"[{table_name}]→ Table overwritten in PostgreSQL.")
        changed_tables.append(table_name)
        return gold_df, overwrite_required
    else:
        return postgres_df, overwrite_required


def process_table(table_name: str):
    try:
        s3_path = f"{S3_GOLD_PATH}/{table_name}/"
        gold_df = spark.read.parquet(s3_path)
        logging.info(
            f"[{table_name}]→ Read {gold_df.count()} rows from Gold layer: {s3_path}")

        # 1. Read data from PostgreSQL via JDBC
        postgres_df, is_overwritten = check_exist_and_schema_mismatch(
            table_name, gold_df)

        # 2. Determine insertions and deletions when table is not overwritten
        if not is_overwritten:
            logging.info(
                f"[{table_name}]→ Read {postgres_df.count()} rows from Postgres.")
            insert_data = gold_df.join(
                postgres_df, on="hash_value", how="left_anti")
            delete_data = postgres_df.join(
                gold_df, on="hash_value", how="left_anti")

            num_insert = insert_data.count()
            num_delete = delete_data.count()

            logging.info(
                f"[{table_name}][CDC] Insert: {num_insert}, Delete: {num_delete}")

            # Insert
            if num_insert > 0:
                insert_df = insert_data.select(*gold_df.columns)
                logging.info(
                    f"[{table_name}]→ Spark: Append {num_insert} records to PostgreSQL table successfully!")
                insert_df.write.jdbc(
                    url=JDBC_URL,
                    table=table_name,
                    mode="append",
                    properties=PG_PROPERTIES
                )

            # Delete
            if num_delete > 0:
                # Get all hash_value from delete_data as a list
                delete_ids = [row['hash_value']
                              for row in delete_data.select('hash_value').collect()]
                with SQLALCHEMY_ENGINE.begin() as conn:
                    batch_size = 500
                    for i in range(0, len(delete_ids), batch_size):
                        batch = delete_ids[i:i + batch_size]
                        ids_str = ",".join([f"'{x}'" for x in batch])
                        delete_sql = f"DELETE FROM {table_name} WHERE hash_value IN ({ids_str});"
                        conn.execute(text(delete_sql))
                        logging.info(
                            f"[{table_name}]→ Deleted {len(batch)} rows from PostgreSQL")
                        print(delete_sql)

            if num_insert > 0 or num_delete > 0:
                changed_tables.append(table_name)

        logging.info(f"✅ Completed!")

    except Exception as e:
        logging.error(f"[{table_name}] ❌ Failed processing table: {e}")
        raise


def main():
    for table_name in TABLE_LIST:
        process_table(table_name)
    logging.info("✅ All tables processed successfully!")

    # Update the status for changed tables
    logging.info(f"📷 Changed tables: {changed_tables}")
    dbt_path = f"dbt_tracking/{datetime.datetime.now().strftime('%Y%m%d')}_changed_tables.json"
    if len(changed_tables):
        changed_data = [{"table_name": table_name, "is_processed": True}
                        for table_name in changed_tables]
        data = json.dumps(changed_data)
        write_to_s3(data, dbt_path)


if __name__ == "__main__":
    main()
    job.commit()
    logging.info("🎉 Glue Spark Job completed.")
