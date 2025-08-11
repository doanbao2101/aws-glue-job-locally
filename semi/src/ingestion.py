import sys
import json
import requests
import boto3
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession

# -------------------------------
# Ingestion Job Arguments
# -------------------------------
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'BASE_URL',
    'S3_BRONZE_BUCKET',
    'S3_BRONZE_PREFIX',
    'OUTPUT_FORMAT',
    'CATEGORY_LIST',
    'IS_PARTITION'
])

BASE_URL = args['BASE_URL']
S3_BRONZE_BUCKET = args['S3_BRONZE_BUCKET']
S3_BRONZE_PREFIX = args['S3_BRONZE_PREFIX']
OUTPUT_FORMAT = args['OUTPUT_FORMAT'].lower()
IS_PARTITION = args['IS_PARTITION'].lower() == 'true'
RAW_CATEGORIES = args['CATEGORY_LIST']

# Validate format
if OUTPUT_FORMAT not in ['json', 'csv']:
    raise ValueError("❌ OUTPUT_FORMAT must be one of: json, csv")

# Parse CSV list from CATEGORY_LIST
CATEGORIES = [cat.strip() for cat in RAW_CATEGORIES.split(',') if cat.strip()]

# -------------------------------
# Glue Context Setup
# -------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3_client = boto3.client('s3', region_name='us-west-2')
INGESTION_DATE = datetime.utcnow().strftime('%Y-%m-%d')  # For partition
INGESTION_TIMESTAMP = datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S')

# -------------------------------
# Helper Functions
# -------------------------------


def fetch_api_data(base_url, category):
    url = f"{base_url}/{category}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def construct_s3_key(prefix, category, fmt):
    ext = {"json": "json", "csv": "csv", "parquet": "parquet"}[fmt]
    if IS_PARTITION:
        return f"{prefix}/{ext}/ingestion_dt={INGESTION_TIMESTAMP}/{category}.{ext}"
    else:
        # return f"{prefix}/{category}/data.{ext}"
        return f"{prefix}/{ext}/latest/{category}.{ext}"


def upload_to_s3(bucket, key, data, fmt):
    if fmt == "json":
        body = json.dumps(data)
        s3_client.put_object(Body=body, Bucket=bucket, Key=key)
        if IS_PARTITION:
            latest_key = key.replace(
                f"ingestion_dt={INGESTION_TIMESTAMP}", "latest")
            s3_client.put_object(Body=body, Bucket=bucket, Key=latest_key)
            print(f"✅ Uploaded to s3://{S3_BRONZE_BUCKET}/{latest_key}")

    elif fmt == "csv":
        import pandas as pd
        from io import StringIO
        df = pd.DataFrame(data)
        buffer = StringIO()
        df.to_csv(buffer, index=False)
        s3_client.put_object(Body=buffer.getvalue(), Bucket=bucket, Key=key)


# -------------------------------
# Main Loop
# -------------------------------
for category in CATEGORIES:
    try:
        print(f"📥 Fetching category: {category}")
        data = fetch_api_data(BASE_URL, category)
        s3_key = construct_s3_key(S3_BRONZE_PREFIX, category, OUTPUT_FORMAT)
        upload_to_s3(S3_BRONZE_BUCKET, s3_key, data, OUTPUT_FORMAT)
        print(f"✅ Uploaded to s3://{S3_BRONZE_BUCKET}/{s3_key}")
    except Exception as e:
        print(f"❌ Error processing '{category}': {e}")

try:
    print(f"📥 Fetching category: infectious")
    url = "https://cus2rfc1ee.execute-api.ap-southeast-1.amazonaws.com/infectious/national-leprosy?regionId=0100000000&provinceId=0102800000&cityId=0102802000&year=2025"
    payload = "{\"query\":\"\",\"variables\":{}}"
    headers = {
    'Authorization': 'Basic i2wOyfSbkRFStmxtdbR5CwsMj6AHhOYv46VvJkWEt4LPKX8zLdWzxJipTSw6ji7R',
    'Content-Type': 'application/json'
    }
    response = requests.request("GET", url, headers=headers, data=payload)
    data = response.json()
    s3_key = construct_s3_key(S3_BRONZE_PREFIX, 'infectious', OUTPUT_FORMAT)
    upload_to_s3(S3_BRONZE_BUCKET, s3_key, data, OUTPUT_FORMAT)
    print(f"✅ Uploaded to s3://{S3_BRONZE_BUCKET}/{s3_key}")
except Exception as e:
    print(f"❌ Error processing: {e}")

# -------------------------------
# Finish
# -------------------------------
job.commit()
