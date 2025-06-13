import json
import boto3
import os

s3_client = boto3.client('s3')
TOOLING_ROLE_ARN = os.environ.get(
    'TOOLING_ROLE_ARN', "arn:aws:iam::154983253388:role/service-role/code-build-svc-role")
DBT_DATABASE = os.environ.get('DBT_DATABASE')
DBT_SCHEMA = os.environ.get('DBT_SCHEMA')
DBT_HOST = os.environ.get('DBT_HOST')
DBT_USER = os.environ.get('DBT_USER')
DBT_PORT = os.environ.get('DBT_PORT')
DBT_PASSWORD = os.environ.get('DBT_PASSWORD')
DEFAULT_BUCKET = os.environ.get(
    'S3_BUCKET', "datalake-analytics-bucket-154983253388-us-west-2")
PROJECT_NAME = os.environ.get('PROJECT_NAME', "mghi-dbt-build")
SOURCE_VERSION = os.environ.get('BRANCH_NAME', "develop")
MAPPING_PATH = "dbt_model_mapping/model_mapping.json"


def get_s3_json(bucket_name, object_key):
    try:
        # Read the file content
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        file_content = response['Body'].read().decode('utf-8')

        # Parse the content (assuming it's JSON with a table name)
        data = json.loads(file_content)
        return data
    except Exception as e:
        print(e)
        return None


def lambda_handler(event, context):
    try:
        # Extract bucket name and object key from the S3 event
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']

        changed_tables = get_s3_json(bucket_name, object_key)
        tables = [item.get('table_name') for item in changed_tables]

        models = []
        mapping_model = get_s3_json(DEFAULT_BUCKET, MAPPING_PATH)

        if mapping_model:
            for table in tables:
                if table in mapping_model:
                    models.extend(mapping_model[table])
            models_to_run = ' '.join(set(models))
        else:
            models_to_run = 'all'

        print(models_to_run)
        if len(models_to_run):
            # Assume the role in the tooling account
            sts_client = boto3.client('sts')
            assumed_role = sts_client.assume_role(
                RoleArn=TOOLING_ROLE_ARN,
                RoleSessionName="LambdaCodeBuildTrigger"
            )
            # Use temporary credentials to create a CodeBuild client
            credentials = assumed_role['Credentials']
            codebuild_client = boto3.client(
                'codebuild',
                aws_access_key_id=credentials['AccessKeyId'],
                aws_secret_access_key=credentials['SecretAccessKey'],
                aws_session_token=credentials['SessionToken']
            )
            # Add logic to trigger CodeBuild or another process

            # Environment variables to pass
            environment_variables = [
                {"name": "DBT_DATABASE", "value": DBT_DATABASE, "type": "PLAINTEXT"},
                {"name": "DBT_SCHEMA", "value": DBT_SCHEMA, "type": "PLAINTEXT"},
                {"name": "DBT_HOST", "value": DBT_HOST, "type": "PLAINTEXT"},
                {"name": "DBT_USER", "value": DBT_USER, "type": "PLAINTEXT"},
                {"name": "DBT_PORT", "value": DBT_PORT, "type": "PLAINTEXT"},
                {"name": "DBT_PASSWORD", "value": DBT_PASSWORD, "type": "PLAINTEXT"},
                {"name": "DBT_MODELS_TO_RUN",
                    "value": models_to_run, "type": "PLAINTEXT"},
            ]

            try:
                # Start the CodeBuild project with environment variables
                codebuild_client.start_build(
                    projectName=PROJECT_NAME,
                    sourceVersion=SOURCE_VERSION,
                    environmentVariablesOverride=environment_variables
                )
                return {
                    "statusCode": 200,
                    "body": json.dumps(f"Triggered CodeBuild with environment variables: {tables}")
                }
            except Exception as e:
                print(e)
                return {
                    "statusCode": 500,
                    "body": json.dumps(f"Error triggering CodeBuild: {str(e)}")
                }
    except Exception as e:
        print(e)

    return {
        'statusCode': 200,
        'body': f"There is no table changed!"
    }
