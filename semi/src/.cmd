@REM  💻 Personal Machine

export PROFILE_NAME="bao-doan"
export HOME="/mnt/c/Users/doanb/.aws"
export SCRIPT_PATH="/mnt/c/Projects/MGHI/edp-glue-job/semi/"

@REM  🏢 Company Machine

export PROFILE_NAME="bao-doan"
export HOME="/mnt/c/Users/BAO DOAN/.aws"
export 


@REM 🚀 Run the Job:

sudo docker run -it --rm \
  -v "${HOME}:/home/hadoop/.aws" \
  -v "${SCRIPT_PATH}/src:/home/hadoop/workspace/" \
  -e AWS_PROFILE="$PROFILE_NAME" \
  public.ecr.aws/glue/aws-glue-libs:5 \
  spark-submit /home/hadoop/workspace/semi-ingestion.py \
    --JOB_NAME "my-local-glue-job" \
    --BASE_URL "https://fakestoreapi.com/" \
    --S3_BRONZE_BUCKET "mghi-dev-datalake-raw-bucket-us-west-2-154983253388" \
    --S3_BRONZE_PREFIX "rest_api" \
    --OUTPUT_FORMAT "json" \
    --CATEGORY_LIST "products,users,carts" \
    --IS_PARTITION "false"

@REM * **🔑 AWS Profile:** Set the `$PROFILE_NAME` environment variable to your AWS profile.
@REM * **📦 S3 Bucket:** Verify the correct bucket and permissions.
@REM * **🌐 Base URL:** Ensure the URL (`https://fakestoreapi.com/`) is correct for your data source.


@REM  🔄 Ingestion Job (Example)

sudo docker run -it --rm \
  -v "${HOME}:/home/hadoop/.aws" \
  -v "${SCRIPT_PATH}/src:/home/hadoop/workspace/" \
  -e AWS_PROFILE="$PROFILE_NAME" \
  public.ecr.aws/glue/aws-glue-libs:5 \
  spark-submit "/home/hadoop/workspace/semi-ingestion.py" \
    --JOB_NAME "my-local-glue-job" \
    --BASE_URL "https://fakestoreapi.com/" \
    --S3_BRONZE_BUCKET "mghi-dev-datalake-raw-bucket-us-west-2-154983253388" \
    --S3_BRONZE_PREFIX "rest_api" \
    --OUTPUT_FORMAT "json" \
    --CATEGORY_LIST "products,users,carts" \
    --IS_PARTITION "false"

@REM 🛠 Transformation Job (Example)

sudo docker run -it --rm \
  -v "${HOME}:/home/hadoop/.aws" \
  -v "${SCRIPT_PATH}/src:/home/hadoop/workspace/" \
  -e AWS_PROFILE="$PROFILE_NAME" \
  public.ecr.aws/glue/aws-glue-libs:5 \
  spark-submit "/home/hadoop/workspace/semi-transformation.py" \
    --GLUE_DATABASE "semi" \
    --JOB_NAME "my-local-glue-job" \
    --S3_SILVER_BUCKET "mghi-dev-datalake-stage-bucket-us-west-2-154983253388" \
    --S3_SILVER_PREFIX "rest_api"

@REM 📥 Loading Job (Example)

sudo docker run -it --rm \
  -v "${HOME}:/home/hadoop/.aws" \
  -v "${SCRIPT_PATH}/src:/home/hadoop/workspace/" \
  -e AWS_PROFILE="$PROFILE_NAME" \
  public.ecr.aws/glue/aws-glue-libs:5 \
  spark-submit "/home/hadoop/workspace/semi-loading.py" \
    --GLUE_DATABASE "semi" \
    --JOB_NAME "my-local-glue-job" \
    --S3_SILVER_BUCKET "mghi-dev-datalake-stage-bucket-us-west-2-154983253388" \
    --S3_SILVER_PREFIX "rest_api" \
    --DATA_DESTINATION "redshift"
