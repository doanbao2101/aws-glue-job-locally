
@REM Run Ubuntu on Windows Subsystem for Linux (WSL)
wsl -d Ubuntu

@REM Run docker

@REM  Run build Docker image: docker build -t glue-libs-with-openpyxl .

@REM  💻 Personal Machine

export PROFILE_NAME="la-nmh-dev"
export HOME="/mnt/c/Users/doanb/.aws"
export SCRIPT_PATH=""

@REM  🏢 Company Machine
export PROFILE_NAME="la-nmh-dev"
export HOME="/mnt/c/Users/BAO DOAN/.aws"
export SCRIPT_PATH="/mnt/c/Bao_Doan/Task/Task055_LA/aws-glue-job-locally/excel/"

@REM  🔄 Transfomation Job (Example)
@REM --jars /home/hadoop/jars/spark-excel_2.12-0.13.7.jar,/home/hadoop/jars/poi-ooxml-5.2.2.jar,/home/hadoop/jars/poi-5.2.2.jar,/home/hadoop/jars/commons-collections4-4.4.jar \

sudo docker run -it --rm \
  -v "${HOME}:/home/hadoop/.aws" \
  -v "${SCRIPT_PATH}/src:/home/hadoop/workspace/" \
  -e AWS_PROFILE="$PROFILE_NAME" \
  glue-libs-with-openpyxl-postgres \
  spark-submit \
    /home/hadoop/workspace/transformation.py \
    --JOB_NAME my-local-job \
    --BRONZE_BUCKET "lanhm-dev-datalake-raw-bucket-us-west-2-032397978411/galaxy/" \
    --SILVER_BUCKET "lanhm-dev-datalake-stage-bucket-us-west-2-032397978411/" \
    --FILES "final-attendance.xlsx"
    

