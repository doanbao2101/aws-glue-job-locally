@REM Download .jar

@REM Environments variables
export PROFILE_NAME="bao-doan"
export HOME="/mnt/c/Users/doanb/.aws"
export SCRIPT_PATH="/mnt/c/Bao_Doan/Task/Task052_MGHI/mghi-glue-job/append_data"
export SCRIPT="append_data.py"    
export JOB_NAME="append_data"

@REM Run command
docker run -it --rm \
  -v "${HOME}/.aws:/home/hadoop/.aws" \
  -v "${SCRIPT_PATH}/src:/home/hadoop/workspace" \
  -e AWS_PROFILE="${PROFILE_NAME}" \
  -e AWS_DEFAULT_REGION="us-west-2" \
  public.ecr.aws/glue/aws-glue-libs:5 \
  spark-submit \
    --packages org.postgresql:postgresql:42.7.4 \
    /home/hadoop/workspace/${SCRIPT} \
      --JOB_NAME "${JOB_NAME}" \
      --jdbc_url "jdbc:postgresql://host.docker.internal:5433/mghi_db" \
      --db_user "" \
      --db_password "" \
      --source_schema "s_marts" \
      --target_schema "d_marts" \
      --batchsize 5000
    
@REM For running on Cloud:  --jdbc_url "jdbc:postgresql://vizdatastack-mghivizdatardsstackmghirdsinstancecf5-kdnaz0gsb9lk.cv4k4ecqw2f3.us-west-2.rds.amazonaws.com:5432/mghi_db" \