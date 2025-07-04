---------------------
- Install WSL (Linux)
wsl -install

- Start Linux
wsl -d -Ubuntu

- Pull docker image
docker pull public.ecr.aws/glue/aws-glue-libs:5 

- Set env params
PROFILE_NAME="bao-doan"
1. Personally machine
HOME=/mnt/c/Users/doanb/.aws
SCRIPT_PATH=/mnt/c/Projects/MGHI/edp-glue-job/semi/

2. Company machine
HOME=/mnt/c/Users/BAO\ DOAN/.aws  
SCRIPT_PATH=/mnt/c/Bao_Doan/Task/Task052_MGHI/mghi-glue-job/semi


- Login aws through cmd
aws configure
aws sso login --profile bao-doan

--------------- INGESTION ---------------
sudo docker run -it --rm \
  -v ${HOME}:/home/hadoop/.aws \
  -v ${SCRIPT_PATH}:/home/hadoop/workspace/ \
  -e AWS_PROFILE=$PROFILE_NAME \
  public.ecr.aws/glue/aws-glue-libs:5 \
  spark-submit /home/hadoop/workspace/ingest.py \
  --JOB_NAME my-local-glue-job \
  --BASE_URL https://fakestoreapi.com/ \
  --S3_BRONZE_BUCKET project-dev-datalake-semi-data-bucket-us-west-2-154983253388 \
  --S3_BRONZE_PREFIX rest_api \
  --OUTPUT_FORMAT json \
  --CATEGORY_LIST products,users,carts \
  --IS_PARTITION false


--------------- TRANSFORMATION ---------------
sudo docker run -it --rm \
  -v ${HOME}:/home/hadoop/.aws \
  -v ${SCRIPT_PATH}:/home/hadoop/workspace/ \
  -e AWS_PROFILE=$PROFILE_NAME \
  public.ecr.aws/glue/aws-glue-libs:5 \
  spark-submit /home/hadoop/workspace/transform.py \
  --GLUE_DATABASE semi \
  --JOB_NAME my-local-glue-job \
  --S3_SILVER_BUCKET project-dev-datalake-semi-data-silver-bucket \
  --S3_SILVER_PREFIX rest_api

--------------- LOADING ---------------
sudo docker run -it --rm \
  -v ${HOME}:/home/hadoop/.aws \
  -v ${SCRIPT_PATH}:/home/hadoop/workspace/ \
  -e AWS_PROFILE=$PROFILE_NAME \
  public.ecr.aws/glue/aws-glue-libs:5 \
  spark-submit /home/hadoop/workspace/loading.py \
  --GLUE_DATABASE semi \
  --JOB_NAME my-local-glue-job \
  --S3_SILVER_BUCKET project-dev-datalake-semi-data-silver-bucket \
  --S3_SILVER_PREFIX rest_api \
  --DATA_DESTINATION redshift
