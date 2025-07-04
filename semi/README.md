# 🧪 AWS Glue Job: Local Docker Testing with WSL

This guide walks through how to run AWS Glue jobs locally using Docker on a WSL (Windows Subsystem for Linux) environment. It covers setup steps and how to run Ingestion, Transformation, and Loading (ETL) scripts.

---

## 🚀 Setup Instructions

### 1. Install & Launch WSL

```bash
# Install WSL
wsl --install

# Start Ubuntu (or your preferred distro)
wsl -d Ubuntu
```

### 2. Pull AWS Glue Docker Image

```bash
docker pull public.ecr.aws/glue/aws-glue-libs:5
```

### 3. Set Environment Variables

Set environment paths and AWS profile.

#### Personal Machine

```bash
export PROFILE_NAME="bao-doan"
export HOME=/mnt/c/Users/doanb/.aws
export SCRIPT_PATH=/mnt/c/Projects/MGHI/edp-glue-job/semi/src
```

#### Company Machine

```bash
export PROFILE_NAME="bao-doan"
export HOME="/mnt/c/Users/BAO DOAN/.aws"
export SCRIPT_PATH="/mnt/c/Bao_Doan/Task/Task052_MGHI/mghi-glue-job/semi/src"
```

> 💡 Use double quotes (`"`) for paths with spaces.

### 4. Authenticate with AWS

```bash
aws configure
aws sso login --profile $PROFILE_NAME
```

---

## ⚙️ Running Jobs

### 🔄 Ingestion Job

```bash
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
    --IS_PARTITION true
```

### 🛠 Transformation Job

```bash
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
```

### 📥 Loading Job

```bash
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
```

---

## 📝 Notes

* Make sure Docker is running in your WSL environment.
* Adjust volume mount paths (`HOME` and `SCRIPT_PATH`) if your folder structure differs.
* Ensure your AWS credentials and permissions are correctly configured for the job profile.
