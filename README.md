# ⚙️ Run AWS Glue Jobs Locally with Docker + WSL

This guide helps you **run AWS Glue jobs locally** using **Docker** on **WSL (Windows Subsystem for Linux)**. It's useful for developing, testing, and debugging ETL scripts without deploying to AWS every time.

---

## ✅ Why Run AWS Glue Jobs Locally?

Running Glue jobs locally gives you several advantages:

- 🧪 **Faster Development Loop**: No need to wait for job deployment on the cloud.
- 💰 **Cost Savings**: Avoids AWS Glue compute charges during development.
- 🔧 **Flexible Debugging**: Easily test and debug locally using familiar tools.
- 🚫 **No Internet Dependency**: Work offline or behind firewalls.
- 🔍 **Detailed Logging**: Immediate access to local logs for quicker troubleshooting.

---

## 🏆 Prerequisites

Make sure you have the following:

- ✅ WSL2 with Ubuntu installed
- ✅ Docker Desktop installed & running
- ✅ AWS CLI installed and configured with SSO access
- ✅ Permissions to run Glue jobs
- ✅ Glue job scripts (`semi-ingestion.py`, `semi-transformation.py`, `semi-loading.py`)

---

## 🗺 Architecture Overview

Below is the ETL pipeline from data ingestion to analytics-ready storage:

```text
 ┌────────────┐       ┌────────────┐      ┌────────────┐
 │  REST API  │─────▶│ Glue Ingest│────▶ │  S3 Bronze │
 └────────────┘       └────────────┘      └────────────┘
                                              │
                                              ▼
                                       Glue Catalog (bronze_db)
                                              │
                                              ▼
                                       Glue Transform Job
                                              │
                                              ▼
                                       S3 Silver (.parquet)
                                              │
                                              ▼
                                       Glue Catalog (silver_db)
                                              │
                                              ▼
                                       Loading Job
                                              │
                                              ▼
    ┌────────────┬──────────────┬──────────────┐
    │ Redshift   │     RDS      │   DynamoDB   │
    │  (OLAP)    │ Transactional│   NoSQL DB   │
    └────────────┴──────────────┴──────────────┘
````

---

## 🔧 Setup Instructions

### 1. Install WSL and Launch Ubuntu

```bash
wsl --install
wsl -d Ubuntu
```

### 2. Pull AWS Glue Docker Image

```bash
docker pull public.ecr.aws/glue/aws-glue-libs:5
```

### 3. Set Environment Variables

Update the paths according to your environment:

#### 💻 Personal Machine

```bash
export PROFILE_NAME="bao-doan"
export HOME="/mnt/c/Users/doanb/.aws"
export SCRIPT_PATH="/mnt/c/Projects/MGHI/edp-glue-job/semi/"
```

#### 🏢 Company Machine

```bash
export PROFILE_NAME="bao-doan"
export HOME="/mnt/c/Users/BAO DOAN/.aws"
export SCRIPT_PATH="/mnt/c/Bao_Doan/Task/Task052_MGHI/mghi-glue-job/semi"
```

> 💡 Use quotes if the path contains spaces.

### 4. Authenticate via AWS SSO

```bash
aws configure sso
aws sso login --profile $PROFILE_NAME
```

---

## 🚀 Run Glue Jobs Locally

You can run Glue scripts dynamically by modifying the variables below:

```bash
# Example variables
export SCRIPT="semi-ingestion.py"         # semi-transformation.py or semi-loading.py
export JOB_NAME="my-local-glue-job"
export EXTRA_ARGS="--BASE_URL https://fakestoreapi.com/ --S3_BRONZE_BUCKET my-bronze-bucket --CATEGORY_LIST products,users"

# Run command
sudo docker run -it --rm \
  -v ${HOME}:/home/hadoop/.aws \
  -v ${SCRIPT_PATH}/src:/home/hadoop/workspace/ \
  -e AWS_PROFILE=$PROFILE_NAME \
  public.ecr.aws/glue/aws-glue-libs:5 \
  spark-submit /home/hadoop/workspace/$SCRIPT \
    --JOB_NAME $JOB_NAME $EXTRA_ARGS
```

---

### 🔄 Ingestion Job (Example)

```bash
sudo docker run -it --rm \
  -v ${HOME}:/home/hadoop/.aws \
  -v ${SCRIPT_PATH}/src:/home/hadoop/workspace/ \
  -e AWS_PROFILE=$PROFILE_NAME \
  public.ecr.aws/glue/aws-glue-libs:5 \
  spark-submit /home/hadoop/workspace/semi-ingestion.py \
    --JOB_NAME my-local-glue-job \
    --BASE_URL https://fakestoreapi.com/ \
    --S3_BRONZE_BUCKET mghi-dev-datalake-raw-bucket-us-west-2-154983253388 \
    --S3_BRONZE_PREFIX rest_api \
    --OUTPUT_FORMAT json \
    --CATEGORY_LIST products,users,carts \
    --IS_PARTITION false
```

### 🛠 Transformation Job (Example)

```bash
sudo docker run -it --rm \
  -v ${HOME}:/home/hadoop/.aws \
  -v ${SCRIPT_PATH}/src:/home/hadoop/workspace/ \
  -e AWS_PROFILE=$PROFILE_NAME \
  public.ecr.aws/glue/aws-glue-libs:5 \
  spark-submit /home/hadoop/workspace/semi-transformation.py \
    --GLUE_DATABASE semi \
    --JOB_NAME my-local-glue-job \
    --S3_SILVER_BUCKET mghi-dev-datalake-stage-bucket-us-west-2-154983253388 \
    --S3_SILVER_PREFIX rest_api
```

### 📥 Loading Job (Example)

```bash
sudo docker run -it --rm \
  -v ${HOME}:/home/hadoop/.aws \
  -v ${SCRIPT_PATH}/src:/home/hadoop/workspace/ \
  -e AWS_PROFILE=$PROFILE_NAME \
  public.ecr.aws/glue/aws-glue-libs:5 \
  spark-submit /home/hadoop/workspace/semi-loading.py \
    --GLUE_DATABASE semi \
    --JOB_NAME my-local-glue-job \
    --S3_SILVER_BUCKET mghi-dev-datalake-stage-bucket-us-west-2-154983253388 \
    --S3_SILVER_PREFIX rest_api \
    --DATA_DESTINATION redshift
```

---

## 📝 Notes

* 🐳 Ensure Docker is running under WSL integration.
* 🔄 Adjust paths (`HOME`, `SCRIPT_PATH`) based on your system structure.
* 🔐 Make sure your AWS profile has Glue and S3 permissions.
* 🛠 Use logs printed by the container for debugging and iteration.

---

## 📚 References

* [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
* [Glue Docker Image – AWS Public ECR](https://gallery.ecr.aws/glue/aws-glue-libs)
* [WSL Installation Guide – Microsoft](https://learn.microsoft.com/en-us/windows/wsl/)

---