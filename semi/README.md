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
