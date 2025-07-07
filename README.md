# 📦 Enterprise Data Platform Glue Job Scripts

## Overview

This repository contains AWS Glue job scripts that are essential components of the **Enterprise Data Platform (EDP)** for the MGHI project. These scripts support the **ingestion**, **transformation & validation**, and **loading** of data into an S3-based Data Lake, organized by data type and layer. The architecture is built to handle **structured**, **semi-structured**, and **unstructured** data efficiently.

Key design goals include modularity, scalability, and support for incremental data processing.

## Features

* 🔄 **Incremental Loading**
  Implements hash-based change detection to process only updated data, improving efficiency and performance.

* 🧱 **Layered Architecture**
  Organizes data into **bronze** (raw), **silver** (transformed), and **gold** (curated) layers in Amazon S3.

* 🧩 **Modular Design**
  Clear separation of logic across ingestion, transformation/validation, and loading.

* 🧠 **Multi-Data Type Support**
  Supports structured (e.g., RDBMS), semi-structured (e.g., JSON, CSV), and unstructured (e.g., logs, images) datasets.

## Project Structure

```
semi/
    src/
        ingestion.py         # Ingests semi-structured data from source systems
        loading.py           # Loads data into S3 bronze/silver/gold layers
        transformation.py    # Applies transformation and validation rules

structured/
    src/
        data_loading.py      # Ingests and loads structured data
        lambda_function.py   # AWS Lambda function to trigger Glue jobs

unstructured/
    src/
        # Placeholder for future unstructured data processing scripts
```

## Getting Started

### Prerequisites

* Python 3.8+
* AWS CLI configured with necessary IAM permissions
* Boto3 library

### Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/your-repo/edp-glue-job.git
   cd edp-glue-job
   ```

2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

### Usage

1. Configure your AWS credentials and S3 bucket details in the appropriate configuration file.

2. Run the ingestion script:

   ```bash
   python semi/src/ingestion.py
   ```

3. Perform data transformation and validation:

   ```bash
   python semi/src/transformation.py
   ```

4. Load processed data into S3:

   ```bash
   python semi/src/loading.py
   ```

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request. Make sure to follow the existing code structure and documentation guidelines.

## License

This project is licensed under the **MIT License**. See the `LICENSE` file for full details.