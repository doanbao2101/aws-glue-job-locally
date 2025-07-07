# edp-glue-job

## Overview
The `edp-glue-job` is an ETL (Extract, Transform, Load) pipeline designed for the MGHI project. It processes raw data, applies transformations, and loads the data into S3 storage, organized into bronze, silver, and gold layers. The pipeline supports incremental loading using hash-based change detection, ensuring efficient updates and optimized performance.

## Features
- **Incremental Loading**: Uses hash-based change detection to process only updated data.
- **Layered Architecture**: Data is stored in S3 across bronze (raw), silver (transformed), and gold (curated) layers.
- **Modular Design**: Separate modules for ingestion, transformation, and loading.

## Project Structure
```
semi/
    src/
        ingestion.py       # Handles data ingestion from source systems
        loading.py         # Manages loading data into S3 layers
        transformation.py  # Applies transformations to raw data

structured/
    src/
        data_loading.py    # Handles structured data loading
        lambda_function.py # AWS Lambda function for triggering jobs

unstructured/
    src/                  # Placeholder for unstructured data processing
```

## Getting Started

### Prerequisites
- Python 3.8 or later
- AWS CLI configured with appropriate permissions
- Boto3 library installed

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
1. Configure your AWS environment and S3 bucket details in the configuration file.
2. Run the ingestion script:
   ```bash
   python semi/src/ingestion.py
   ```
3. Apply transformations:
   ```bash
   python semi/src/transformation.py
   ```
4. Load data into S3:
   ```bash
   python semi/src/loading.py
   ```

## Contributing
Contributions are welcome! Please fork the repository and submit a pull request with your changes.

## License
This project is licensed under the MIT License. See the LICENSE file for details.
