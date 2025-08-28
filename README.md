# Data-Engineering-Project-with-Docker-Spark-Airflow-S3
Data Engineering project that utilizes docker, spark and S3 and orchestrate with airflow.

## Project Overview
- Practice building a data engineering pipeline using a real-world retail dataset to analyze delivery performance at scale
- The pipeline can be configured to use cloud storage solutions or MinIO, with the option to run processing locally using Docker

## Problem Statement
- Retailers need an omni-channel approach to adapt to the digital age, especially those with brick-and-mortar investments
- The project aims to create a data foundation for analytics and modeling, providing summary reports for decision-makers

## Tools and Technologies
- **Python**: Used for scripting and data manipulation
- **SQL**: Utilized for querying and data processing
- **Airflow**: Manages ETL workflows and job scheduling
- **Spark**: Handles large-scale data processing and analytics
- **Docker**: Enables local execution of the pipeline without cloud costs

### Cloud Storage Options
- AWS S3: Default storage solution
- GCP Cloud Storage: Optional storage solution
- Azure Blob Storage: Optional storage solution
- MinIO: Local storage alternative for S3-compatible operations

## ETL Pipeline
- **Extract**: Download data from chosen cloud storage or MinIO using Python and Boto3 or equivalent libraries
- **Transform**: Use Spark to manipulate and clean the data, focusing on delivery performance metrics
- **Load**: Upload the cleaned dataset back to the chosen storage solution for further analytics

## Dataset
- The dataset consists of tables from Ecommerce company
- It includes various CSV files representing different aspects of ecommerce operations

## Methodology
- **Data Lake Setup**: Create a mock production data lake using the chosen storage solution with the provided table schema
- **Data Analysis**: Perform exploratory data analysis (EDA) to identify delivery performance issues
- **Spark SQL Job**: Join tables to determine which orders/sellers missed delivery deadlines

## Airflow Configuration
- **Installation**: Install Airflow using `pip install apache-airflow`
- **Docker Usage**: Run Airflow in a Docker container for reproducibility and isolation
- **DAGs and Operators**: Write Python scripts to define ETL processes and steps

## Pipeline Steps
1. **Data Download**: Retrieve the Brazilian ecommerce data from the chosen storage solution
2. **Data Processing**: Use Spark SQL to join tables and filter for missed delivery deadlines
3. **Data Upload**: Save the processed data back to the storage solution in a designated folder for analytics

## Technical Details
- **Boto3 or Equivalent**: Used for interacting with cloud storage in Python scripts
- **Jupyter Notebooks**: Employed for EDA and visualization using libraries like Pandas and Matplotlib
- **Spark Session**: Set up to run SQL operations and write results to CSV

## Airflow DAG Configuration
1. **File Download**: Initial step to download data from storage
2. **Spark Job Execution**: Process data to identify missed delivery deadlines
3. **Data Upload**: Final step to upload results to storage

## Job Scheduling
- **Manual Triggering**: Use Airflow UI to manually trigger DAGs
- **Retry Mechanism**: Implement `.set_upstream()` to handle job retries

## Future Work
- Plan to integrate cloud-based processing using AWS EMR, GCP Dataproc, or Azure HDInsight for running Spark jobs on a

## Getting Started

### **EDA for existing data sets**

Understand the data, data cleaning, transformation using pandas. -- finish eda/eda_without_spark.ipynb

### **Spark Setup**

Follow this [post](https://www.startdataengineering.com/post/spark-local-setup/#3-use-vscode-devcontainers-to-set-up-spark-environment) to setup spark locally using docker without relying on cloud providers 

Then genarate "missed_shipping_limit_orders.csv" using spark and spark sql instead of pandas (like the eda) and save in data directory -- finish src/generate_results_with_spark.py


### **S3 setup**

 for this project, we could use cloud S3 providers but they are usually not free (or free with limitations). Here we use S3 compatible -MinIO [minIO](https://github.com/minio/minio?tab=readme-ov-file).

 e.g. on windows
To run MinIO on a 64-bit Windows host, first download the MinIO executable from [this URL](https://dl.min.io/server/minio/release/windows-amd64/minio.exe). Open a terminal or PowerShell window, navigate to the directory containing `minio.exe` (or add its location to your system `PATH`), and start a standalone MinIO server by running `minio.exe server C:\minio`, replacing `C:\minio` with your desired data storage path. By default, MinIO uses the root credentials `minioadmin:minioadmin`. You can access the MinIO Console, a web-based object browser, by visiting [http://127.0.0.1:9000](http://127.0.0.1:9000) in your browser and logging in with these credentials. The console allows you to create buckets, upload objects, and browse server contents.

### **Combined Docker Setup for Spark and Airflow**

This project uses a single Docker Compose setup to run both Spark and Airflow containers, simplifying orchestration and resource management.

#### 1 Prerequisites

- **Install Docker Desktop**  
    Download and install Docker Desktop from [docker.com](https://www.docker.com/products/docker-desktop).

- **Install Docker Compose**  
    Docker Desktop includes Docker Compose by default.

#### 2. Clone the Repository

```bash
git clone <your-repo-url>
cd Data-Engineering-Project-with-Docker-Spark-Airflow-S3
```

#### 3 Configure Docker Compose

Create a `docker-compose.yml` file in your project root with the following content:

```yaml
version: '3.8'

services:
    spark-master:
        image: bitnami/spark:latest
        container_name: spark-master
        environment:
            - SPARK_MODE=master
        ports:
            - "7077:7077"
            - "8080:8080"

    spark-worker:
        image: bitnami/spark:latest
        container_name: spark-worker
        environment:
            - SPARK_MODE=worker
            - SPARK_MASTER_URL=spark://spark-master:7077
        depends_on:
            - spark-master
        ports:
            - "8081:8081"

    airflow-webserver:
        image: apache/airflow:latest
        container_name: airflow-webserver
        environment:
            - AIRFLOW__CORE__EXECUTOR=LocalExecutor
            - AIRFLOW__CORE__FERNET_KEY=your_fernet_key
            - AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
            - AIRFLOW__CORE__LOAD_EXAMPLES=False
        volumes:
            - ./airflow/dags:/opt/airflow/dags
            - ./airflow/logs:/opt/airflow/logs
            - ./airflow/plugins:/opt/airflow/plugins
        ports:
            - "8082:8080"
        command: webserver
        depends_on:
            - spark-master
            - spark-worker
```

#### 4 Initialize Airflow Directories

```bash
mkdir -p airflow/dags airflow/logs airflow/plugins
```

#### 5 Start All Services

```bash
docker-compose up -d
```

#### 6 Initialize Airflow Database and Create Admin User

```bash
docker exec -it airflow-webserver airflow db init
docker exec -it airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

#### 7 Access UIs

- **Spark UI:** [http://localhost:8080](http://localhost:8080)
- **Airflow UI:** [http://localhost:8082](http://localhost:8082)

#### 8 Running Spark Jobs

Place your PySpark scripts in a shared directory or copy them into the master container. Submit jobs using:

```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /path/to/your_script.py
```

#### 9 Adding Airflow DAGs

Add your DAG Python files to `airflow/dags/` to have them automatically detected by Airflow.