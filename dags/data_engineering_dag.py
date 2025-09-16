"""
Airflow DAG Exercise: Spark → S3 Upload → S3 Download
-----------------------------------------------------
Complete the missing parts to build an Airflow DAG that:
1. Runs a Spark job first
2. Uploads results to MinIO/S3
3. Downloads results back from MinIO/S3

Hints:
- Use BashOperator to call Python scripts.
- Use the `>>` operator to set task order.
- Update the paths to your actual script locations.NOTE, the scripts' paths should be accessible within the Airflow container.
"""

# TODO: Import required Airflow classes
from datetime import datetime
from airflow import DAG
from airflow.operators.____ import ____   # TODO: import the correct operator

# TODO: Define the DAG
dag = DAG(
    '____',   # TODO: give a DAG name
    description='Run Spark job, then upload results to S3, then download back',
    schedule_interval='0 5 * * *',
    start_date=datetime(2019, 7, 10),
    catchup=False
)

# 1. Spark job (process data first)
spark_job_operator = ____(
    task_id='____',   # TODO: give a task id
    bash_command='python /path/to/airflow/scripts/____.py',  # TODO: update script path
    dag=dag
)

# 2. Upload results to MinIO/S3
s3_upload_operator = ____(
    task_id='____',
    bash_command='python /path/to/airflow/scripts/____.py',  # TODO: update script path
    dag=dag
)

# 3. Download results back from MinIO/S3
s3_download_operator = ____(
    task_id='____',
    bash_command='python /path/to/airflow/scripts/____.py',  # TODO: update script path
    dag=dag
)

# TODO: Set task order: Spark -> Upload -> Download
____ >> ____ >> ____
