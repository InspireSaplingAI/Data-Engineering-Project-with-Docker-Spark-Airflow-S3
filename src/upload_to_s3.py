"""
MinIO S3 Upload Exercise
------------------------
Complete the missing parts to make the code work with MinIO.
Hints:
1. You need to create a boto3 S3 client with MinIO endpoint and keys.
2. You will scan a local directory for CSV files.
3. Upload each CSV into the bucket under "Clean_Data/" folder.

Resources:
- MinIO docs: https://min.io/docs
- Boto3 S3 client: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
"""

import os
import boto3
from boto3.s3.transfer import S3Transfer

########################################
## Fill in your setup below ##
########################################
access_key = '____'   # TODO: add your MinIO access key
secret_key = '____'   # TODO: add your MinIO secret key
s3_bucket_name = '____'  # TODO: set your MinIO bucket name
endpoint_url = 'http://____:9000'  # TODO: set MinIO endpoint (host:port)
upload_base_path = '____'  # TODO: local directory to scan for CSVs

# TODO: Create a boto3 client with endpoint_url, keys, and config for s3v4 signing
client = boto3.client(
    's3',
    aws_access_key_id=____,
    aws_secret_access_key=____,
    endpoint_url=____,
    config=boto3.session.Config(signature_version='s3v4')
)

# Create a transfer manager
transfer = S3Transfer(client)

# Function to upload only CSV files
def uploadDirectory(filepath, s3_bucket_name):
    for root, dirs, files in os.walk(filepath):
        for file in files:
            if file.endswith('.csv'):  # Only CSV files
                local_path = os.path.join(root, file)
                s3_key = f"Processed_Data/{file}"
                print(f"Uploading {local_path} -> {s3_bucket_name}/{s3_key}")
                # TODO: use transfer.upload_file(...) to upload

# TODO: Call the function with your upload_base_path
uploadDirectory(filepath=____, s3_bucket_name=____)
