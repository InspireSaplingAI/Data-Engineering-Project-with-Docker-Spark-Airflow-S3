"""
MinIO S3 Download Exercise
--------------------------
Complete the missing parts to make the code work with MinIO.
Hints:
1. You need to create a boto3 S3 client with MinIO endpoint and keys.
2. Use the client to download a file from the bucket.
3. Make sure the local download path exists (or create it).
"""

import boto3
import os

########################################
## Fill in your setup below ##
########################################
access_key = '____'   # TODO: add your MinIO access key
secret_key = '____'   # TODO: add your MinIO secret key
s3_bucket_name = '____'  # TODO: set your MinIO bucket name
s3_filename = 'Processed_Data/missed_shipping_limit_orders.csv'  # remote file key
download_path = '____'  # TODO: local path where file will be saved, e.g. /data/missed_shipping_limit_orders.csv (NOTE, /data is volumn mounted in docker)
endpoint_url = 'http://____:9000'  # TODO: set MinIO endpoint (host:port)

# Ensure download directory exists
os.makedirs(os.path.dirname(download_path), exist_ok=True)

# TODO: Create a boto3 client with endpoint_url, keys, and config for s3v4 signing
client = boto3.client(
    's3',
    aws_access_key_id=____,
    aws_secret_access_key=____,
    endpoint_url=____,
    config=boto3.session.Config(signature_version='s3v4')
)

print("Downloading from MinIO...")

# TODO: Download the file from MinIO
client.download_file(____, ____, ____)

print(f"Download complete: {download_path}")
