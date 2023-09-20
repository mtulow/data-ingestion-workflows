import os
import sys
import wget
import logging
import threading
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

# Read environment variables
load_dotenv(Path(__file__).parent.parent / '.env')
# Read the S3 bucketname
BUCKET = os.environ.get("AWS_S3_BUCKET")


class ProgressPercentage(object):

    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()

def download_from_web(url: str, file_name: str):
    """Download file from web"""
    print()
    wget.download(url, file_name)
    print()
    return

def compress_file(file_name: str, file_path: str):
    """Compress file"""
    print()
    df = pd.read_parquet(file_name)
    df.to_parquet(file_path, engine='pyarrow', compression='gzip')
    print()
    return

def upload_to_s3(bucket: str, src_file: str, dst_file: str, *, prefix: str = 'trip-data'):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    config = TransferConfig(multipart_threshold=5 * 1024 * 1024, 
                            multipart_chunksize=5 * 1024 * 1024,
                            max_concurrency=10,
                            use_threads=True)

    client = boto3.client("s3")
    try:
        print()
        response = client.upload_file(src_file, bucket, dst_file,
                                      Config=config,
                                      Callback=ProgressPercentage(src_file),)
        print()
    except ClientError as e:
        logging.error(e)
        raise e

def delete_local_file(file_name: str):
    if os.path.isfile(file_name):
        os.remove(file_name)

def web_to_s3(service: str, year: int):
    """Download file from web, compress it, and upload it to S3."""
    # loop through months
    for month in range(1,13):
        # construct variables
        url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{service}_tripdata_{year:04d}-{month:02d}.parquet'
        file_name = f'{service}_tripdata_{year:04d}-{month:02d}.parquet'
        file_path = f'{service}_tripdata_{year:04d}-{month:02d}.parquet.gz'

        try:
            # download it using wget
            download_from_web(url, file_name)

            # compress it using pandas
            compress_file(file_name, file_path)

            # upload it to S3 
            upload_to_s3(BUCKET, file_path, file_path)
            
        finally:
            # delete local file
            delete_local_file(file_name)
            delete_local_file(file_path)


if __name__ == '__main__':
    print()
    web_to_s3('green', 2019)
    web_to_s3('green', 2020)
    # web_to_s3('yellow', 2019)
    # web_to_s3('yellow', 2020)
    print()