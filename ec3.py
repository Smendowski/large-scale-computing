import os
import time

import boto3
from botocore.exceptions import ClientError

S3_BUCKET_NAME = 'lab05-smendowski-mateusz'


class ClientS3:
    bucket_name: str

    def __init__(self):
        self.bucket_name = S3_BUCKET_NAME
        self.s3_client = boto3.client('s3', region_name='us-east-1')

    def upload(self, filename: str):
        obj = os.path.basename(filename)
        try:
            self.s3_client.upload_file(
                filename, self.bucket_name, obj)
        except ClientError as e:
            print(e)

    def download(self, filename: str):
        remote_name = filename
        local_name = f"{filename}-downloaded"
        self.s3_client.download_file(
            self.bucket_name, remote_name, local_name)


def lab05_scenario():
    s3 = ClientS3()

    tic = time.perf_counter()
    s3.upload('tempfile')
    toc = time.perf_counter()
    print(f"Upload took: {toc-tic} seconds.")

    tic = time.perf_counter()
    s3.download('tempfile')
    toc = time.perf_counter()
    print(f"Download took: {toc-tic} seconds.")


if __name__ == '__main__':
    lab05_scenario()
