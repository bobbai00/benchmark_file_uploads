import boto3
import requests
import subprocess
import time
import argparse
import os
from botocore.exceptions import ClientError


def ensure_bucket_exists(s3, bucket_name):
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
    except ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            print(f"Bucket '{bucket_name}' does not exist. Creating bucket...")
            s3.create_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' created successfully.")
        else:
            print(f"Error checking bucket: {e}")
            raise


def upload_minio(bucket, object_name, file_path, endpoint, access_key, secret_key):
    s3 = boto3.client('s3',
                      endpoint_url=endpoint,
                      aws_access_key_id=access_key,
                      aws_secret_access_key=secret_key)

    # Ensure the bucket exists
    ensure_bucket_exists(s3, bucket)

    start = time.time()
    s3.upload_file(file_path, bucket, object_name)
    end = time.time()
    return end - start


def download_minio(bucket, object_name, download_path, endpoint, access_key, secret_key):
    s3 = boto3.client('s3',
                      endpoint_url=endpoint,
                      aws_access_key_id=access_key,
                      aws_secret_access_key=secret_key)
    start = time.time()
    s3.download_file(bucket, object_name, download_path)
    end = time.time()
    return end - start


def upload_http(file_path, url):
    f = open(file_path, 'rb')
    files = {'file': (os.path.basename(file_path), f, 'application/octet-stream')}
    start = time.time()
    response = requests.post(url, json = None, files=files, stream=True, verify=False)
    end = time.time()
    return end - start, response.status_code


def upload_scp(file_path, user, host, remote_path):
    start = time.time()
    subprocess.run(["scp", file_path, f"{user}@{host}:{remote_path}"])
    end = time.time()
    return end - start


def main():
    parser = argparse.ArgumentParser(description='Benchmark file upload/download methods.')
    parser.add_argument('--file', type=str, required=True, help='Path to the file')
    parser.add_argument('--minio-endpoint', type=str, default='http://localhost:9000', help='MinIO endpoint')
    parser.add_argument('--minio-bucket', type=str, default='test-bucket', help='MinIO bucket name')
    parser.add_argument('--minio-access-key', type=str, default='minioadmin', help='MinIO access key')
    parser.add_argument('--minio-secret-key', type=str, default='minioadmin', help='MinIO secret key')
    parser.add_argument('--http-url', type=str, default='http://127.0.0.1:9999/upload', help='HTTP upload URL')
    parser.add_argument('--scp-user', type=str, default='bobbai0509', help='SCP username')
    parser.add_argument('--scp-host', type=str, default='cherry00', help='SCP host')
    parser.add_argument('--scp-remote-path', type=str, default='/tmp/', help='Remote path for SCP upload')
    args = parser.parse_args()

    # MinIO Upload
    print("Uploading to MinIO...")
    minio_upload_time = upload_minio(args.minio_bucket, 'testfile', args.file,
                                     args.minio_endpoint, args.minio_access_key, args.minio_secret_key)
    print(f"MinIO Upload Time: {minio_upload_time:.2f} seconds")

    # HTTP Upload
    print("Uploading via HTTP...")
    http_upload_time, status_code = upload_http(args.file, args.http_url)
    print(f"HTTP Upload Time: {http_upload_time:.2f} seconds, Status Code: {status_code}")

    # SCP Upload
    print("Uploading via SCP...")
    scp_upload_time = upload_scp(args.file, args.scp_user, args.scp_host, args.scp_remote_path)
    print(f"SCP Upload Time: {scp_upload_time:.2f} seconds")


if __name__ == "__main__":
    main()