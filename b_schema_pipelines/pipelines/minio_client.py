import boto3
from botocore.exceptions import ClientError
from utils.config import load_config

class MinioClient:
    def __init__(self):
        self.config = load_config("b_schema_pipelines/pipelines/pipeline_config.yaml")
        self.endpoint = self.config["minio"]["endpoint"]
        self.access_key = self.config["minio"]["access_key"]
        self.secret_key = self.config["minio"]["secret_key"]
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )
    
    def create_bucket(self, bucket_name):
        try:
            self.s3_client.create_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' created successfully.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
                print(f"Bucket '{bucket_name}' already exists and is owned by you.")
            else:
                print(f"Failed to create bucket '{bucket_name}': {e}")
                raise
