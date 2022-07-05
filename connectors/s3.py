import boto3
from botocore.exceptions import ClientError


class S3Connector:
    def __init__(self, logger):
        self.logger = logger

    def get_client(self, region="ap-south-1"):
        try:
            client = boto3.client('s3')
            return client
        except Exception as err:
            self.logger.error(f"Error in Connecting to S3: {err}")


    def list_buckets(self, client):
        try:
            response = client.list_buckets()
            self.logger.info('Existing buckets:')
            for bucket in response['Buckets']:
                self.logger.info(f"""  {bucket["Name"]}""")
            return response.get("Buckets")
        except Exception as err:
            self.logger.error(f"Error Retrieving Buckets: {err}")


    def create_bucket(self, client, bucket_name):
        try:
            location = {'LocationConstraint': "ap-south-1"}
            client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
        except ClientError as err:
            self.logger.error(f"Bucket Creation Error: {err}")
            return False
        return True


    def upload_objects(self, client, bucket_name, src, dest):
        try:
            client = boto3.resource('s3')
            client.Bucket(bucket_name).upload_file(src, dest)
        except Exception as err:
            self.logger.error(f"Error inserting Objects: {err}")
            return False
        return True
