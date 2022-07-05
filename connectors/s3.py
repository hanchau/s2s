import boto3
from botocore.exceptions import ClientError


class S3Connector:
    def __init__(self, logger, region="ap-south-1"):
        try:
            self.logger = logger
            self.client = boto3.client('s3')
            self.res_client = boto3.resource('s3')
        except Exception as err:
            self.logger.error(f"Error in Connecting to S3: {err}")

    def list_buckets(self):
        try:
            response = self.client.list_buckets()
            self.logger.info('Existing buckets:')
            for bucket in response['Buckets']:
                self.logger.info(f"""  {bucket["Name"]}""")
            return response.get("Buckets")
        except Exception as err:
            self.logger.error(f"Error Retrieving Buckets: {err}")

    def list_objects(self, bucket):
        try:
            bucket_ob = self.res_client.Bucket(bucket)
            self.logger.info('Existing Objects in Bucket {bucket} are:')
            for object in bucket_ob.objects.all():
                self.logger.info(f"""  {bucket["Name"]}""")
            return bucket_ob.objects.all()
        except Exception as err:
            self.logger.error(f"Error Retrieving Buckets: {err}")



    def create_bucket(self, bucket, region="ap-south-1"):
        try:
            location = {'LocationConstraint': region}
            self.client.create_bucket(Bucket=bucket, CreateBucketConfiguration=location)
        except ClientError as err:
            self.logger.error(f"Bucket Creation Error: {err}")
            return False
        return True


    def upload_objects(self, bucket, src, dest):
        try:
            self.res_client.Bucket(bucket).upload_file(src, dest)
        except Exception as err:
            self.logger.error(f"Error inserting Objects: {err}")
            return False
        return True


    def download_objects(self, bucket, file, to_file):
        try:
            self.res_client.Bucket(bucket).download_file(file, to_file)
        except Exception as err:
            self.logger.error(f"Error inserting Objects: {err}")
            return False
        return True