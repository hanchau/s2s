from asyncio.log import logger
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
            self.logger.info(f'Existing buckets:')
            for bucket in response['Buckets']:
                self.logger.info(f"""----------> {bucket["Name"]}""")
            return response.get("Buckets")
        except Exception as err:
            self.logger.error(f"Error Retrieving Buckets: {err}")

    def list_objects(self, bucket):
        try:
            bucket_ob = self.res_client.Bucket(bucket)
            self.logger.info(f'Existing Objects in Bucket {bucket} are:')
            for object in bucket_ob.objects.all():
                self.logger.info(f"""----------> {object.key}""")
            return bucket_ob.objects.all()
        except Exception as err:
            self.logger.error(f"Error Retrieving Buckets: {err}")


    def delete_object(self, bucket, file):
        try:
            self.res_client.Object(bucket, file).delete()
            self.logger.info(f'Deleted Object {file} from Bucket {bucket}.')
            return True
        except Exception as err:
            self.logger.error(f"Error Deleting Object: {err}")
        return False


    def create_bucket(self, bucket, region="ap-south-1"):
        try:
            location = {'LocationConstraint': region}
            self.client.create_bucket(Bucket=bucket, CreateBucketConfiguration=location)
            self.logger.info(f"Bucket {bucket} created successfully.")
        except ClientError as err:
            self.logger.error(f"Bucket Creation Error: {err}")
            return False
        return True

    def delete_bucket(self, bucket):
        try:
            self.client.delete_bucket(Bucket=bucket)
            self.logger.info(f"Bucket {bucket} deleted successfully.")
        except ClientError as err:
            self.logger.error(f"Bucket Deletion Error: {err}")
            return False
        return True

    def upload_objects(self, bucket, src, dest):
        try:
            self.res_client.Bucket(bucket).upload_file(src, dest)
            self.logger.info(f"Uploaded object {src} to {dest} in Bucket {bucket}.")
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