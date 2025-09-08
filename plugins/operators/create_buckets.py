from airflow.sdk import BaseOperator
import boto3

class createBucketOperator(BaseOperator):
    def __init__(self,bucket_name, access_key, secret_key, endpoint_url, **kwargs):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.access_key = access_key
        self.secret_key = secret_key
        self.endpoint_url = endpoint_url

    def execute(self, context):
        s3 = boto3.resource(
        "s3",
        endpoint_url=self.endpoint_url,
        aws_access_key_id=self.access_key,
        aws_secret_access_key=self.secret_key
        )
        try:
            bucket = s3.Bucket(self.bucket_name)
            if bucket.creation_date:
                self.log.info("bucket exists") 
                
            else:    
                s3.create_bucket(Bucket=self.bucket_name)       
                self.log.info(f"Bucket {self.bucket_name} created successfully")

        except Exception as e:
            print("Error occured:", e)