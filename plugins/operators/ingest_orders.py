from airflow.sdk import BaseOperator
from include.ingestion.generate_sales import generate_sales_data
from include.ingestion.write_object_to_S3 import write_to_s3 

class ingestOrders(BaseOperator):
    def __init__(self, base_filename, minio_url, minio_access_key, minio_secret_key, current_timestamp, **kwargs):
        super().__init__(**kwargs)
        self.base_filename = base_filename
        self.minio_url = minio_url
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.current_timestamp = current_timestamp 

    def execute(self, context): 
        self.log.info('ingesting mytek orders')
        mytek_orders = generate_sales_data() 

        self.log.info('writing data to S3')
        write_to_s3(mytek_orders, self.base_filename, self.minio_url, 
                    self.minio_access_key, self.minio_secret_key, self.current_timestamp)
        self.log.info('data is written successfully!')

