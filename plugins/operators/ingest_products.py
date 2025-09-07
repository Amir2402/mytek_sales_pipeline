from airflow.sdk import BaseOperator
from include.ingestion.ingest_products import ingest_products, get_products_url
from include.ingestion.write_object_to_S3 import write_to_s3 

class ingestProducts(BaseOperator):
    def __init__(self, base_filename, minio_url, minio_access_key, minio_secret_key, current_timestamp,**kwargs):
        self.base_filename = base_filename
        self.minio_url = minio_url
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.current_timestamp = current_timestamp 

    def execute(self):
        self.log.info('ingesting mytek products')
        mytek_items = ingest_products(get_products_url())

        self.log.info('writing data to S3')
        write_to_s3(mytek_items, self.base_filename, self.minio_url, 
                    self.minio_access_key, self.minio_secret_key)
        self.log.info('data is written successfully!')
