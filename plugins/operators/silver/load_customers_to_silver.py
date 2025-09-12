from airflow.sdk import BaseOperator
from include.transform.connect_to_duckdb import connect_duck_db_to_S3
from include.transform.queries import (read_data_into_table, write_to_silver_layer,
                                       create_customers_table)

class loadCustomersToSilver(BaseOperator): 
    def __init__(self, table_name, read_table_name, minio_access_key, minio_secret_key, current_timestamp, **kwargs):
        super().__init__(**kwargs) 
        self.table_name = table_name
        self.read_table_name = read_table_name
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.current_timestamp = current_timestamp
        self.conn = connect_duck_db_to_S3(self.minio_access_key, self.minio_secret_key)

    def execute(self, context):
        self.log.info('reading order table from S3')
        self.conn.sql(read_data_into_table(self.read_table_name, self.current_timestamp.year,
                                                  self.current_timestamp.month, 
                                                  self.current_timestamp.day))

        self.log.info('transforming orders into customer data')
        self.conn.sql(create_customers_table)

        self.log.info('writing customer data to silver layer')
        self.conn.sql(write_to_silver_layer(self.table_name, self.current_timestamp.year,
                                            self.current_timestamp.month, self.current_timestamp.day))
        self.log.info('customer data is written successfully to silver layer!')
