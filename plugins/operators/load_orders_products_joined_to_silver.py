from airflow.sdk import BaseOperator
from include.transform.connect_to_duckdb import connect_duck_db_to_S3
from include.transform.queries import (read_data_into_table, read_silver_data_into_table, 
                                       create_orders_products_joined_table, write_to_silver_layer)

class loadOrdersProductsJoinedToSilver(BaseOperator): 
    def __init__(self, table_name, read_table_name, minio_access_key, minio_secret_key, current_timestamp, **kwargs):
        super().__init__(**kwargs) 
        self.table_name = table_name
        self.read_table_name = read_table_name
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.current_timestamp = current_timestamp
        self.conn = connect_duck_db_to_S3(self.minio_access_key, self.minio_secret_key)

    def execute(self, context):
        self.log.info('reading products table from S3')
        self.conn.sql(read_silver_data_into_table(self.read_table_name, self.current_timestamp.year,
                                                  self.current_timestamp.month, 
                                                  self.current_timestamp.day))

        self.log.info('reading orders table from S3')
        self.conn.sql(read_data_into_table('mytek_orders', self.current_timestamp.year,
                                                  self.current_timestamp.month, 
                                                  self.current_timestamp.day))
        
        self.log.info('creating orders_products_joined')
        self.conn.sql(create_orders_products_joined_table)

        self.log.info('writing orders_products_joined data to silver layer')
        self.conn.sql(write_to_silver_layer('orders_products_joined', self.current_timestamp.year,
                                            self.current_timestamp.month, self.current_timestamp.day))
        
        self.log.info('orders_products_joined is written successfully to silver layer!')
