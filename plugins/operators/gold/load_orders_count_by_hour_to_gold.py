from airflow.sdk import BaseOperator
from include.transform.connect_to_duckdb import connect_duck_db_to_S3
from include.transform.queries import (read_silver_data_into_table, orders_count_by_hour,
                                       write_to_gold_layer)

class loadOrdersCountByHourToGold(BaseOperator): 
    def __init__(self, table_name, read_table_name, minio_access_key, minio_secret_key, current_timestamp, **kwargs):
        super().__init__(**kwargs) 
        self.table_name = table_name
        self.read_table_name = read_table_name
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.current_timestamp = current_timestamp
        self.conn = connect_duck_db_to_S3(self.minio_access_key, self.minio_secret_key)

    def execute(self, context):
        self.log.info('reading order_product table from S3')
        self.conn.sql(read_silver_data_into_table(self.read_table_name, self.current_timestamp.year,
                                                  self.current_timestamp.month, 
                                                  self.current_timestamp.day))

        self.log.info('Aggregating orders by hour')
        self.conn.sql(orders_count_by_hour)

        self.log.info('writing orders aggregation to gold layer')
        self.conn.sql(write_to_gold_layer(self.table_name, self.current_timestamp.year,
                                            self.current_timestamp.month, self.current_timestamp.day))
        self.log.info('Orders aggregation is written successfully to gold layer!')
         
