from airflow.sdk import BaseOperator
from include.transform.connect_to_duckdb import connect_duck_db_to_S3
from include.transform.queries import read_silver_data_into_table, total_spending_by_city, write_to_gold_layer

class loadTotalSpendingByCityToGold(BaseOperator): 
    def __init__(self, table_name, read_table_name, minio_access_key, minio_secret_key, current_timestamp, **kwargs):
        super().__init__(**kwargs) 
        self.table_name = table_name
        self.read_table_name = read_table_name
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.current_timestamp = current_timestamp
        self.conn = connect_duck_db_to_S3(self.minio_access_key, self.minio_secret_key)

    def execute(self, context):
        self.log.info('reading orders_products table from S3')
        self.conn.sql(read_silver_data_into_table(self.read_table_name, self.current_timestamp.year,
                                                  self.current_timestamp.month, 
                                                  self.current_timestamp.day))
        
        self.log.info('reading customers table from S3')
        self.conn.sql(read_silver_data_into_table('customers_table', self.current_timestamp.year,
                                                  self.current_timestamp.month, 
                                                  self.current_timestamp.day))

        self.log.info('creating spending_by_city table')
        self.conn.sql(total_spending_by_city)

        self.log.info('writing spending_by_city data to gold layer')
        self.conn.sql(write_to_gold_layer(self.table_name, self.current_timestamp.year,
                                            self.current_timestamp.month, self.current_timestamp.day))
        self.log.info('spending_by_city data is written successfully to gold layer!')
         
