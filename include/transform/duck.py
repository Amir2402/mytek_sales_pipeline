import duckdb 
from connect_to_duckdb import connect_duck_db_to_S3 
from queries import read_silver_data_into_table, total_products_sold_by_category, write_to_gold_layer
from datetime import datetime 

conn = connect_duck_db_to_S3('admin', 'admin123')
now = datetime.now()
conn.sql(f"""
        CREATE TABLE test AS 
            SELECT 
                *
            FROM 
                read_parquet('s3://gold/products_sold_count_by_category/year={now.year}/month={now.month}/day={now.day}.parquet');
    """)

conn.sql('SELECT * FROM test').show()

