from datetime import datetime 
from connect_to_duckdb import connect_duck_db_to_S3
from queries import read_data_into_table, create_product_table

conn = connect_duck_db_to_S3('admin', 'admin123')
now = datetime.now()

conn.sql(read_data_into_table('mytek_products', now.year, now.month, now.day))
conn.sql(create_product_table)

conn.sql('SELECT * FROM products_table').show()