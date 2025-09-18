from include.transform.connect_to_duckdb import connect_duck_db_to_S3

conn = connect_duck_db_to_S3('admin', 'admin123')
conn.sql(f"""
        CREATE SECRET secret_minio_test (
        TYPE S3,
        ENDPOINT 'minio:9000',
        URL_STYLE 'path',
        USE_SSL false,
        KEY_ID admin,
        SECRET admin123
        );
        CREATE TABLE orders_products_joined AS 
            SELECT *
            FROM delta_scan('s3://silver/orders_products_joined');
    """ )

conn.sql('SELECT  FROM orders_products_joined;').show()