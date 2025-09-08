import duckdb 

def connect_duck_db_to_S3(access_key, secret_key):
    conn = duckdb.connect()
    conn.install_extension("httpfs")
    conn.load_extension("httpfs")
    minio_endpoint = 'minio:9000'

    conn.execute(f"SET s3_access_key_id='{access_key}';")
    conn.execute(f"SET s3_secret_access_key='{secret_key}';")
    conn.execute(f"SET s3_endpoint='{minio_endpoint}';")
    conn.execute(f"SET s3_use_ssl=false;")
    conn.execute(f"SET s3_url_style='path';")

    return conn