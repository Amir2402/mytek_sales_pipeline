import duckdb 
from deltalake.writer import write_deltalake
from plugins.helpers.variables import MINIO_ACCESS_KEY, MINIO_SECRET_KEY
import os
from duckdb import InternalException

os.environ["AWS_ALLOW_HTTP"] = "true"

def connect_duck_db_to_S3(access_key, secret_key):
    conn = duckdb.connect()
    
    conn.install_extension("httpfs")
    conn.load_extension("httpfs")
    conn.install_extension("delta")
    conn.load_extension("delta")
    minio_endpoint = 'minio:9000'
    
    conn.execute(f"SET s3_region='us-east-1';")
    conn.execute(f"SET s3_access_key_id='{access_key}';")
    conn.execute(f"SET s3_secret_access_key='{secret_key}';")
    conn.execute(f"SET s3_endpoint='{minio_endpoint}';")
    conn.execute(f"SET s3_use_ssl=false;")
    conn.execute(f"SET s3_url_style='path';")

    return conn

def write_delta_to_s3(table_name, conn, layer):
    storage_options = {"AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY, 
                        "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
                        "AWS_ENDPOINT_URL": "http://minio:9000"
                        }
    
    df = conn.sql(f"SELECT * FROM {table_name};").arrow()
    s3_path = f"s3://{layer}/{table_name}"
    write_deltalake(s3_path,
                    data = df, 
                    partition_by = ["year", "month", "day"],
                    storage_options = storage_options)



