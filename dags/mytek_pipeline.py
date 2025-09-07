from airflow.decorators import dag
from plugins.operators.ingest_products import ingestProducts
from plugins.helpers.variables import MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_URL 
from datetime import datetime 

@dag(
    dag_id = "mytek_pipeline", 
    start_date = datetime(2021, 10, 10),
    catchup = False
)
def generate_dag(): 
    ingest_mytek_products = ingestProducts(
        task_id = 'ingest_mytek_products', 
        base_filename = 'mytek_data', 
        minio_url = MINIO_URL, 
        minio_access_key = MINIO_ACCESS_KEY, 
        minio_secret_key = MINIO_SECRET_KEY, 
        current_timestamp = datetime.now()
    )

    ingest_mytek_products 