from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from plugins.operators.create_buckets import createBucketOperator 
from plugins.operators.ingest_products import ingestProducts
from plugins.operators.ingest_orders import ingestOrders 
from plugins.operators.load_customers_to_silver import loadCustomersToSilver 
from plugins.operators.load_products_to_silver import loadProductsToSilver
from plugins.operators.load_orders_products_joined_to_silver import loadOrdersProductsJoinedToSilver
from plugins.helpers.variables import MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_URL 
from datetime import datetime 

@dag(
    dag_id = "mytek_pipeline", 
    start_date = datetime(2021, 10, 10),
    catchup = False
)
def generate_dag(): 
    create_bronze_bukcet = createBucketOperator(
        task_id = "create_bronze_bucket",
        bucket_name = "bronze", 
        access_key = MINIO_ACCESS_KEY, 
        secret_key = MINIO_SECRET_KEY, 
        endpoint_url = MINIO_URL
    )

    create_silver_bukcet = createBucketOperator(
        task_id = "create_silver_bucket",
        bucket_name = "silver", 
        access_key = MINIO_ACCESS_KEY, 
        secret_key = MINIO_SECRET_KEY, 
        endpoint_url = MINIO_URL
    )

    create_gold_bukcet = createBucketOperator(
        task_id = "create_gold_bucket",
        bucket_name = "gold", 
        access_key = MINIO_ACCESS_KEY, 
        secret_key = MINIO_SECRET_KEY, 
        endpoint_url = MINIO_URL
    )

    emptry_operator1 = EmptyOperator(
        task_id = 'empty_operator1'
    )

    ingest_mytek_products = ingestProducts(
        task_id = 'ingest_mytek_products', 
        base_filename = 'mytek_products', 
        minio_url = MINIO_URL, 
        minio_access_key = MINIO_ACCESS_KEY, 
        minio_secret_key = MINIO_SECRET_KEY, 
        current_timestamp = datetime.now()
    )

    ingest_mytek_orders = ingestOrders(
        task_id = 'ingest_mytek_orders', 
        base_filename = 'mytek_orders', 
        minio_url = MINIO_URL, 
        minio_access_key = MINIO_ACCESS_KEY, 
        minio_secret_key = MINIO_SECRET_KEY, 
        current_timestamp = datetime.now()
    )

    load_customers_to_silver = loadCustomersToSilver(
        task_id = 'load_customers_to_silver', 
        table_name = "customers_table",
        read_table_name = 'mytek_orders',
        minio_access_key = MINIO_ACCESS_KEY,
        minio_secret_key = MINIO_SECRET_KEY,
        current_timestamp = datetime.now(),
    )

    load_products_to_silver = loadProductsToSilver(
        task_id = 'load_products_to_silver', 
        table_name = "products_table",
        read_table_name = 'mytek_products',
        minio_access_key = MINIO_ACCESS_KEY,
        minio_secret_key = MINIO_SECRET_KEY,
        current_timestamp = datetime.now(),
    )

    load_orders_products_joined_to_silver = loadOrdersProductsJoinedToSilver(
        task_id = 'load_orders_products_joined_to_silver', 
        table_name = "orders_products_joined_table",
        read_table_name = 'products_table',
        minio_access_key = MINIO_ACCESS_KEY,
        minio_secret_key = MINIO_SECRET_KEY,
        current_timestamp = datetime.now(),
    )

    emptry_operator2 = EmptyOperator(
        task_id = 'empty_operator2'
    )

    [create_bronze_bukcet, create_silver_bukcet, create_gold_bukcet] >> emptry_operator1
    emptry_operator1 >> [ingest_mytek_orders, ingest_mytek_products] >> emptry_operator2
    emptry_operator2 >> [load_customers_to_silver, load_products_to_silver, load_orders_products_joined_to_silver]

generate_dag()