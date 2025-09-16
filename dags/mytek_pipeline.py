from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from plugins.operators.create_buckets import createBucketOperator 
from plugins.operators.bronze.ingest_products import ingestProducts
from plugins.operators.bronze.ingest_orders import ingestOrders 
from plugins.operators.silver.load_customers_to_silver import loadCustomersToSilver 
from plugins.operators.silver.load_products_to_silver import loadProductsToSilver
from plugins.operators.silver.load_orders_products_joined_to_silver import loadOrdersProductsJoinedToSilver
from plugins.operators.gold.load_products_count_to_gold import loadProductsCountToGold
from plugins.operators.gold.load_total_spending_by_city_to_gold import loadTotalSpendingByCityToGold
from plugins.operators.gold.load_orders_count_by_hour_to_gold import loadOrdersCountByHourToGold
from plugins.helpers.variables import MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_URL 
from datetime import datetime, timedelta

def check_daily_tasks(): 
    now = datetime.now()
    if now.hour == 0: 
        return ['load_customers_to_silver', 'load_products_to_silver']
    
    return ['end_workflow']

@dag(
    dag_id = "mytek_pipeline",
    start_date = datetime(2021, 10, 10),
    catchup = False,
    schedule = '@hourly'
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

    trigger_data_ingestion = EmptyOperator(
        task_id = 'trigger_data_ingestion'
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

    trigger_silver_layer_operations = BranchPythonOperator(
        task_id = 'trigger_silver_layer_operations',
        python_callable = check_daily_tasks
    )

    load_customers_to_silver = loadCustomersToSilver(
        task_id = 'load_customers_to_silver', 
        table_name = "customers_table",
        read_table_name = 'mytek_orders',
        minio_access_key = MINIO_ACCESS_KEY,
        minio_secret_key = MINIO_SECRET_KEY,
        current_timestamp = datetime.now() - timedelta(days=1),
    )

    load_products_to_silver = loadProductsToSilver(
        task_id = 'load_products_to_silver', 
        table_name = "products_table",
        read_table_name = 'mytek_products',
        minio_access_key = MINIO_ACCESS_KEY,
        minio_secret_key = MINIO_SECRET_KEY,
        current_timestamp = datetime.now() - timedelta(days=1),
    )

    load_orders_products_joined_to_silver = loadOrdersProductsJoinedToSilver(
        task_id = 'load_orders_products_joined_to_silver', 
        trigger_rule='one_success',
        table_name = "orders_products_joined",
        read_table_name = 'products_table',
        minio_access_key = MINIO_ACCESS_KEY,
        minio_secret_key = MINIO_SECRET_KEY,
        current_timestamp = datetime.now() - timedelta(days=1),
    )

    trigger_gold_layer_operations = EmptyOperator(
        task_id = 'trigger_gold_layer_operations'
    )

    load_products_count_to_gold = loadProductsCountToGold(
        task_id = 'load_products_by_category_to_gold', 
        table_name = "products_sold_count_by_category",
        read_table_name = 'orders_products_joined',
        minio_access_key = MINIO_ACCESS_KEY,
        minio_secret_key = MINIO_SECRET_KEY,
        current_timestamp = datetime.now() - timedelta(days=1),
    )

    load_total_spending_by_city_to_gold = loadTotalSpendingByCityToGold(
        task_id = 'load_total_spending_by_city_to_gold', 
        table_name = "spending_by_city",
        read_table_name = 'orders_products_joined',
        minio_access_key = MINIO_ACCESS_KEY,
        minio_secret_key = MINIO_SECRET_KEY,
        current_timestamp = datetime.now() - timedelta(days=1),
    )

    load_orders_count_by_hour_to_gold = loadOrdersCountByHourToGold(
        task_id = 'load_orders_count_by_hour_to_gold', 
        table_name = "orders_count_by_hour",
        read_table_name = 'orders_products_joined',
        minio_access_key = MINIO_ACCESS_KEY,
        minio_secret_key = MINIO_SECRET_KEY,
        current_timestamp = datetime.now() - timedelta(days=1),
    )

    end_workflow = EmptyOperator(
        task_id = "end_workflow"
    )

    
    [create_bronze_bukcet, create_silver_bukcet, create_gold_bukcet] >> trigger_data_ingestion
    trigger_data_ingestion >> [ingest_mytek_orders, ingest_mytek_products] >> trigger_silver_layer_operations
    trigger_silver_layer_operations >> [load_customers_to_silver, load_products_to_silver, end_workflow]
    [load_customers_to_silver, load_products_to_silver] >> load_orders_products_joined_to_silver >> trigger_gold_layer_operations 
    trigger_gold_layer_operations >> [load_products_count_to_gold, load_total_spending_by_city_to_gold, load_orders_count_by_hour_to_gold]

generate_dag()