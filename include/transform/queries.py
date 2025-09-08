def read_data_into_orders_table(read_table_name, current_year, current_month, current_day): 
    read_data_into_orders_table = f"""
        CREATE TABLE {read_table_name}_table AS 
            SELECT 
                UNNEST(data) AS orders_data
            FROM 
                read_json('s3://bronze/{read_table_name}/year={current_year}/month={current_month}/day={current_day}/*.json');
    """
    return read_data_into_orders_table

create_customers_table = """
    CREATE TABLE customers_table AS 
        WITH customer_cte AS (
            SELECT 
                orders_data.customer_id AS customer_id, 
                orders_data.first_name AS first_name, 
                orders_data.last_name AS last_name, 
                orders_data.gender AS gender, 
                orders_data.age AS age,
                orders_data.city AS city,
                orders_data.email AS email,
                orders_data.phone AS phone, 
                ROW_NUMBER() OVER (
                    PARTITION BY orders_data.customer_id 
                    ORDER BY orders_data.customer_id
                ) AS row_number
            FROM mytek_orders_table
        )
        SELECT 
            customer_id, 
            first_name, 
            last_name, 
            gender, 
            age,
            city,
            email,
            phone
        FROM 
            customer_cte 
        WHERE 
            row_number = 1; 
"""

def write_to_silver_layer(table_name, current_year, current_month, current_day):
    write_to_silver_query = f"""
        COPY {table_name} 
        TO 's3://silver/{table_name}/year={current_year}/month={current_month}/day={current_day}.parquet' (FORMAT parquet);
    """
    return write_to_silver_query