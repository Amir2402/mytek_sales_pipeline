def read_data_into_table(read_table_name, current_year, current_month, current_day): 
    read_data_into_table = f"""
        CREATE TABLE {read_table_name}_table AS 
            SELECT 
                UNNEST(data) AS data
            FROM 
                read_json('s3://bronze/{read_table_name}/year={current_year}/month={current_month}/day={current_day}/*.json');
    """
    return read_data_into_table

def write_to_silver_layer(table_name, current_year, current_month, current_day):
    write_to_silver_query = f"""
        COPY {table_name} 
        TO 's3://silver/{table_name}/year={current_year}/month={current_month}/day={current_day}.parquet' (FORMAT parquet);
    """
    return write_to_silver_query

create_customers_table = """
    CREATE TABLE customers_table AS 
        WITH customer_cte AS (
            SELECT 
                data.customer_id AS customer_id, 
                data.first_name AS first_name, 
                data.last_name AS last_name, 
                data.gender AS gender, 
                data.age AS age,
                data.city AS city,
                data.email AS email,
                data.phone AS phone, 
                ROW_NUMBER() OVER (
                    PARTITION BY data.customer_id 
                    ORDER BY data.customer_id
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

#Use try_cast instead of cast in case the input record contains weird chars
create_products_table = """
    CREATE TABLE products_table AS
        WITH products_cte as 
            (SELECT 
                data.product_sku as product_sku, 
                data.product_name as product_name, 
                REPLACE(REPLACE(REPLACE(data.product_price, ',', '.'), 'DT', ''), ' ', '') AS product_price, 
                data.category as product_category, 
                data.subcategory as product_subcategory 
            FROM 
                mytek_products_table)
        SELECT
            product_sku,
            product_name, 
            TRY_CAST(product_price AS DOUBLE) AS product_price, 
            product_category, 
            product_subcategory
        FROM 
            products_cte; 
"""