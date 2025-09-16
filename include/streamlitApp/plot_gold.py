import streamlit as st
from datetime import datetime 
from plugins.helpers.variables import MINIO_ACCESS_KEY, MINIO_SECRET_KEY 
from include.transform.queries import read_delta_data_into_table
from include.transform.connect_to_duckdb import connect_duck_db_to_S3

st.set_page_config(page_title="Sales Dashboard", layout="wide")

selected_date = st.date_input("Select a date", datetime.now())
year, month, day = selected_date.year, selected_date.month, selected_date.day

conn = connect_duck_db_to_S3(MINIO_ACCESS_KEY, MINIO_SECRET_KEY)

conn.sql(read_delta_data_into_table('products_sold_count_by_category', year, month, day, 'gold'))
products_df = conn.sql("SELECT * FROM products_sold_count_by_category;").df()

conn.sql(read_delta_data_into_table('orders_count_by_hour', year, month, day, 'gold'))
orders_df = conn.sql("SELECT * FROM orders_count_by_hour;").df()

conn.sql(read_delta_data_into_table('spending_by_city', year, month, day, 'gold'))
spending_df = conn.sql("SELECT * FROM spending_by_city;").df()

tab1, tab2, tab3 = st.tabs(["📦 Products", "🕒 Orders by Hour", "🏙️ Spending by City"])

with tab1:
    st.subheader("Products Sold by Category")
    st.metric("Total Products Sold", int(products_df["products_count"].sum()))
    st.bar_chart(products_df, x="product_category", y="products_count")
    st.dataframe(products_df, use_container_width=True)

with tab2:
    st.subheader("Orders Distribution by Hour")
    st.line_chart(orders_df, x="order_hour", y="orders_count")
    st.dataframe(orders_df, use_container_width=True)

with tab3:
    st.subheader("Spending by City")
    st.bar_chart(spending_df.sort_values("spending_by_city", ascending=False), x="city", y="spending_by_city")
    st.dataframe(spending_df, use_container_width=True)