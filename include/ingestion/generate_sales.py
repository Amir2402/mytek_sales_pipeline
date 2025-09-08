import random 
import csv 
import uuid 
from datetime import datetime, timedelta

def random_datetime_last_hour():
    now = datetime.now()
    one_hour_ago = now - timedelta(hours=1)
    random_seconds = random.randint(0, int((now - one_hour_ago).total_seconds()))
    return one_hour_ago + timedelta(seconds=random_seconds)

def generate_sales_data(): 
    number_of_orders = random.randint(2000, 5000)

    with open('include/data/products_sku.csv', mode ='r') as file:
        products_sku = list(csv.reader(file))
    
    with open('include/data/customers.csv', mode ='r') as file:
        customers = list(csv.reader(file))
    orders_data = [] 

    for _ in range(0, number_of_orders): 
        order = {}
        products_per_order = random.randint(1, 10)
        customer_record = random.choice(customers)
        order_products = random.choices(products_sku, k = products_per_order)

        for itr in range(len(order_products)): 
            order_products[itr] = order_products[itr][0]
        order['order_id'] = str(uuid.uuid4())
        order['products'] = order_products
        order['customer_id'] = customer_record[0]
        order['first_name'] = customer_record[1]
        order['last_name'] = customer_record[2]
        order['gender'] = customer_record[3]
        order['age'] = customer_record[4]
        order['city'] = customer_record[5]
        order['email'] = customer_record[6]
        order['phone'] = customer_record[7] 
        order['order_date'] = random_datetime_last_hour().strftime("%Y-%m-%d %H:%M:%S")
        orders_data.append(order)
    
    return orders_data

print(generate_sales_data())