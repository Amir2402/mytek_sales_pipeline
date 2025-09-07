from bs4 import BeautifulSoup 
import requests as rq 

def get_products_url(): 
    res = rq.get("https://www.mytek.tn/")
    soup = BeautifulSoup(res.content, "html.parser") 
    url_list = [] 
    all_products = soup.find('li', class_ = 'rootverticalnav nav-1 category-item')
    
    for item in all_products.find_all('div', class_ = 'grid-item-6 clearfix'): 
        item_category = item.find('div', class_ = 'title_normal').a.text
        
        for url in item.find_all('a', class_ = 'clearfix'):
            url_list.append([item_category, url.span.text, url['href']])
    
    return url_list

def get_max_index_page(link):
    res = rq.get(link) 
    soup = BeautifulSoup(res.content, "html.parser")
    index_list = soup.find_all('li', class_ = 'page-item')

    if len(index_list) > 1: 
        return index_list[-2].a.text 

    return 1 

def fetch_page_details(request_url): 
    res = rq.get(request_url)
    soup = BeautifulSoup(res.content, 'html.parser')
    card_bodies = soup.find_all('div', class_ = 'card-body')
    page_items = []

    for item in card_bodies: 
        try: 
            product_name = item.find('a', class_ = 'product-item-link').text
            product_sku = item.find('div', class_ = 'sku').text
            product_price = item.find('span', class_ = 'final-price').text
            page_items.append({
                'product_sku': product_sku,
                'product_name': product_name,
                'product_price': product_price 
            })
        
        except: 
            print('different page structure')

    return page_items

def ingest_products(products_url): 
    products_url = get_products_url()
    mytek_items = []

    for category, subcategory, link in products_url: 
        max_index = get_max_index_page(link)
        
        for itr in range(1, int(max_index) + 1): 
            request_url = link + '?page' + str(itr)
            records = fetch_page_details(request_url)
            print(category, subcategory, itr)

            for record in records:  
                record['category'] = category
                record['subcategory'] = subcategory 
                mytek_items.append(record)

    return mytek_items


    