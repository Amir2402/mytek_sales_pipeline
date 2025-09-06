from bs4 import BeautifulSoup 
import requests as rq 

def get_products_url(): 
    res = rq.get("https://www.mytek.tn/")
    soup = BeautifulSoup(res.content) 
    url_list = [] 
    all_products = soup.find('li', class_ = 'rootverticalnav nav-1 category-item')
    
    for item in all_products.find_all('div', class_ = 'grid-item-6 clearfix'): 
        item_category = item.find('div', class_ = 'title_normal').a.text
        
        for url in item.find_all('a', class_ = 'clearfix'):
            url_list.append([item_category, url.span.text, url['href']])

def ingest_products(): 
    products_url = get_products_url()
    
    for category, subcategory, link in products_url: 
        pass 
    