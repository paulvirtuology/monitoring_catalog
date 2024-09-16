import scrapy
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
from scrapy.crawler import CrawlerProcess
from multiprocessing import Pool
import pandas_gbq
import time
import itertools
from concurrent.futures import ThreadPoolExecutor
import math
import logging
from dotenv import load_dotenv, find_dotenv
import os



dotenv = find_dotenv()
load_dotenv(dotenv)

logging.basicConfig(
    filename='scrapy_spider.log',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.WARNING  # Niveau global des logs à partir de WARNING
)
# Définir les variables globales pour l'authentification
API_KEY=os.environ.get("SCRAPERAPI")
CREDS_FILE = './credentials.json'
credentials = service_account.Credentials.from_service_account_file(CREDS_FILE)
bq_client = bigquery.Client(credentials=credentials)

class PythonScrapyItem(scrapy.Item):
    name = scrapy.Field()
    categories = scrapy.Field()
    url = scrapy.Field()
    marques = scrapy.Field()
    link_marques=scrapy.Field()
    avalability=scrapy.Field()
    description=scrapy.Field()
    price1 = scrapy.Field()
    price2= scrapy.Field()
    bp=scrapy.Field()
    image =scrapy.Field()
    expedie_par=scrapy.Field()
    vendu_par=scrapy.Field()
    evaluation = scrapy.Field()
    etoiles=scrapy.Field()
    aplus=scrapy.Field()
    brand_story = scrapy.Field()
    variation_color= scrapy.Field()
    variation_size= scrapy.Field()
    variation_style= scrapy.Field()
    date= scrapy.Field()
    Client= scrapy.Field()

class MySpider(scrapy.Spider):
    name = "proxy_port_spider"
    custom_settings = {
        'ITEM_PIPELINES': {
            '__main__.DataFramePipeline': 300
        },
        'DOWNLOAD_DELAY': 0.1,
        'CONCURRENT_REQUESTS': 300,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 150,
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 0.1,
        'AUTOTHROTTLE_MAX_DELAY': 10,
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 80,
        'RETRY_TIMES': 3,
        'RETRY_HTTP_CODES': [429, 500, 502, 503, 504, 403, 408, 202],
        'REQUEST_FINGERPRINTER_IMPLEMENTATION': '2.7',
        'LOG_LEVEL': 'WARNING'
    }

    def __init__(self, urls=[]):
        self.urls = urls

    def start_requests(self):
        for url, client in self.urls:
            meta = {
                "proxy": f"http://scraperapi:{API_KEY}@proxy-server.scraperapi.com:8001",
                "proxy": f"https://scraperapi:{API_KEY}@proxy-server.scraperapi.com:8001",
                "client": client
            }
            yield scrapy.Request(url=url, callback=self.parse, meta=meta)

    def parse(self, response):
        client = response.meta['client']
        item = PythonScrapyItem()
        item['url'] = response.url
        item['name'] = response.css('#productTitle::text').get()
        item['categories'] = response.css('#wayfinding-breadcrumbs_feature_div ul li span a::text').getall()
        item['marques'] = response.css('#bylineInfo::text').get()
        item['link_marques'] = response.css('a#bylineInfo::attr(href)').get()
        item['price1'] = response.css('div#corePriceDisplay_desktop_feature_div span.a-price-whole::text').get()
        item['price2'] = response.css('div#corePriceDisplay_desktop_feature_div span.a-price-fraction::text').get()
        item['avalability'] = response.css('div#availability > span::text').get()
        item['description'] = response.css('#productDescription p span::text').get()
        
        try:
            item['bp'] = ';'.join(response.css('#productFactsDesktopExpander .a-size-base::text').getall())
            
            if not item['bp']:
                item['bp'] = ';'.join(response.css('span.visualRpdText::text').getall())
                
                if not item['bp']:
                    item['bp'] = ';'.join(response.css('#feature-bullets .a-list-item::text').getall())


        except Exception:
            item['bp'] = response.xpath('//ul[@class="a-unordered-list a-vertical a-spacing-mini"]/li/span/text()').extract()

        item['image'] = response.css('#altImages ul img::attr(src)').getall()

        try:
            item['expedie_par'] = response.css(".offer-display-feature-text-message::text").get()
        except:
            item['expedie_par'] = response.css("#fulfillerInfoFeature_feature_div .offer-display-feature-text-message").get()

        try:
            item['vendu_par'] = response.css("#merchantInfoFeature_feature_div > div.offer-display-feature-text > div > span::text").get()
            if item['vendu_par'] is None:
                item['vendu_par'] = response.css("#sellerProfileTriggerId::text").get()
        except:
            item['vendu_par'] = response.css("#merchantInfoFeature_feature_div .offer-display-feature-text-message::text").get()

        item['evaluation'] = response.css('div#cm_cr_dp_d_rating_histogram div.a-row.a-spacing-medium.averageStarRatingNumerical > span::text').get()
        item['etoiles'] = response.css('div#cm_cr_dp_d_rating_histogram span > span::text').get()
        block = response.css('[id^="aplus_feature_div"]')
        item['aplus'] = block.css('img::attr(src)').extract()
        brand = response.css('[id^="aplusBrandStory_feature_div"]')
        item['brand_story'] = brand.css('img::attr(src)').extract()
        item['variation_color'] = response.xpath('//*[@id="variation_color_name"]/ul/li/@data-defaultasin').extract()
        item['variation_style'] = response.xpath('//*[@id="variation_style_name"]/ul/li/@data-defaultasin').extract()
        item['variation_size'] = response.xpath('//*[@id="variation_size_name"]/ul/li/@data-defaultasin').extract()
        item['date'] = pd.to_datetime('today').strftime('%d/%m/%Y')
        item['Client'] = client

        yield item

class DataFramePipeline:
    def open_spider(self, spider):
        self.items = []
        self.counter = 0

    def close_spider(self, spider):
        if self.items:
            self.df = pd.DataFrame(self.items)
            self.send_to_google_sheet()
            self.items = []
            self.counter = 0

    def process_item(self, item, spider):
        self.items.append(dict(item))
        self.counter += 1
        if self.counter >= 1000:
            self.df = pd.DataFrame(self.items)
            self.send_to_google_sheet()
            self.items = []
            self.counter = 0
        return item
    
    def send_to_google_sheet(self):
        attributes = ['aplus', 'avalability', 'bp', 'brand_story', 'categories', 'description', 'etoiles', 'evaluation', 'expedie_par', 'image', 'link_marques', 'marques', 'name', 'price1', 'price2', 'url', 'variation_color', 'variation_size', 'variation_style', 'vendu_par', 'date', 'Client']
        df_to_upload = pd.DataFrame(self.items)[attributes]

        for attr in attributes:
            if df_to_upload[attr].dtype == object:
                df_to_upload[attr] = df_to_upload[attr].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)

        if not df_to_upload.empty:
            df_to_upload = df_to_upload.drop_duplicates()
            table_id = 'etail-335818.etail_testing.monitoring_live_stg'
            pandas_gbq.context.credentials = credentials
            pandas_gbq.context.project = 'etail-335818'
            pandas_gbq.to_gbq(df_to_upload, table_id, project_id='etail-335818', if_exists='append', progress_bar=True)
            print("Les données ont été chargées avec succès dans BigQuery.")

def fetch_data_in_batches(batch_size=10000):
    query = """
    SELECT DISTINCT ASIN, Pays, Client
    FROM `etail-335818.etail_prod.vendor_central_catalog` WHERE Availability <> 'Permanently unavailable'
    """
    query_job = bq_client.query(query)
    total_rows = query_job.result().total_rows
    num_batches = math.ceil(total_rows / batch_size)

    base_url = "https://www.amazon."
    
    for i in range(num_batches):
        offset = i * batch_size
        batch_query = f"{query} LIMIT {batch_size} OFFSET {offset}"
        batch_job = bq_client.query(batch_query)
        
        urls = []
    
        for row in batch_job.result():
            asin = row.ASIN
            pays = row.Pays
            client = row.Client
            
            if pays == "FR":
                url = f"{base_url}fr/dp/{asin}"
            elif pays == "DE":
                url = f"{base_url}de/dp/{asin}"
            elif pays == "ES":
                url = f"{base_url}es/dp/{asin}"
            elif pays == "IT":
                url = f"{base_url}it/dp/{asin}"
            elif pays == "NL":
                url = f"{base_url}nl/dp/{asin}"
            elif pays == "PL":
                url = f"{base_url}pl/dp/{asin}"
            elif pays == "GB":
                url = f"{base_url}co.uk/dp/{asin}"
            elif pays == "SE":
                url = f"{base_url}se/dp/{asin}"
            elif pays == "BE":
                url = f"{base_url}com.be/dp/{asin}"
            else:
                continue
            
            urls.append((url, client))
            
        yield urls

def process_batch(batch):
    run_spider(batch)

def run_spider(urls):
    process = CrawlerProcess()
    process.crawl(MySpider, urls=urls)
    process.start()

if __name__ == "__main__":
    start_time = time.time()
    
    with Pool(processes=8) as pool:
        pool.map(process_batch, fetch_data_in_batches(batch_size=10000))
    
    end_time = time.time()
    print(f'Total execution time: {end_time - start_time:.2f} seconds.')
