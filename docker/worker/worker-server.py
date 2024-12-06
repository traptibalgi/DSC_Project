import os
import subprocess
import logging
from minio import Minio
from minio.error import S3Error
import redis
import json
import requests 
import hashlib
import time
from bs4 import BeautifulSoup
import pandas as pd

# Initialize logging
logging.basicConfig(level=logging.DEBUG)

# Initialize Redis connection
redis_client = redis.StrictRedis(host='redis', port=6379, db=0, decode_responses=True)

# Configure MinIO client using environment variables
minio_client = Minio(
    'minio-proj.minio-ns.svc.cluster.local:9000',  # MinIO server host and port
    access_key='rootuser',  # MinIO root username
    secret_key='rootpass123',  # MinIO root password
    secure=False  # Disable SSL (assuming MinIO is not using HTTPS)
)

# Ensure that the bucket exists
bucket_name = "reviews"
if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)
    logging.debug(f"Bucket '{bucket_name}' created in MinIO.")

# Amazon scraping functions
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36',
    'Accept-Language': 'en-US, en;q=0.5'
}

def getdata(url):
    r = requests.get(url, headers=HEADERS)
    return r.text

def html_code(url):
    htmldata = getdata(url)
    soup = BeautifulSoup(htmldata, 'html.parser')
    return soup

def get_product_info(soup):
    product_name = soup.find('span', {'id': 'productTitle'})
    product_name = product_name.get_text().strip() if product_name else "N/A"
    
    user_rating = soup.find('span', {'class': 'a-icon-alt'})
    user_rating = user_rating.get_text().strip() if user_rating else "N/A"
    
    return product_name, user_rating

def get_reviews(soup):
    reviews = []
    review_elements = soup.find_all("div", class_="a-expander-content reviewText review-text-content a-expander-partial-collapse-content")
    
    for review_element in review_elements:
        review_text = review_element.get_text().strip()
        review_date = review_element.find_previous('span', class_="a-size-base a-color-secondary review-date")
        review_date = review_date.get_text().strip() if review_date else "N/A"
        reviews.append({'Review': review_text, 'Review Date': review_date})
    
    return reviews

def get_reviews_from_multiple_pages(product_url, num_pages=5):
    reviews = []
    soup = html_code(product_url)
    product_name, user_rating = get_product_info(soup)
    
    for page in range(1, num_pages + 1):
        paginated_url = f"{product_url}?pageNumber={page}"
        soup = html_code(paginated_url)
        page_reviews = get_reviews(soup)
        
        for review in page_reviews:
            reviews.append({
                'Product Name': product_name,
                'User Rating': user_rating,
                'Review Date': review['Review Date'],
                'Review': review['Review']
            })
        time.sleep(2)
    
    return reviews

def process_task(reviewhash):
    try:
        # Retrieve Amazon URL from Redis
        amazon_url = redis_client.hget(reviewhash, "amazon_url")
        if not amazon_url:
            logging.error(f"No Amazon URL provided for reviewhash {reviewhash}")
            return

        # Scrape reviews
        reviews_data = get_reviews_from_multiple_pages(amazon_url)
        if not reviews_data:
            logging.error(f"No reviews found for {amazon_url}")
            return

        # Save reviews to CSV
        csv_file = f"/tmp/{reviewhash}_reviews.csv"
        df = pd.DataFrame(reviews_data)
        df.to_csv(csv_file, index=False)
        logging.debug(f"CSV file created: {csv_file}")

        # Upload CSV to MinIO
        csv_object_path = f"{reviewhash}/reviews.csv"
        minio_client.fput_object(bucket_name, csv_object_path, csv_file)
        logging.debug(f"Uploaded CSV to MinIO: {csv_object_path}")

    except Exception as e:
        logging.error(f"Error processing task for {reviewhash}: {str(e)}")

# def process_retrieval(reviewhash):
#     try:
#         # Retrieve CSV from MinIO
#         csv_object_path = f"{reviewhash}/reviews.csv"
#         csv_file_path = f"/tmp/{reviewhash}_reviews.csv"
#         minio_client.fget_object(bucket_name, csv_object_path, csv_file_path)
#         logging.debug(f"Downloaded CSV from MinIO: {csv_file_path}")

#         # Load the CSV and extract the product name
#         df = pd.read_csv(csv_file_path)
#         product_name = df['Product Name'][0]  # Assuming all rows have the same product name

#         # Send product name to the REST server
#         payload = {"product_name": product_name}
#         response = requests.post(REST_SERVER_URL, json=payload)
#         if response.status_code == 200:
#             logging.debug(f"Product name sent to REST server: {product_name}")
#         else:
#             logging.error(f"Failed to send product name. REST server responded with: {response.text}")

#     except Exception as e:
#         logging.error(f"Error processing task for {reviewhash}: {str(e)}")

def worker():
    logging.debug("Worker is listening for tasks in the Redis queue...")
    while True:
        reviewhash = redis_client.brpop("toWorker", 0)[1]
        logging.debug(f"Received task for review with hash: {reviewhash}")
        process_task(reviewhash)

if __name__ == "__main__":
    worker()