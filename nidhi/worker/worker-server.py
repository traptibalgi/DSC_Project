import os
import logging
from minio import Minio
from minio.error import S3Error
import redis
import requests 
import time
import bs4
from bs4 import BeautifulSoup
import pandas as pd

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize Redis connection
redis_client = redis.StrictRedis(host='redis', port=6379, db=0, decode_responses=True)

# Configure MinIO client
minio_client = Minio(
    'minio-proj.minio-ns.svc.cluster.local:9000',
    access_key='rootuser',
    secret_key='rootpass123',
    secure=False
)

# Ensure that the bucket exists
bucket_name = "reviews"
try:
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
        logging.info(f"Bucket '{bucket_name}' created in MinIO.")
except S3Error as e:
    logging.error(f"Error with MinIO: {e}")

# Amazon scraping constants
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36',
    'Accept-Language': 'en-US, en;q=0.5'
}

def getdata(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        r.raise_for_status()
        return r.text
    except requests.RequestException as e:
        logging.error(f"Error fetching URL {url}: {e}")
        return None

def html_code(url):
    htmldata = getdata(url)
    if htmldata:
        return BeautifulSoup(htmldata, 'html.parser')
    return None

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
    if not soup:
        return reviews

    product_name, user_rating = get_product_info(soup)
    
    for page in range(1, num_pages + 1):
        paginated_url = f"{product_url}?pageNumber={page}"
        soup = html_code(paginated_url)
        if not soup:
            continue
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
        logging.info(f"Starting task for reviewhash: {reviewhash}")
        redis_client.hset(reviewhash, "status", "processing")

        amazon_url = redis_client.hget(reviewhash, "link")
        if not amazon_url:
            raise ValueError(f"No Amazon URL provided for reviewhash {reviewhash}")

        logging.info(f"Scraping reviews for URL: {amazon_url}")
        reviews_data = get_reviews_from_multiple_pages(amazon_url)
        if not reviews_data:
            raise ValueError(f"No reviews found for {amazon_url}")

        csv_file = f"/tmp/{reviewhash}_reviews.csv"
        df = pd.DataFrame(reviews_data)
        df.to_csv(csv_file, index=False)
        logging.info(f"CSV file created: {csv_file}")

        csv_object_path = f"{reviewhash}/reviews.csv"
        minio_client.fput_object(bucket_name, csv_object_path, csv_file)
        logging.info(f"Uploaded CSV to MinIO: {csv_object_path}")

        redis_client.hset(reviewhash, "status", "completed")
        logging.info(f"Task completed for reviewhash: {reviewhash}")

    except requests.exceptions.RequestException as e:
        logging.error(f"Network error for {reviewhash}: {str(e)}")
        redis_client.hset(reviewhash, "status", "failed")
    except ValueError as e:
        logging.error(str(e))
        redis_client.hset(reviewhash, "status", "failed")
    except Exception as e:
        logging.error(f"Unexpected error for {reviewhash}: {str(e)}")
        redis_client.hset(reviewhash, "status", "failed")
    # finally:
    #     if os.path.exists(csv_file):
    #         os.remove(csv_file)
    #         logging.info(f"Temporary file removed: {csv_file}")

def worker():
    logging.info("Worker is listening for tasks in the Redis queue...")
    while True:
        try:
            task = redis_client.brpop("linkQueue", 0)
            if task:
                reviewhash = task[1]
                logging.info(f"Received task for review with hash: {reviewhash}")
                process_task(reviewhash)
        except redis.RedisError as e:
            logging.error(f"Redis error: {e}")
            time.sleep(5)  # Wait before retrying
        except Exception as e:
            logging.error(f"Unexpected error in worker loop: {e}")
            time.sleep(5)  # Wait before retrying

if __name__ == "__main__":
    worker()