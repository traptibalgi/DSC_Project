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
import bs4
from bs4 import BeautifulSoup
import pandas as pd
from transformers import pipeline
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
from io import StringIO
from transformers import AutoModelForSequenceClassification
import torch
from transformers import BertTokenizer, BertModel
from sklearn.preprocessing import normalize
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS
import nltk
from collections import Counter


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
output_bucket_name = "analysis_output"
if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)
    logging.debug(f"Bucket '{bucket_name}' created in MinIO.")

if not minio_client.bucket_exists(output_bucket_name):
    minio_client.make_bucket(output_bucket_name)
    logging.debug(f"Bucket '{output_bucket_name}' created in MinIO.")

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

def improved_summary(reviews):
    # Combine all reviews into a single string
    combined_reviews = ' '.join(reviews)

    # Load pre-trained summarization model and tokenizer
    model_name = "facebook/bart-large-cnn"  # Use a more advanced summarizer
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForSeq2SeqLM.from_pretrained(model_name)

    # Tokenize the input text
    inputs = tokenizer(
        combined_reviews,
        return_tensors="pt",
        truncation=True,
        max_length=1024  # Truncate large inputs to avoid memory issues
    )

    # Generate summary with adjusted decoding settings
    summary_ids = model.generate(
        inputs.input_ids,
        max_length=100,  # Set maximum summary length
        num_beams=4,     # Use beam search with 4 beams
        repetition_penalty=2.5,  # Penalize repetition
        length_penalty=1.0,      # Control output length
        early_stopping=True
    )

    # Decode the generated summary
    summary = tokenizer.decode(summary_ids[0], skip_special_tokens=True)

    return summary

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

        # ANALYSIS: Generate the summary of the reviews
        summary = improved_summary(df['Review'])

        # Save summary to DataFrame and then upload it to another MinIO bucket
        output_df = pd.DataFrame({'summary': [summary]})
        summary_file = f"/tmp/{reviewhash}_summary_output.csv"
        output_df.to_csv(summary_file, index=False)
        logging.info(f"Summary file created: {summary_file}")

        # Upload the summary to the analysis output bucket
        summary_object_path = f"{reviewhash}/summary_output.csv"
        minio_client.fput_object(output_bucket_name, summary_object_path, summary_file)
        logging.info(f"Uploaded summary to MinIO: {summary_object_path}")

    except Exception as e:
        logging.error(f"Error processing task for {reviewhash}: {str(e)}")

def worker():
    logging.debug("Worker is listening for tasks in the Redis queue...")
    while True:
        reviewhash = redis_client.brpop("toWorker", 0)[1]
        logging.debug(f"Received task for review with hash: {reviewhash}")
        process_task(reviewhash)

if __name__ == "__main__":
    worker()