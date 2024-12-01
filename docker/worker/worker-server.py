import os
import subprocess
import logging
from minio import Minio
from minio.error import S3Error
import redis
import json
import requests 
import hashlib

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
bucket_name = "songs"
if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)
    logging.debug(f"Bucket '{bucket_name}' created in MinIO.")

# Path where processed tracks are saved
TRACK_STORAGE = "/app/track_storage"
os.makedirs(TRACK_STORAGE, exist_ok=True)

# Output bucket where separated tracks will be stored
output_bucket = "output"

if not minio_client.bucket_exists(output_bucket):
    minio_client.make_bucket(output_bucket)
    logging.debug(f"Output bucket '{output_bucket}' created in MinIO.")

def run_demucs(separation_command):
    """ Run the DEMUCS separation command and capture its output. """
    try:
        result = subprocess.run(separation_command, capture_output=True, text=True, check=True)
        logging.debug(f"DEMucs output: {result.stdout}")
        logging.error(f"DEMucs error: {result.stderr}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error running DEMucs command: {e}")
        logging.error(f"Error stdout: {e.stdout}")
        logging.error(f"Error stderr: {e.stderr}")

def process_task(songhash):
    try:
        # Download the MP3 file from MinIO
        mp3_file_path = f"/tmp/{songhash}.mp3"
        minio_client.fget_object(bucket_name, f"{songhash}.mp3", mp3_file_path)
        logging.debug(f"Downloaded MP3 file for {songhash} to {mp3_file_path}")

        # Construct the DEMUCS separation command
        output_dir = f"/tmp/output/{songhash}"
        os.makedirs(output_dir, exist_ok=True)

        separation_command = [
            "python3", "-m", "demucs.separate", 
            "--out", output_dir, 
            mp3_file_path,
            "--mp3"
        ]
        logging.debug(f"Running DEMUCS command: {' '.join(separation_command)}")

        # Run the separation
        run_demucs(separation_command)

        # Check if output was generated
        separated_dir = os.path.join(output_dir, "mdx_extra_q", songhash)
        if os.path.exists(separated_dir):
            logging.debug(f"Separated tracks are available in {separated_dir}")

            # Now upload the separated tracks to the 'output' bucket in MinIO
            for track in os.listdir(separated_dir):
                track_path = os.path.join(separated_dir, track)
                if os.path.isfile(track_path):
                    # Upload each track to MinIO
                    minio_object_path = f"{songhash}/{track}"
                    try:
                        minio_client.fput_object(output_bucket, minio_object_path, track_path)
                        logging.debug(f"Uploaded {minio_object_path} to MinIO '{output_bucket}' bucket.")
                    except S3Error as e:
                        logging.error(f"Error uploading {minio_object_path} to MinIO: {e}")
        else:
            logging.error(f"Separated tracks were not found in {separated_dir}")

    except Exception as e:
        logging.error(f"Error processing task for {songhash}: {str(e)}")

def worker():
    logging.debug("Worker is listening for tasks in the Redis queue...")
    while True:
        # Listen for tasks in Redis queue
        songhash = redis_client.brpop("toWorker", 0)[1]
        logging.debug(f"Received task for song with hash: {songhash}")

        # Retrieve callback data from Redis
        callback_data = redis_client.hget(songhash, "callback")
        if callback_data:
            callback = json.loads(callback_data)
            callback_url = callback.get("url")
            callback_payload = callback.get("data")

            # Process the task
            process_task(songhash)

            # Notify the callback URL with the result after processing
            if callback_url:
                try:
                    response = requests.post(callback_url, json={
                        "status": "completed",
                        "songhash": songhash,
                        "track_data": callback_payload
                    })
                    if response.status_code == 200:
                        logging.debug(f"Callback notification sent to {callback_url}")
                    else:
                        logging.error(f"Failed to send callback notification to {callback_url}, status code: {response.status_code}")
                except Exception as e:
                    logging.error(f"Error sending callback notification: {str(e)}")
        else:
            logging.error(f"No callback found for song with hash: {songhash}")

if __name__ == "__main__":
    worker()
