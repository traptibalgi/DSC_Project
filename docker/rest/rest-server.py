import os
import base64
import json
import redis
import hashlib
import logging
import io
import uuid
from flask import Flask, request, jsonify, send_file
from werkzeug.utils import secure_filename
from minio import Minio
from minio.error import S3Error

app = Flask(__name__)

# Path where processed tracks are saved
TRACK_STORAGE = "/app/track_storage"
os.makedirs(TRACK_STORAGE, exist_ok=True)

# Initialize logging
logging.basicConfig(level=logging.DEBUG)

# Initialize Redis connection
redis_client = redis.StrictRedis(host='redis', port=6379, db=0, decode_responses=True)

# Configure MinIO client using environment variables
minio_client = Minio(
    'minio-proj.minio-ns.svc.cluster.local:9000',  # MinIO server host and port
    access_key='rootuser',  # MinIO root username
    secret_key='rootpass123',  # MinIO root password
    secure=False  
)

# Ensure that the bucket exists
bucket_name = "songs"
if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)
    logging.debug(f"Bucket '{bucket_name}' created in MinIO.")

@app.route('/', methods=['GET'])
def hello():
    return '<h1>Music Separation Server</h1><p>Use a valid endpoint</p>'

@app.route('/apiv1/separate', methods=['POST'])
def separate():
    try:
        # Parse the incoming JSON
        data = request.get_json()
        logging.debug(f"Received data: {data}")

        callback = data.get("callback")

        # Check for required fields
        if 'mp3' not in data or 'callback' not in data:
            return jsonify({'error': 'Missing required fields: mp3 or callback'}), 400

        # Decode base64 MP3 data
        mp3_data = base64.b64decode(data['mp3'])
        songhash = uuid.uuid4().hex
        song_filename = secure_filename(f"{songhash}.mp3")

        # Convert mp3_data (bytes) into a file-like object using io.BytesIO
        mp3_file = io.BytesIO(mp3_data)

        # Upload MP3 to MinIO
        minio_client.put_object(
            bucket_name,
            song_filename,
            data=mp3_file,
            length=len(mp3_data),
            content_type='audio/mp3'
        )
        logging.debug(f"Uploaded {song_filename} to MinIO.")

        # Store job details in Redis
        redis_client.hset(songhash, mapping={
            "songhash": songhash,
            "callback": json.dumps(callback)
        })
        redis_client.lpush("toWorker", songhash)
        logging.debug(f"Song enqueued for separation with hash: {songhash}")

        return jsonify({'hash': songhash, 'reason': 'Song enqueued for separation'}), 200

    except Exception as e:
        logging.error(f"Error in /apiv1/separate: {str(e)}")
        return jsonify({'error': 'Internal Server Error', 'details': str(e)}), 500

@app.route('/apiv1/queue', methods=['GET'])
def get_queue():
    try:
        queue = redis_client.lrange("toWorker", 0, -1)
        return jsonify({'queue': queue}), 200
    except Exception as e:
        logging.error(f"Error in /apiv1/queue: {str(e)}")
        return jsonify({'error': 'Internal Server Error', 'details': str(e)}), 500

@app.route('/apiv1/track/<songhash>/<track>', methods=['GET'])
def get_track(songhash, track):
    try:
        track_filename = f"{songhash}_{track}.mp3"

        # Check if the track exists in MinIO
        try:
            track_data = minio_client.get_object(bucket_name, track_filename)
            return send_file(
                track_data,
                as_attachment=True,
                attachment_filename=f"{track}.mp3"
            )
        except S3Error as e:
            logging.error(f"Error fetching track from MinIO: {str(e)}")
            return jsonify({"error": "Track not found in MinIO"}), 404

    except Exception as e:
        logging.error(f"Error in /apiv1/track: {str(e)}")
        return jsonify({'error': 'Internal Server Error', 'details': str(e)}), 500

@app.route('/apiv1/remove/<songhash>/<track>', methods=['DELETE'])
def remove_track(songhash, track):
    try:
        track_filename = f"{songhash}_{track}.mp3"

        # Try to remove the track from MinIO
        try:
            minio_client.remove_object(bucket_name, track_filename)
            return jsonify({"status": "Track removed from MinIO"}), 200
        except S3Error as e:
            logging.error(f"Error deleting track from MinIO: {str(e)}")
            return jsonify({"error": "Track not found in MinIO"}), 404

    except Exception as e:
        logging.error(f"Error in /apiv1/remove: {str(e)}")
        return jsonify({'error': 'Internal Server Error', 'details': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)