import os
import json
import logging
import redis
from flask import Flask, request, jsonify

# Initialize Flask app
app = Flask(__name__)

# Initialize logging
logging.basicConfig(level=logging.DEBUG)

# Initialize Redis connection
redis_client = redis.StrictRedis(host='redis', port=6379, db=0, decode_responses=True)

# Endpoint to accept Amazon link and push it to Redis
@app.route('/apiv1/link', methods=['POST'])
def add_link():
    try:
        # Parse the incoming JSON
        data = request.get_json()
        logging.debug(f"Received data: {data}")

        # Validate the input
        link = data.get("link")
        if not link or not link.startswith("https://www.amazon."):
            return jsonify({"error": "Invalid or missing Amazon link"}), 400

        # Generate a unique identifier for the link
        link_id = str(uuid.uuid4())

        # Store the link in Redis
        redis_client.hset(link_id, mapping={
            "link_id": link_id,
            "link": link
        })
        redis_client.lpush("linkQueue", link_id)

        logging.debug(f"Link enqueued with ID: {link_id}")
        return jsonify({"link_id": link_id, "status": "Link enqueued for processing"}), 200

    except Exception as e:
        logging.error(f"Error in /apiv1/link: {str(e)}")
        return jsonify({"error": "Internal Server Error", "details": str(e)}), 500

# Endpoint to view the queue of links
@app.route('/apiv1/queue', methods=['GET'])
def get_queue():
    try:
        # Retrieve the list of enqueued link IDs
        queue = redis_client.lrange("linkQueue", 0, -1)

        # Fetch details for each link from Redis
        links = [redis_client.hgetall(link_id) for link_id in queue]

        return jsonify({"queue": links}), 200

    except Exception as e:
        logging.error(f"Error in /apiv1/queue: {str(e)}")
        return jsonify({"error": "Internal Server Error", "details": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)