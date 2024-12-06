import os
import json
import uuid
import logging
import redis
from flask import Flask, request, jsonify, render_template_string
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

logging.basicConfig(level=logging.DEBUG)

# Initialize Redis connection
try:
    redis_client = redis.StrictRedis(host='redis', port=6379, db=0, decode_responses=True)
except Exception as e:
    logging.error(f"Failed to connect to Redis: {str(e)}")

@app.route('/', methods=['GET', 'POST'])
def home():
    if request.method == 'POST':
        # Get the link submitted by the user
        link = request.form.get('link')
        if link and link.startswith("https://a.co/"):
            try:
                # Generate a unique ID and store the link in Redis
                link_id = str(uuid.uuid4())
                redis_client.hset(link_id, mapping={"link_id": link_id, "link": link})
                redis_client.lpush("linkQueue", link_id)
                logging.debug(f"Link enqueued with ID: {link_id}")
                return jsonify({"link_id": link_id, "status": "Link enqueued for processing"}), 200
            except Exception as e:
                logging.error(f"Error in /: {str(e)}")
                return jsonify({"error": "Internal Server Error", "details": str(e)}), 500
        else:
            return "Invalid or missing Amazon link. Please try again.", 400

    # Render the HTML form
    html_content = '''
    <!doctype html>
    <html lang="en">
    <head>
        <title>Link Submission</title>
    </head>
    <body>
        <h1>Enter Amazon Link</h1>
        <form method="post">
            <input type="text" name="link" placeholder="https://www.amazon.com/dp/example1" required>
            <button type="submit">Submit</button>
        </form>
    </body>
    </html>
    '''
    return render_template_string(html_content)

@app.route('/apiv1/link', methods=['POST'])
def add_link():
    try:
        data = request.get_json()
        logging.debug(f"Received data: {data}")
        link = data.get("link")
        if not link or not link.startswith("https://www.amazon."):
            return jsonify({"error": "Invalid or missing Amazon link"}), 400
        link_id = str(uuid.uuid4())
        redis_client.hset(link_id, mapping={"link_id": link_id, "link": link})
        redis_client.lpush("linkQueue", link_id)
        logging.debug(f"Link enqueued with ID: {link_id}")
        return jsonify({"link_id": link_id, "status": "Link enqueued for processing"}), 200
    except Exception as e:
        logging.error(f"Error in /apiv1/link: {str(e)}")
        return jsonify({"error": "Internal Server Error", "details": str(e)}), 500

@app.route('/apiv1/queue', methods=['GET'])
def get_queue():
    try:
        queue = redis_client.lrange("linkQueue", 0, -1)
        links = [redis_client.hgetall(link_id) for link_id in queue]
        return jsonify({"queue": links}), 200
    except Exception as e:
        logging.error(f"Error in /apiv1/queue: {str(e)}")
        return jsonify({"error": "Internal Server Error", "details": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
