#!/usr/bin/env python3

import requests
import json, jsonpickle
import os
import sys

# Use localhost & port 5000 if not specified by environment variable REST
REST = os.getenv("REST") or "localhost:5000"

def mkReq(reqmethod, endpoint, data, verbose=True):
    try:
        url = f"http://{REST}/{endpoint}"
        jsonData = jsonpickle.encode(data)
        if verbose and data is not None:
            print(f"Sending request to {url} with payload: {json.dumps(data, indent=2)}")
        response = reqmethod(url, data=jsonData, headers={'Content-type': 'application/json'})
        if response.status_code == 200:
            jsonResponse = json.dumps(response.json(), indent=4, sort_keys=True)
            print(f"Response: {jsonResponse}")
            return response.json()
        else:
            print(f"Error {response.status_code}: {response.text}")
    except Exception as e:
        print(f"Exception during request to {endpoint}: {str(e)}")

# List of test Amazon links
amazon_links = [
    "https://www.amazon.com/dp/example1",
    "https://www.amazon.com/dp/example2",
    "https://www.amazon.com/dp/example3"
]

# Test each link by sending it to the /apiv1/link endpoint
for link in amazon_links:
    print(f"Processing link: {link}")
    mkReq(requests.post, "apiv1/link",
          data={
              "link": link,
              "callback": {
                  "url": "http://localhost:5000/callback",
                  "data": {"link": link, "status": "to be updated"}
              }
          },
          verbose=True
          )

# Fetch the current queue from the server
print("Fetching queue from server...")
mkReq(requests.get, "apiv1/queue", data=None)

sys.exit(0)
