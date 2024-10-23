"""
 Class to handle writing to endpoints
"""

import requests
import datetime
import json

class RedoxApiRequest:
    def __init__(self, auth, base_url):
        self.auth = auth
        self.base_url = base_url

    def make_request(self, http_method, resource, interaction, data=None):
        method = getattr(requests, http_method)
        endpoint = f"{resource}/{interaction}"
        url = f"{self.base_url}{endpoint}"

        auth_result = self.auth.get_token()

        headers = {
            'Authorization': f"Bearer {auth_result['access_token']}",
            'Content-Type': 'application/json'
        }

        response = method(url, headers=headers, data=data)

        if response.status_code == 200:
            try:
                print(f"{datetime.datetime.now()} - {response.json()}")
            except json.JSONDecodeError:
                print(f"{datetime.datetime.now()} - Failed to decode JSON from response.")
        else:
            print(f"{datetime.datetime.now()} - Request failed with status code: {response.status_code}")
            print(f"{datetime.datetime.now()} - Error message from endpoint: {response.text}")