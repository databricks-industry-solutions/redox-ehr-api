"""
 Class to handle writing to endpoints
"""

class RedoxApiRequest:
    def __init__(self, auth, base_url, source_id):
        self.auth = auth
        self.base_url = base_url
        self.source_id = source_id

    def make_request(self, http_method, resource, action, data=None):
        method = getattr(requests, http_method)
        endpoint = f"{resource}/{action}"
        url = f"{self.base_url}{endpoint}"

        headers = {
            'Authorization': f"Bearer {auth.get_token()['access_token']}",
            'Content-Type': 'application/json',
            'Redox-Source-Id': source_id
        }

        response = method(url, headers=headers, data=data)
        print(json.dumps(response.json(), indent=4))
        