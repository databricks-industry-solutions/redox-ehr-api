"""
 Class to handle writing to endpoints
"""

class RedoxApiRequest:
    def __init__(self, auth, base_url):
        self.auth = auth
        self.base_url = base_url

    def make_request(self, http_method, resource, action, data=None):
        method = getattr(requests, http_method)
        endpoint = f"{resource}/{action}"
        url = f"{self.base_url}{endpoint}"

        headers = {
            'Authorization': f"Bearer {auth.get_token()['access_token']}",
            'Content-Type': 'application/json'
        }

        response = method(url, headers=headers, data=data)
        print(json.dumps(response.json(), indent=4))
        