"""
 Build classes here
"""

import datetime, jwt, requests, json
from uuid import uuid4

class RedoxApiAuth():
  def __init__(self, 
               client_id, 
               private_key, 
               auth_json,
               auth_location = 'https://api.redoxengine.com/v2/auth/token'):
    self.client_id = client_id
    self.private_key = private_key
    self.auth_json = json.loads(auth_json)
    self.auth_location = auth_location

  """
    Provide authentication to Redox's API and return valid token
      @param expiration = the datetime when the token expires, default 5 minutes
      @param timeout = seconds to timeout request, default 30 
  """
  def new_token(self, expiration = datetime.datetime.now() + datetime.timedelta(minutes=5), timeout=30): 
    return requests.post(self.auth_location, 
        data= {
        'grant_type': 'client_credentials',
        'client_assertion_type': 'urn:ietf:params:oauth:client-assertion-type:jwt-bearer',
        'client_assertion': jwt.encode(
           {
              'iss': self.client_id,
              'sub': self.client_id,
              'aud': self.auth_location,
              'exp': int(expiration.strftime('%s')),
              'iat': int(datetime.datetime.now().strftime('%s')),
              'jti': uuid4().hex,
          },
          self.private_key,
          algorithm=self.auth_json['alg'],
          headers={
            'kid': self.auth_json['kid'],
            'alg': self.auth_json['alg'],
            'typ': 'JWT',
          })
      }, timeout=timeout)

  def can_connect(self):
    pass #TODO
