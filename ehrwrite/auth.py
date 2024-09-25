"""
 Build classes here
"""

import datetime, jwt, requests, json, zoneinfo
from uuid import uuid4

class RedoxApiAuth():
  def __init__(self, 
               client_id, 
               private_key, 
               auth_json,
               auth_location = 'https://api.redoxengine.com/v2/auth/token'):
    self.__client_id = client_id
    self.__private_key = private_key
    self.__auth_json = json.loads(auth_json)
    self.auth_location = auth_location
    self.token = None
    #TODO need to track token expiration time

  def get_token(self,
                now = datetime.datetime.now(zoneinfo.ZoneInfo("America/New_York")),
                expiration = datetime.datetime.now(zoneinfo.ZoneInfo("America/New_York")) + datetime.timedelta(minutes=5),
                timeout=30):
    if self.token is None: #TODO or token is expired
      t = self.generate_token(now, expiration, timeout)
      t.raise_for_status()
      self.token = json.loads(t.text) #TODO error checking needed
    return self.token

  def __call__(self, r):
    r.headers['Authorization'] = 'Bearer %s' % self.token
    return r

  """
    Provide authentication to Redox's API and return valid token
      @param expiration = the datetime when the token expires, default 5 minutes
      @param timeout = seconds to timeout request, default 30 
  """
  def generate_token(self,
                     now = datetime.datetime.now(zoneinfo.ZoneInfo("America/New_York")),
                     expiration = datetime.datetime.now(zoneinfo.ZoneInfo("America/New_York")) + datetime.timedelta(minutes=5),
                     timeout=30): 
    return requests.post(self.auth_location, 
        data= {
        'grant_type': 'client_credentials',
        'client_assertion_type': 'urn:ietf:params:oauth:client-assertion-type:jwt-bearer',
        'client_assertion': jwt.encode(
           {
              'iss': self.__client_id,
              'sub': self.__client_id,
              'aud': self.__auth_location,
              'exp': int(expiration.timestamp()),
              'iat': int(now.timestamp()),
              'jti': uuid4().hex,
          },
          self.__private_key,
          algorithm=self.__auth_json['alg'],
          headers={
            'kid': self.__auth_json['kid'],
            'alg': self.__auth_json['alg'],
            'typ': 'JWT',
          })
      }, timeout=timeout)

  def can_connect(self):
    pass #TODO



