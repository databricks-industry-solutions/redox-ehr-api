# Databricks notebook source
# DBTITLE 1,install requirements
pip install axios python-jose

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,environment setup
with open('/Volumes/yamada_dev/redox_fhir/config/private key.pem', 'rb') as f:
  private_key = f.read()

redox_client_id = '40c57cc3-e4b5-4bb5-a19d-a7cc77204fa5'
private_key_kid = 'JKMTAeKln8EIpbuNePD7Lm0HM_yUkojryb1tnm2Z7Gc'
redox_auth_location = 'https://api.redoxengine.com/v2/auth/token'
redox_base_url = 'https://api.redoxengine.com/fhir/R4/redox-fhir-sandbox/Development/'
## All Redox FHIR request URLs start with this base: https://api.redoxengine.com/fhir/R4/[organization-name]/[environment-type]/
auth_json = '{"kid": "JKMTAeKln8EIpbuNePD7Lm0HM_yUkojryb1tnm2Z7Gc", "alg": "RS384"}'
redox_source_id = '91b0ab2f-7b86-441d-9cbf-9b9a6d648d59'

# COMMAND ----------

# DBTITLE 1,auth
import datetime, jwt, requests, json, zoneinfo
from uuid import uuid4

class RedoxApiAuth(requests.auth.AuthBase):
  def __init__(self, 
               client_id, 
               private_key, 
               auth_json,
               auth_location = 'https://api.redoxengine.com/v2/auth/token'):
    self.__client_id = client_id
    self.__private_key = private_key
    self.__auth_json = json.loads(auth_json)
    self.auth_location = auth_location
    self.__token = None
    self.__token_expiry = None

  def get_token(self,
                now = datetime.datetime.now(zoneinfo.ZoneInfo("America/New_York")),
                expiration = datetime.datetime.now(zoneinfo.ZoneInfo("America/New_York")) + datetime.timedelta(minutes=5),
                timeout=30):
    if self.__token is None or now >= self.__token_expiry:
      t = self.generate_token(now, expiration, timeout)
      t.raise_for_status()
      self.__token = json.loads(t.text)
      self.__token_expiry = expiration
    return self.__token
  
  def __call__(self, r):
    r.headers['Authorization'] = 'Bearer %s' % self.__token['access_token']
    return r

  """
    Provide authentication to Redox's API and return valid token
      @param expiration = the datetime when the token expires, default 5 minutes
      @param timeout = seconds to timeout request, default 30 
  """
  def generate_token(self, now = datetime.datetime.now(), expiration = datetime.datetime.now() + datetime.timedelta(minutes=5), timeout=30): 
    return requests.post(self.auth_location, 
        data= {
        'grant_type': 'client_credentials',
        'client_assertion_type': 'urn:ietf:params:oauth:client-assertion-type:jwt-bearer',
        'client_assertion': jwt.encode(
           {
              'iss': self.__client_id,
              'sub': self.__client_id,
              'aud': self.auth_location,
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

  def get_token_expiry(self):
    return self.__token_expiry  

# COMMAND ----------

# DBTITLE 1,test auth
auth = RedoxApiAuth(redox_client_id, private_key, auth_json, redox_auth_location)
auth.get_token()

# COMMAND ----------

# DBTITLE 1,2nd test auth
auth.get_token()

# COMMAND ----------

# DBTITLE 1,test token expiry
auth.get_token_expiry()

# COMMAND ----------

# DBTITLE 1,seed observation bundle for testing
observation_bundle = """{
  "resourceType": "Bundle",
  "type": "message",
  "entry": [
    {
      "resource": {
        "category": [
          {
            "coding": [
              {
                "code": "vital-signs",
                "display": "vital-signs",
                "system": "http://terminology.hl7.org/CodeSystem/observation-category"
              }
            ]
          }
        ],
        "code": {
          "coding": [
            {
              "code": "8302-2",
              "display": "Body Height",
              "system": "http://loinc.org"
            }
          ],
          "text": "Body Height"
        },
        "effectiveDateTime": "2020-12-15T18:06:33-05:00",
        "id": "acbeab78-648d-1ab1-37b9-77d91c528950",
        "issued": "2020-12-15T18:06:33.245-05:00",
        "meta": {
          "lastUpdated": "2024-07-15T15:38:09.805744+00:00",
          "profile": [
            "http://hl7.org/fhir/StructureDefinition/bodyheight",
            "http://hl7.org/fhir/StructureDefinition/vitalsigns"
          ],
          "versionId": "MTcyMTA1Nzg4OTgwNTc0NDAwMA"
        },
        "resourceType": "Observation",
        "status": "final",
        "subject": {
          "reference": "Patient/58117110-ae47-452a-be2c-2d82b3a9e24b"
        },
        "valueQuantity": {
          "code": "cm",
          "system": "http://unitsofmeasure.org",
          "unit": "cm",
          "value": 1224.2024
        }
      }
    }
  ]
}"""

# COMMAND ----------

# DBTITLE 1,endpoint
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
            'Redox-Source-Id': self.source_id
        }

        response = method(url, headers=headers, data=data)
        print(json.dumps(response.json(), indent=4))

# COMMAND ----------

# DBTITLE 1,test endpoint
if __name__ == "__main__":
    client_id = redox_client_id
    key = private_key
    auth_json = auth_json
    base_url = redox_base_url
    auth = RedoxApiAuth(client_id, key, auth_json, redox_auth_location)
    source_id = redox_source_id

    api_request = RedoxApiRequest(auth, base_url, source_id)

    api_request.make_request('post', 'Observation', '$observation-create', data=observation_bundle)
