# Databricks notebook source
# MAGIC %md # Auth

# COMMAND ----------

# DBTITLE 1,install requirements
# MAGIC %pip install axios python-jose

# COMMAND ----------

# DBTITLE 1,authentication handler
import datetime, jwt, requests, json, zoneinfo
from uuid import uuid4

class RedoxApiAuth(requests.auth.AuthBase):
  def __init__(self, 
               client_id, 
               private_key, 
               auth_json,
               auth_location = 'https://api.redoxengine.com/v2/auth/token'):
    self.client_id = client_id
    self.private_key = private_key
    self.auth_json = json.loads(auth_json)
    self.auth_location = auth_location
    self.token = None

  def get_token(self, 
        now = datetime.datetime.now(zoneinfo.ZoneInfo("America/New_York")),
        expiration = datetime.datetime.now(zoneinfo.ZoneInfo("America/New_York")) + datetime.timedelta(minutes=5), timeout=30):
    if self.token is None: #or token is expired
      t = self.generate_token(now, expiration, timeout)
      t.raise_for_status()
      self.token = json.loads(t.text)
    return self.token

  def __call__(self, r):
    r.headers['Authorization'] = 'Bearer %s' % self.token['access_token']
    return r

  """
    Provide authentication to Redox's API and return valid token
    Responsible for calling this API before the request is made 
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
              'iss': self.client_id,
              'sub': self.client_id,
              'aud': self.auth_location,
              'exp': int(expiration.timestamp()),
              'iat': int(now.timestamp()),
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
    

# COMMAND ----------

# DBTITLE 1,auth parameters
key = """<private key here>"""
client_id = '<client id here>'
redox_auth_location = 'https://api.redoxengine.com/v2/auth/token'
## All Redox FHIR request URLs start with this base: https://api.redoxengine.com/fhir/R4/[organization-name]/[environment-type]/
auth_json = """<auth json here>"""

auth = RedoxApiAuth(client_id, key, auth_json)

# COMMAND ----------

auth_result = auth.get_token(now = datetime.datetime.now(zoneinfo.ZoneInfo("America/New_York")),
  expiration = (datetime.datetime.now(zoneinfo.ZoneInfo("America/New_York")) + datetime.timedelta(minutes=5)))
auth_result

# COMMAND ----------

# MAGIC %md # Sample Observation Data 

# COMMAND ----------

# MAGIC %pip install git+https://github.com/databricks-industry-solutions/dbignite-forked.git

# COMMAND ----------

from dbignite.writer.bundler import *
from dbignite.writer.fhir_encoder import *
import json

# Create a dummy Dataframe with 1 row of data
data = spark.createDataFrame([('58117110-ae47-452a-be2c-2d82b3a9e24b', 
                              'acbeab78-648d-1ab1-37b9-77d91c528950', 
                              'final',
                              'cm',
                              '404.2')],
                             ['PATIENT_MRN', 'OUTCOME_ID', 'STATE', 'CODE', 'VALUE'])

# Define a mapping from DF columns to FHIR Schema, including a hardcoded value for Patient.identifier.system
maps = [Mapping('PATIENT_MRN', 'Patient.id'), 
    Mapping('OUTCOME_ID', 'Observation.id'),
		Mapping('STATE', 'Observation.status'),
    Mapping('CODE', 'Observation.valueQuantity.code'),
    Mapping('CODE', 'Observation.valueQuantity.unit'),
    Mapping('VALUE', 'Observation.valueQuantity.value'),
    Mapping('http://unitsofmeasure.org', 'Observation.valueQuantity.system', True),
    Mapping('vital-signs', 'Observation.category.coding.code', True),
    Mapping('vital-signs', 'Observation.category.coding.display', True),
    Mapping('http://terminology.hl7.org/CodeSystem/observation-category', 'Observation.category.coding.system', True),
    Mapping('8302-2', 'Observation.code.coding.code', True),
    Mapping('Body Height', 'Observation.code.coding.display', True),
    Mapping('http://loinc.org', 'Observation.code.coding.system', True)
    ]

# Instance of the encoder & bundle writer
m = MappingManager(maps, data.schema)
b = Bundle(m)
result = b.df_to_fhir(data)
observation_bundle = '\n'.join([str(x) for x in result.map(lambda x: json.loads(x)).map(lambda x: json.dumps(x, indent=4)).take(10)])
#Pretty printing the resulting RDD
print(observation_bundle)

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
          "value": 404.2
        }
      }
    }
  ]
}"""

# COMMAND ----------

# MAGIC %md # Post Sample to EHR

# COMMAND ----------

auth = RedoxApiAuth(client_id, key, auth_json)
auth_result = auth.get_token(
  now = datetime.datetime.now(zoneinfo.ZoneInfo("America/New_York")),
  expiration = datetime.datetime.now(zoneinfo.ZoneInfo("America/New_York")) + datetime.timedelta(minutes=5))
auth_result

# COMMAND ----------

#TODO make this a class
base_url = 'https://api.redoxengine.com/fhir/R4/'
org = 'redox-fhir-sandbox'
env = 'Development'
endpoint = 'Observation'
action = '$observation-create'
http_method = 'post'

url = base_url + org + '/' + env + '/' + endpoint + '/' + action
print(url)
#https://api.redoxengine.com/fhir/R4//redox-fhir-sandbox/Development/Observation/$observation-create

# COMMAND ----------

response = requests.post(url, auth=auth, data=observation_bundle)

# COMMAND ----------

response

# COMMAND ----------

response.text

# COMMAND ----------

response = (
  result
  .map(lambda x: json.loads(x))
  .map(lambda x: json.dumps(x, indent=4))
  .map(lambda x: requests.post(url, auth=auth, data=x).text)
)

# COMMAND ----------

response.take(1)

# COMMAND ----------

# MAGIC %md # Confirm Post Updated EHR

# COMMAND ----------

url = base_url + org + '/' + env + '/' + 'Patient' + '/58117110-ae47-452a-be2c-2d82b3a9e24b'
response = requests.get(url, auth=auth)
 

# COMMAND ----------

response

# COMMAND ----------

response.text

# COMMAND ----------

# DBTITLE 1,fhir api action execution
def redox_fhir_api_action(client_id, kid, auth_location, private_key, base_url, http_method, resource, interaction, bundle=None):
    auth = BackendServiceAuth(auth_location, client_id, kid, private_key)

    method = getattr(requests, http_method)

    endpoint = resource+'/'+interaction

    response = method(base_url+endpoint, auth=auth, data=bundle)
    
    if response.status_code == 200:
        try:
            print(f"{datetime.datetime.now()} - {response.json()}")
        except JSONDecodeError:
            print(f"{datetime.datetime.now()} - Failed to decode JSON from response.")
    else:
        print(f"{datetime.datetime.now()} - Request failed with status code: {response.status_code}")
        print(f"{datetime.datetime.now()} - Error message from endpoint: {response.text}")

# COMMAND ----------

# DBTITLE 1,test 1 - patient search
redox_fhir_api_action(redox_client_id, private_key_kid, redox_auth_location, private_key, redox_base_url, 'post', 'Patient','_search')

# COMMAND ----------

# DBTITLE 1,test 2 - get patient
redox_fhir_api_action(redox_client_id, private_key_kid, redox_auth_location, private_key, redox_base_url, 'get', 'Patient','58117110-ae47-452a-be2c-2d82b3a9e24b')
