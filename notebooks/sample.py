# Databricks notebook source
# MAGIC %md # Auth

# COMMAND ----------

# DBTITLE 1,Install requirements
# MAGIC %pip install axios python-jose

# COMMAND ----------

# DBTITLE 1,Authentication handler
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

  def get_token(self, 
        now = datetime.datetime.now(zoneinfo.ZoneInfo("America/New_York")),
        expiration = datetime.datetime.now(zoneinfo.ZoneInfo("America/New_York")) + datetime.timedelta(minutes=5), timeout=30):
    if self.__token is None: #or token is expired
      t = self.generate_token(now, expiration, timeout)
      t.raise_for_status()
      self.__token = json.loads(t.text)
    return self.__token

  def __call__(self, r):
    r.headers['Authorization'] = 'Bearer %s' % self.get_token()['access_token']
    r.headers['Redox-Source-Id'] = '91b0ab2f-7b86-441d-9cbf-9b9a6d648d59'
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
    

# COMMAND ----------

# DBTITLE 1,Auth params
key = """-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDnKSps27qL1baP
XB6AUDTFLEq59LTe6Jc/9iJ6v5kG1/7HQqrPOxfDZjg8VhNdi/MpfMWIu8g6/6QF
iT+hIIE3gOK02Fe36SR/fselr5Rn0oKi0Hkr3TQuC7X69BqNTKC7X1tqrmt3Y/Ew
2PV9x0hcE1nxA7CGQQJJ3PZerTU46800hDtz/21s5cVGCIpW0zDNYK7ms0NdizNe
QZJgwRP/Xh+waSJCXM+1Y/3L1FwC+GwGcrGExPnAj1isJi+YF3MV+3m/mNVsgBR7
qAxRrU6MzV3NuVDNsm5iQvNb/aUbIAKmo9fgT+9OQsQgXWYUhCjAH5RZthskhV5+
69pEOb3LAgMBAAECggEAbPPssLFiPC2HZguzvqPPM44cTIiHxbbYL0gKWVO8Q0sE
JZQJITLHkQw5fkrNxt5DRq8uBbQIfLJoc0mM6ZuB/BCuCjIcKi1jzAoL/M6Egujo
NOhQ7HlO8lMHu5hZeXcAHUClPGOoeHXtRSpFjzqiIu/mCSDFj4nNk9Xnc5ltB/3K
3oM9HrPY0gcTT9PldR/XCAoZkOccuwyBB800TgDn48IqyAcviRup0nP8wsFyNXYj
Gq5ZPfhgzYC8XMP3yl6HCj5DZOvp8ymOgLAGjTGInPxXMjJUK9SYk0Xhvp1ICWYA
12o+z4cgjW1J7Xbps+huO5VnZa/M61W3+4jdGLErCQKBgQD72IW7U/A+7w/GfbKZ
gvMdNjU0icS7O8CrVpcYS8ApT9cHUed52PHSUS8e5kcLYWOIIMJ0J595BreAc7xW
EDaU1BIbfGO4Ukpd1dxi0R/A648FQhXnlhU5m9chulrsrNaiCT4VXkDiHKb+/f63
W9kvcH7pD/ZXVSSoNDvyTWbVQwKBgQDq+UvQgMjC+jvWvNbNZu+31MVA7gznnLcR
OG6xf+IIqznZqM0Nx3n5cQERNn7OzF4d7KVyG5DQCQGLeRVEFfu7tEYeKRc2i3F9
l/8X0AFpU2o+ayFGpfg5vp1WUOS2oHOAVhk3vgQYO4BE0s1HeuDmcKcQKgBz8ggA
yGUSdEmo2QKBgFnmBobCBcZRS85M3E4UAnhw+zkP3JwBTltbA4B2udeFWnqQcBJ8
sIWk2S8sgDu9veu/1enkovZuwV4m8hm+m90wfm41Ttti9IzeZFcZ0z3oWBtGZc9P
MbBowriVbxOA9vTMW88A4CRWmf336M7HCJX4kDTdr8+o4vbv/l4V5xlFAoGBAN9K
7tACQkZ/U7QaGipxgqZ4TMcKMTs8wv9bWF3XQC6qwLvl3I8WnAP+blw0JEdTELFx
PDv2lCQOGui2tQ3RsjpK80MgG04bL0uB0MmJQn4b5d/RIHspp30AdjYbg21Fht1T
dH1zViLNEHTWunsN+4EhVrFWqL2GKDLOPLZPydvpAoGBAOoDRvkiEqfZYOvCaOZd
AZ9hBLcXr5IqBOzE5VXx1xXg2pZVMu80sLdkFzRN9xv7MZXnhcqAijvAJ2lxasMT
QtmSYu5+YwX7xuqogRsNC5KLtZVV9I+pGsMs046hYn3mT7DGmNHISKL9vyRJdQQC
yzmNP6QzIb4fQTufeOapkowB
-----END PRIVATE KEY-----
"""
client_id = 'ec962a8d-0cf0-4c77-9164-ebf0d5a8b674'
redox_auth_location = 'https://api.redoxengine.com/v2/auth/token'
## All Redox FHIR request URLs start with this base: https://api.redoxengine.com/fhir/R4/[organization-name]/[environment-type]/
auth_json =  """
{
  "kty": "RSA",
  "kid": "SXdU50UhUmnSbOHaqJrdcFELslYrbJeQaSXXBBcgqtM",
  "alg": "RS384",
  "use": "sig"
}
"""

# COMMAND ----------

# DBTITLE 1,Authenticate and get token
auth = RedoxApiAuth(client_id, key, auth_json)
auth_result = auth.get_token(now = datetime.datetime.now(zoneinfo.ZoneInfo("America/New_York")),
  expiration = (datetime.datetime.now(zoneinfo.ZoneInfo("America/New_York")) + datetime.timedelta(minutes=5)))
auth_result

# COMMAND ----------

# MAGIC %md # Sample Observation Data 

# COMMAND ----------

# DBTITLE 1,Install dbignite to write FHIR data
# MAGIC %pip install git+https://github.com/databricks-industry-solutions/dbignite-forked.git

# COMMAND ----------

# DBTITLE 1,Create FHIR from table
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
    #hardcoded metadata
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
result_rdd = b.df_to_fhir(data)
observation_bundle = '\n'.join([str(x) for x in result_rdd.map(lambda x: json.loads(x)).map(lambda x: json.dumps(x, indent=4)).take(10)])
#Pretty printing the resulting RDD
print(observation_bundle)

# COMMAND ----------

XXXobservation_bundle = """{
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

# DBTITLE 1,authenticate
auth = RedoxApiAuth(client_id, key, auth_json)
auth_result = auth.get_token(
  now = datetime.datetime.now(zoneinfo.ZoneInfo("America/New_York")),
  expiration = datetime.datetime.now(zoneinfo.ZoneInfo("America/New_York")) + datetime.timedelta(minutes=5))
auth_result

# COMMAND ----------

# DBTITLE 1,endpoint to post
#TODO make this a class
from pyspark.sql.functions import pandas_udf, col
import pandas as pd
from typing import Iterator

base_url = 'https://api.redoxengine.com/fhir/R4/'
org = 'redox-fhir-sandbox'
env = 'Development'
endpoint = 'Observation'
action = '$observation-create'
http_method = 'post'

url = base_url + org + '/' + env + '/' + endpoint + '/' + action
print(url)

"""
postToRedoxUDF does not work currently
"""
@pandas_udf(returnType=MapType(StringType(), StringType()))
def postToRedoxUDF(data_iter: Iterator[pd.Series]) -> Iterator[pd.Series]: 
  auth = RedoxApiAuth(client_id, key, auth_json)
  auth.generate_token()
  for row in data_iter:
    response = requests.post(url, auth=auth, data=data)
    yield {
      'response_status_code': response.status_code, 
      'response_time_seconds': (response.elapsed.microseconds / 1000000),
      'response_headers': response.headers,
      'response_text': response.text,
      'response_url': response.url
    }

def postToRedox(url, auth, partition):
  auth.get_token() #generate a new token for the partition 
  return map(lambda row: {'request_payload': row, 'response': postToRedoxRow(url, auth, row)}, partition)

def postToRedoxRow(url, auth, row):
  response = requests.post(url, auth=auth, data=row)
  return {
    'response_status_code': response.status_code, 
    'response_time_seconds': (response.elapsed.microseconds / 1000000),
    'response_headers': response.headers,
    'response_text': response.text,
    'response_url': response.url
  }
          

#https://api.redoxengine.com/fhir/R4//redox-fhir-sandbox/Development/Observation/$observation-create

# COMMAND ----------


response = requests.post(url, auth=auth, data=observation_bundle)
response
response.text

# COMMAND ----------

# DBTITLE 1,RDD Approach
"""
response = (
  result_rdd
  .map(lambda x: json.loads(x))
  .map(lambda x: json.dumps(x, indent=4))
  .map(lambda x: requests.post(url, auth=auth, data=x))
)
"""

response = (
  result_rdd
  .map(lambda x: json.loads(x))
  .map(lambda x: json.dumps(x, indent=4))
  .mapPartitions(lambda partition: postToRedox(url, auth, partition))
)

# COMMAND ----------

response.take(1)

# COMMAND ----------

# DBTITLE 1,Pandas UDF

(
  result_rdd
  .map(lambda x: json.loads(x))
  .map(lambda x: Row(**{"fhir_msg": json.dumps(x, indent=4)}))
  .toDF().select(col("fhir_msg"), postToRedoxUDF(col("fhir_msg")))
).show()

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

# DBTITLE 1,test 2 - get patient
redox_fhir_api_action(redox_client_id, private_key_kid, redox_auth_location, private_key, redox_base_url, 'get', 'Patient','58117110-ae47-452a-be2c-2d82b3a9e24b')
