# Databricks notebook source
# DBTITLE 1,Install Accelerator
pip install git+https://github.com/databricks-industry-solutions/redox-ehr-api

# COMMAND ----------

# DBTITLE 1,Credentials setup
private_key = """
"""

redox_client_id = 'ec962a8d-0cf0-4c77-9164-ebf0d5a8b674'
auth_json = """
{
  "kty": "RSA",
  "kid": "SXdU50UhUmnSbOHaqJrdcFELslYrbJeQaSXXBBcgqtM",
  "alg": "RS384",
  "use": "sig"
}
"""

redox_source_id = '91b0ab2f-7b86-441d-9cbf-9b9a6d648d59'

# COMMAND ----------

# DBTITLE 1,Check Redox API Connection
from redoxwrite import *
import json
auth = RedoxApiAuth(redox_client_id, private_key, auth_json, redox_source_id)
print("Is connection successful? " + str(auth.can_connect()))

# COMMAND ----------

# DBTITLE 1,Create API Endpoint
#All Redox FHIR request URLs start with this base: https://api.redoxengine.com/fhir/R4/[organization-name]/[environment-type]/
redox_base_url = 'https://api.redoxengine.com/fhir/R4/redox-fhir-sandbox/Development/'

rapi = RedoxApiRequest(auth, base_url = redox_base_url)

# COMMAND ----------

# DBTITLE 1,Search for Observations
result = rapi.make_request("post", resource="Observation", action="_search")
if result['response']['response_status_code'] != 200:
  print("Error from API " + str(result['response']['response_text']) )

# COMMAND ----------

# DBTITLE 1,Take one random observation
data = json.loads(result['response']['response_text'])
observation = data['entry'][3]
print(json.dumps(observation, indent=4))

# COMMAND ----------

# DBTITLE 1,Update the observation
import random
# Generate a random integer between 1 and 10 (inclusive)
val = random.randint(1, 10)
observation['resource']['valueQuantity']['value'] = val
del observation['search']
print("New Value: " + str(val) + "\n" + json.dumps(observation, indent=4))

# COMMAND ----------

# DBTITLE 1,Cast a single resource to Bundle
observation_bundle = {
  "resourceType": "Bundle",
  "entry":[observation]
}

# COMMAND ----------

print(json.dumps(observation_bundle, indent=4))

# COMMAND ----------

# DBTITLE 1,Update the observation
response = rapi.make_request("post", resource="Observation", action="$observation-update", data=json.dumps(observation_bundle))
print(response['response'])

# COMMAND ----------

# DBTITLE 1,Confirm the observation has been updated
response = rapi.make_request("get", resource="Observation", action=observation['fullUrl'].split('/')[-1])
data = json.loads(response['response']['response_text'])

# COMMAND ----------

data

# COMMAND ----------

observation_bundle

# COMMAND ----------

url = "https://fhir.redoxengine.com/fhir-sandbox/Observation/368fe92e-5e4d-43ff-8238-5a9bec59b548"

response = rapi.make_request("get", resource="Observation", action="368fe92e-5e4d-43ff-8238-5a9bec59b548")

# COMMAND ----------

response
