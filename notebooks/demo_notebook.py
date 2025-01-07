# Databricks notebook source
# DBTITLE 1,Install Accelerator
pip install git+https://github.com/databricks-industry-solutions/redox-ehr-api

# COMMAND ----------

# MAGIC %md # Connect to Redox

# COMMAND ----------

# DBTITLE 1,Credentials setup
private_key = """REDACTED"""

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

# MAGIC %md # Interact with EHR

# COMMAND ----------

# DBTITLE 1,Search for Observations
result = rapi.make_request("post", resource="Observation", action="_search")
if result['response']['response_status_code'] != 200:
  print("Error from API " + str(result['response']['response_text']) )

# COMMAND ----------

# DBTITLE 1,Sample Search Results
data = json.loads(result['response']['response_text'])
print("Number of observations found " + str(len(data['entry'])))
observation = data['entry'][3]
print(json.dumps(observation, indent=2))


# COMMAND ----------

# DBTITLE 1,Update a random observation
import random
from datetime import datetime
"""
Following Redox specs for observation-update
https://docs.redoxengine.com/api-reference/fhir-api-reference/fhir-resources/Observation/$observation-update/
"""
# Set value to the datetime minute.seconds
observation['resource']['valueQuantity']['value'] = float(datetime.now().strftime('%M.%S%f')[:-4])
del observation['search']
del observation['resource']['meta']
print(json.dumps(observation, indent=2))

# COMMAND ----------

# DBTITLE 1,Wrap resources in a Bundle
#Wrap the observation & patient info in a bundle
patient_id = observation['resource']['subject']['reference'].split('/')[1]
observation_bundle = {
  "resourceType": "Bundle",
  "type": "message",
  "entry":[{"resource": {"resourceType": "Patient", "id": patient_id}},
          observation]
}

# COMMAND ----------

print(json.dumps(observation_bundle, indent=2))

# COMMAND ----------

# DBTITLE 1,Push update to the EHR through Redox
response = rapi.make_request("post", resource="Observation", action="$observation-update", data=json.dumps(observation_bundle))
print(json.dumps(json.loads(response['response']['response_text']), indent=2))

# COMMAND ----------

# DBTITLE 1,Confirm the observation has been updated
response = rapi.make_request("get", resource="Observation", action=observation['fullUrl'].split('/')[-1])
print(json.dumps(json.loads(response['response']['response_text']), indent=2))

# COMMAND ----------

# MAGIC %md # Running on Spark

# COMMAND ----------

# DBTITLE 1,Sample Dataframe with Observation
df = spark.createDataFrame([
  ('58117110-ae47-452a-be2c-2d82b3a9e24b', 
  float(datetime.now().strftime('%M.%S%f')[:-4]),
  'fea42e82-eee6-449b-8a48-29fa5976169a')],
['PATIENT_MRN', 'OBSERVATION_VALUE', "OBSERVATION_ID"])
df.show()

# COMMAND ----------

#create a tuple of (row, response)
observation = ( df.
    rdd.
    map(lambda row: (row,
      rapi.make_request("get", resource="Observation", action=row.asDict().get('OBSERVATION_ID')))
    )
)
print(json.dumps(json.loads(observation.take(1)[0][1]['response']['response_text']), indent=2))

# COMMAND ----------

# DBTITLE 1,Update Observation
def updateObservation(data, value):
  observation = json.loads(data['response']['response_text'])
  # Set value to the datetime minute.seconds
  observation['valueQuantity']['value'] = value
  del observation['meta']
  patient_id = observation['subject']['reference'].split('/')[1]
  observation_bundle = {
    "resourceType": "Bundle",
    "type": "message",
    "entry":[{"resource": {"resourceType": "Patient", "id": patient_id}},
           {"resource": observation}]
  }
  return rapi.make_request("post", resource="Observation", action="$observation-update", data=json.dumps(observation_bundle))


#Update the observation with the value from OBSERVATION_VALUE in our table
result =(observation.
    map(lambda data: updateObservation(data[1], data[0].asDict().get('OBSERVATION_VALUE')))
).take(1)[0]['response']['response_text']

print(json.dumps(json.loads(result), indent=2))

# COMMAND ----------

# DBTITLE 1,Confirm Observation Updated
response = rapi.make_request("get", resource="Observation", action='fea42e82-eee6-449b-8a48-29fa5976169a')
print(json.dumps(json.loads(response['response']['response_text']), indent=2))
