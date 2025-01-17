# Databricks notebook source
# DBTITLE 1,Install Accelerator
pip install git+https://github.com/databricks-industry-solutions/redox-ehr-api

# COMMAND ----------

# MAGIC %md # Connect to Redox

# COMMAND ----------

# DBTITLE 1,Credentials setup
private_key = """REDACTED"""

redox_client_id = 'REDACTED'
auth_json = """
{
  "kty": "RSA",
  "kid": "SXdU50UhUmnSbOHaqJrdcFELslYrbJeQaSXXBBcgqtM",
  "alg": "RS384",
  "use": "sig"
}
"""

redox_source_id = 'REDACTED'

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

# DBTITLE 1,Create an observation
#creating an observation for remaining length of 4 day stay at a hospital 
observation = """
{
   "resourceType":"Bundle",
   "entry":[
      {
         "resource":{
            "category":[
               {
                  "coding":[
                     {
                        "code":"survey",
                        "display":"Survey",
                        "system":"http://terminology.hl7.org/CodeSystem/observation-category"
                     }
                  ]
               }
            ],
            "code":{
               "coding":[
                  {
                     "code":"78033-8",
                     "display":"Remaining Hospital Stay",
                     "system":"http://loinc.org"
                  }
               ],
               "text":"Remaining Hospital Stay"
            },
            "effectiveDateTime":"2024-01-28T18:06:33.245-05:00",
            "issued":"2024-01-28T18:06:33.245-05:00",
            "resourceType":"Observation",
            "status":"final",
            "valueQuantity":{
               "code":"days",
               "system":"https://www.nubc.org/CodeSystem/RevenueCodes",
               "unit":"days",
               "value":4
            },
            "subject": {
              "reference": "Patient/58117110-ae47-452a-be2c-2d82b3a9e24b"
            },
            "identifier": [
            {
              "system": "urn:databricks",
              "value": "1234567890"
            }
          ]
         }
      },
      {
         "resource":{
           "resourceType": "Patient",
           "identifier": [
            {
              "system": "urn:redox:health-one:MR",
              "value": "0000991458"
            },
            {
              "system": "http://hl7.org/fhir/sid/us-ssn",
              "value": "547-01-9991"
            }
          ]
         }
      }
   ]
}
"""

# COMMAND ----------

print(json.loads(json.dumps(observation, indent=2)))

# COMMAND ----------

# DBTITLE 1,Post Observation to EHR
result = rapi.make_request("post", resource="Observation", action="$observation-create", data=observation)

# COMMAND ----------

# DBTITLE 1,Confirm Post Succeded
if result['response']['response_status_code'] != 200:
  print("Failed to update the patient information")
print(json.dumps(json.loads(result['response']['response_text']), indent=2))

# COMMAND ----------

# DBTITLE 1,Confirm Data Created
observation_id = json.loads(result['response']['response_text'])['entry'][0]['response']['location'].split('/')[-3]
response = rapi.make_request("get", resource="Observation", action=observation_id)
data = json.loads(response['response']['response_text'])

assert data['valueQuantity']['value'] == 4
print(json.dumps(data, indent=2))

# COMMAND ----------

# MAGIC %md # Running on Spark

# COMMAND ----------

# DBTITLE 1,Dataframe with Observation
df = spark.createDataFrame([
  ('58117110-ae47-452a-be2c-2d82b3a9e24b', 
  3,
  observation_id)],
['PATIENT_MRN', 'LENGTH_OF_STAY', "OBSERVATION_ID"])
df.show()

# COMMAND ----------

# DBTITLE 1,Call the API from the DataFrame
#create a tuple of (row, get response)
observation_rdd = ( df.
    rdd.
    map(lambda row: (row,
      rapi.make_request("get", resource="Observation", action=row.asDict().get('OBSERVATION_ID')))
    )
)
print(json.dumps(json.loads(observation_rdd.take(1)[0][1]['response']['response_text']), indent=2))

# COMMAND ----------

# DBTITLE 1,Update The Observation via DataFrame
def update_observation(value, data):
  data['valueQuantity']['value'] = value
  return json.dumps(
    {
      "resourceType": "Bundle", 
      "entry": [{'resource': data}]
    })

#Create a tuple of (row, post response) 
updated_rdd = ( observation_rdd.
    map(lambda row_response_tuple:
       (
         row_response_tuple[0],  #the row from the DataFrame
         rapi.make_request("post", 
                           resource="Observation", 
                           action ="$observation-update",
                           data = update_observation(
                            row_response_tuple[0].asDict().get('LENGTH_OF_STAY'), #value from DF
                            json.loads(row_response_tuple[1]['response']['response_text']))
          ) #the API payload to use
       )
    )
) 

# COMMAND ----------

# DBTITLE 1,Confirm the Post Succeded
updated_rdd.toDF(['row', 'response']).select("response.response.response_status_code", "response.response.response_text").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Confirm Observation Updated
observation_id = json.loads(result['response']['response_text'])['entry'][0]['response']['location'].split('/')[-3]
response = rapi.make_request("get", resource="Observation", action=observation_id)
data = json.loads(response['response']['response_text'])

assert data['valueQuantity']['value'] == 3 #now has been updated to 3 instead of 4
print(json.dumps(data, indent=2))
