<img src=https://raw.githubusercontent.com/databricks-industry-solutions/.github/main/profile/solacc_logo.png width="600px">

[![DBR](https://img.shields.io/badge/DBR-CHANGE_ME-red?logo=databricks&style=for-the-badge)](https://docs.databricks.com/release-notes/runtime/CHANGE_ME.html)
[![CLOUD](https://img.shields.io/badge/CLOUD-CHANGE_ME-blue?logo=googlecloud&style=for-the-badge)](https://databricks.com/try-databricks)

## Introduction
Healthcare organizations often face challenges when integrating with various EHR systems due to the complexity and diversity of healthcare data standards. This accelerator uses the [Redox FHIR API](https://docs.redoxengine.com/basics/redox-fhir-api/introduction-to-the-redox-fhir-api/), from Databricks partner [Redox](https://docs.redoxengine.com/how-to-use-redox/manage-cloud-connectivity/create-a-destination-for-microsoft-azure-databricks/), to write data from Databricks to any EHR system, allowing developers to focus on building innovative healthcare solutions rather than wrestling with integration complexities.

## Reference Architecture
![logo](https://github.com/databricks-industry-solutions/redox-ehr-api/blob/main/img/architecture_ref.png?raw=true)

## Getting Started

To use this accelerator, you will need to have a Redox account and API credentials. Visit [Redox's documentation](https://docs.redoxengine.com/) for more information on setting up your account and obtaining the necessary credentials.

### Installation 
Install this package via pip 

```python
pip install git+https://github.com/databricks-industry-solutions/redox-ehr-api
```

The python wheel file is available for download under this repo's [releases](https://github.com/databricks-industry-solutions/redox-ehr-api/releases).

### Authentication

Instructions for creating, managing, and authenticating OAuth API keys are available in the [Redox developer docs](https://docs.redoxengine.com/api-reference/fhir-api-reference/authenticate-an-oauth-api-key/)

```python
from redoxwrite import *
auth = RedoxApiAuth(redox_client_id, private_key, auth_json)
print("Is connection successful? " + str(auth.can_connect()))
```

Troubleshooting connection errors

```python
result = auth.generate_token()
result.json()
"""Output will look something like this
{'error': 'invalid_request',
 'error_description': '...',
 'error_uri': '...'
}
"""
```

### Interfacing with Redox API Endpoints

_RedoxApiRequest()_ takes the auth and FHIR endpoint and provides the interface for all FHIR actions against Redox

> [!NOTE]
> Paged search is not currently included in the RedoxApiRequest class

```python
rapi = RedoxApiRequest(auth, base_url = 'https://api.redoxengine.com/fhir/R4/redox-fhir-sandbox/Development/')
#e.g.
#Search for patients
rapi.make_request("post", resource="Patient", action="_search")

#Get a specific patient
rapi.make_request("get", resource="Patient", action=<patient_id>)

#Update a patient chart
rapi.make_request("post", resource="Observation", action="$observation-create", data=<json FHIR bundle>)
```

### Response Object

```json
{"request":
  {
    "http_method": "The requested HTTP Method",
    "url": "The full URL requested",
    "data": "Any data sent along with the request"
  },
  "response": {
    "response_status_code": "The HTTP response code returned from Redox",
    "response_time_seconds": "The time the request took in seconds",
    "response_headers": "The response headers from Redox",
    "response_text": "The Response data, aka json object",
    "response_url": "The response URL updated"
  }
}
```

### Usage in Spark 

API functions are stateless and can be used directly in Spark RDDs, functions, or UDFs.

```python
df = spark.createDataFrame([
  ('58117110-ae47-452a-be2c-2d82b3a9e24b', 
  float(datetime.now().strftime('%M.%S%f')[:-4]),
  'fea42e82-eee6-449b-8a48-29fa5976169a')],
['PATIENT_MRN', 'OBSERVATION_VALUE', "OBSERVATION_ID"])
df.show()

+--------------------+-----------------+--------------------+
|         PATIENT_MRN|OBSERVATION_VALUE|      OBSERVATION_ID|
+--------------------+-----------------+--------------------+
|58117110-ae47-452...|            58.17|fea42e82-eee6-449...|
+--------------------+-----------------+--------------------+

observation = ( df.
    rdd.
    map(lambda row: (row,
      rapi.make_request("get", resource="Observation", action=row.asDict().get('OBSERVATION_ID')))
    )
)
print(json.dumps(json.loads(observation.take(1)[0][1]['response']['response_text']), indent=2))

"""
{
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
...
"""
```

### Example Usage

Including writebacks, see the [demo notebook](https://github.com/databricks-industry-solutions/redox-ehr-api/blob/main/00_demo_notebook.py) and an [HTML friendly version](https://databricks-industry-solutions.github.io/redox-ehr-api). 


## Authors
- <emma.yamada@databricks.com>
- <aaron.zavora@databricks.com>

## Project support 

Please note the code in this project is provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of these projects. The source in this project is provided subject to the Databricks [License](./LICENSE.md). All included or referenced third party libraries are subject to the licenses set forth below.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support.

Ensure compliance with all relevant regulations and standards when implementing healthcare integrations.

## License

&copy; 2024 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.

| library                                | description             | license    | source                                              |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|
|pyjwt| JWT web token framework in python | MIT | https://github.com/jpadilla/pyjwt |
