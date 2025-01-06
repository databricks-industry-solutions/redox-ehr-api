<img src=https://raw.githubusercontent.com/databricks-industry-solutions/.github/main/profile/solacc_logo.png width="600px">

[![DBR](https://img.shields.io/badge/DBR-CHANGE_ME-red?logo=databricks&style=for-the-badge)](https://docs.databricks.com/release-notes/runtime/CHANGE_ME.html)
[![CLOUD](https://img.shields.io/badge/CLOUD-CHANGE_ME-blue?logo=googlecloud&style=for-the-badge)](https://databricks.com/try-databricks)

## Business Problem
This accelerator uses a Databricks partner, [Redox](https://docs.redoxengine.com/how-to-use-redox/manage-cloud-connectivity/create-a-destination-for-microsoft-azure-databricks/), to write data from Databricks to any EHR system.

## Reference Architecture
![logo](https://github.com/databricks-industry-solutions/redox-ehr-api/blob/main/img/architecture_ref.png?raw=true)

## Getting Started

### Installation 
Install this package via pip 

```python
pip install git+https://github.com/databricks-industry-solutions/redox-ehr-api
```

The python wheel file is available for download under this repo's [releases](https://github.com/databricks-industry-solutions/redox-ehr-api/releases).

### Authentication

Key, Auth Json, and Client ID retrieved from [Redox developer docs](https://docs.redoxengine.com/api-reference/fhir-api-reference/authenticate-an-oauth-api-key/)

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

TODO

### Usage in Spark 

TODO

### Example Usage

Including writebacks, see the demo notebook and an HTML friendly version. TODO


## Authors
- <emma.yamada@databricks.com>
- <aaron.zavora@databricks.com>

## Project support 

Please note the code in this project is provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of these projects. The source in this project is provided subject to the Databricks [License](./LICENSE.md). All included or referenced third party libraries are subject to the licenses set forth below.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support. 

## License

&copy; 2024 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.

| library                                | description             | license    | source                                              |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|
