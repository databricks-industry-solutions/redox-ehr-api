# Databricks notebook source
# DBTITLE 1,Credentials setup

private_key = """
-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDHJez9j64sN9j1
OQnSZ8eIf7He3J0JlMThPSnbwdiq9zQwUUndxAQ2nuNrA1cy+J7WUNnJwZmaJFKq
gH840YQy84z4lFN8pkuWQSHLsewP5oebnD64rDnxYkwoR1uD0vAu1AXKIWAe3N2a
9+LRQuh7xF2HKMuibHsjLa8vbLJuwJpYkOPCZ/bxhAmTyq3UIqsjw2ksbuF0V1eq
C+tBBkdXoypnlRzlbfdYC+u+ltqam0hVUjwIKDCEihtg79ao6SsZW71hK7jsYmHi
vRN5WBhm6HtbjYK534oj8/kG74e7doQL4oRvNnMBUMkb7WGOaB2C8nKeyd0kohww
gc2S12f5AgMBAAECggEAJlv0R6z2yBajyCxJ64jI4s5x5PMKno9U0uvUlbtDcD74
gvwNZdV9WEYHmRPGJo/EDJT7NkT/wLSRZb0lhDy7IZNPCoyLfj2L3q/CAjnNtgxZ
/4u7exfVe1zLPZDtHDmzwNlfGh2OpbM2TkTEIDmqjTh4KXIeszUBDPgeP9zIi9Nf
inrVaOjl1zwiuJidbxwsia7fVl68HdrL7Krkqci02w8BQWhL6F/S4b8NfxrtDAxk
v0zm9J1in0pjvKtbVzXA2qXfOj78g5RVideCSTpymLNJ5RgEUq2BZOytS3+SzzCb
GaJhRuexIU7w758OWRxgTig2i3QQ9cEa4vPelWcNuQKBgQDkJZ9sRFPEuBsJj2+K
JBE8GN7S+A9kywU7Zb7tgoaGT1GH7viO0Odpi9N19xEeU/qwMABz73nZ5sZiQ+q9
Db+z1K+TObOzVq9/ZN6c4hhjzTSFomGVPKNnZwHZulu37ij71GJEDd+RtDOCi2J2
Izc+5aOE4ZhrTCvb+C6G29DLtQKBgQDfdf2BRcGPzUzFjmcfwlPls5x2N/s27ZVx
e4k5ou53xu4V7144Gee39SqdQrkHmukjg368hDJdNracm/ApPYdrjwsjANJXkFkY
ILQQ6f7Dc8WxRZRWzrtrXGICvemt+vbUrSIn4GwfTmP5w62MNmsyDeu3cnNGL+gJ
e+6y2cB9tQKBgQCOVB5J17J+tfBAHZiTEH8kA8v2x0QrODCSZp4e49/yqEcPy3iK
+C51/QI1xKWMSw3InpmZuhtFYh//K6mkuZAPqy7BZS0DQ6AGlLIAI1jd4iXS/INu
K78xAeT4pLcVXuF4gX2wQQtphYbg+P26/6s2dOJ3QpnozkNKXmEARt/SRQKBgEyo
G0jHdzkvglCbI0E/1qwLy3a6iZE0O3nsmQyOmiO4uGAJ91ZjfJwcnHvKMdMsDyJB
r65X4zca19YtoFtlYhlBvt5JH98uA4JFZcAPpXfDNWQ0rEiDLsQLswuhvpISb65R
nk/zquOqbp11xQk+edN39w69UlIXiRAH1cDA9kmpAoGAEven0O3LKXJWcaX4TOrb
WdlYjtj2VK40hLEjMTjnEnIZfEuq4FZsV+b44eOyZs+m9OnNxESjCStK6JtUMRHp
vxWcyguE1//QRggb345AQk9x4d8zJQyADrNn8ZbVdf3UWOstSdMIHZvl5RZXKxQs
NvMrTO1wQKMDU4kB6JrE2qA=
-----END PRIVATE KEY-----
"""
redox_client_id = '40c57cc3-e4b5-4bb5-a19d-a7cc77204fa5'
auth_json = """
{
  "e": "AQAB",
  "n": "xyXs_Y-uLDfY9TkJ0mfHiH-x3tydCZTE4T0p28HYqvc0MFFJ3cQENp7jawNXMvie1lDZycGZmiRSqoB_ONGEMvOM-JRTfKZLlkEhy7HsD-aHm5w-uKw58WJMKEdbg9LwLtQFyiFgHtzdmvfi0ULoe8RdhyjLomx7Iy2vL2yybsCaWJDjwmf28YQJk8qt1CKrI8NpLG7hdFdXqgvrQQZHV6MqZ5Uc5W33WAvrvpbamptIVVI8CCgwhIobYO_WqOkrGVu9YSu47GJh4r0TeVgYZuh7W42Cud-KI_P5Bu-Hu3aEC-KEbzZzAVDJG-1hjmgdgvJynsndJKIcMIHNktdn-Q",
  "d": "Jlv0R6z2yBajyCxJ64jI4s5x5PMKno9U0uvUlbtDcD74gvwNZdV9WEYHmRPGJo_EDJT7NkT_wLSRZb0lhDy7IZNPCoyLfj2L3q_CAjnNtgxZ_4u7exfVe1zLPZDtHDmzwNlfGh2OpbM2TkTEIDmqjTh4KXIeszUBDPgeP9zIi9NfinrVaOjl1zwiuJidbxwsia7fVl68HdrL7Krkqci02w8BQWhL6F_S4b8NfxrtDAxkv0zm9J1in0pjvKtbVzXA2qXfOj78g5RVideCSTpymLNJ5RgEUq2BZOytS3-SzzCbGaJhRuexIU7w758OWRxgTig2i3QQ9cEa4vPelWcNuQ",
  "p": "5CWfbERTxLgbCY9viiQRPBje0vgPZMsFO2W-7YKGhk9Rh-74jtDnaYvTdfcRHlP6sDAAc-952ebGYkPqvQ2_s9Svkzmzs1avf2TenOIYY800haJhlTyjZ2cB2bpbt-4o-9RiRA3fkbQzgotidiM3PuWjhOGYa0wr2_guhtvQy7U",
  "q": "33X9gUXBj81MxY5nH8JT5bOcdjf7Nu2VcXuJOaLud8buFe9eOBnnt_UqnUK5B5rpI4N-vIQyXTa2nJvwKT2Ha48LIwDSV5BZGCC0EOn-w3PFsUWUVs67a1xiAr3prfr21K0iJ-BsH05j-cOtjDZrMg3rt3JzRi_oCXvustnAfbU",
  "dp": "jlQeSdeyfrXwQB2YkxB_JAPL9sdEKzgwkmaeHuPf8qhHD8t4ivgudf0CNcSljEsNyJ6ZmbobRWIf_yuppLmQD6suwWUtA0OgBpSyACNY3eIl0vyDbiu_MQHk-KS3FV7heIF9sEELaYWG4Pj9uv-rNnTid0KZ6M5DSl5hAEbf0kU",
  "dq": "TKgbSMd3OS-CUJsjQT_WrAvLdrqJkTQ7eeyZDI6aI7i4YAn3VmN8nByce8ox0ywPIkGvrlfjNxrX1i2gW2ViGUG-3kkf3y4DgkVlwA-ld8M1ZDSsSIMuxAuzC6G-khJvrlGeT_Oq46punXXFCT5503f3Dr1SUheJEAfVwMD2Sak",
  "qi": "Even0O3LKXJWcaX4TOrbWdlYjtj2VK40hLEjMTjnEnIZfEuq4FZsV-b44eOyZs-m9OnNxESjCStK6JtUMRHpvxWcyguE1__QRggb345AQk9x4d8zJQyADrNn8ZbVdf3UWOstSdMIHZvl5RZXKxQsNvMrTO1wQKMDU4kB6JrE2qA",
  "kty": "RSA",
  "kid": "DTXccKR4h4Dfhfs1w7CctFOqR545kKj5Xp3T48UEMLA",
  "alg": "RS384",
  "use": "sig"
}
"""

redox_source_id = '91b0ab2f-7b86-441d-9cbf-9b9a6d648d59'

#private_key_kid = 'JKMTAeKln8EIpbuNePD7Lm0HM_yUkojryb1tnm2Z7Gc'
#redox_auth_location = 'https://api.redoxengine.com/v2/auth/token'
#redox_base_url = 'https://api.redoxengine.com/fhir/R4/redox-fhir-sandbox/Development/'
## All Redox FHIR request URLs start with this base: https://api.redoxengine.com/fhir/R4/[organization-name]/[environment-type]/

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
