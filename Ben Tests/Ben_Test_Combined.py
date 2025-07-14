import os
import requests
from dotenv import load_dotenv
import urllib.parse

load_dotenv()
CORESIGNAL_API_KEY = os.getenv("CORESIGNAL_API_KEY", "")

endpoints = [
    "https://api.coresignal.com/cdapi/v2/company_multi_source/search/es_dsl",
    "https://api.coresignal.com/cdapi/v1/company/search/es_dsl",
    "https://api.coresignal.com/cdapi/v1/company_multi_source/search/es_dsl",
    "https://api.coresignal.com/cdapi/v2/multi_source/company/search/es_dsl",
    "https://api.coresignal.com/cdapi/v1/multi_source/company/search",
    "https://api.coresignal.com/cdapi/v2/multi_source/company/search",
    "https://api.coresignal.com/api/v1/company/search",
    "https://api.coresignal.com/api/v2/company/search",
    "https://coresignal.com/api/v1/company/search",
]

auth_variants = [
    {"apikey": CORESIGNAL_API_KEY},
    {"Authorization": f"Bearer {CORESIGNAL_API_KEY}"},
    {"Authorization": f"ApiKey {CORESIGNAL_API_KEY}"},
    {"X-API-Key": CORESIGNAL_API_KEY},
    {"api-key": CORESIGNAL_API_KEY},
    {"token": CORESIGNAL_API_KEY},
]

payload = {"query": {"match_all": {}}}

print("Testing all combinations of endpoint, auth header, and HTTP method...")
for endpoint in endpoints:
    for i, auth_header in enumerate(auth_variants):
        headers = {**auth_header, "Content-Type": "application/json"}
        # Test POST
        try:
            resp = requests.post(endpoint, headers=headers, json=payload, timeout=10)
            print(f"POST | {endpoint} | {list(auth_header.keys())[0]}: {resp.status_code} - {resp.text[:100]}")
            if resp.status_code != 404 and resp.status_code != 422:
                print(f"*** WORKING COMBINATION: POST | {endpoint} | {auth_header} ***")
                exit(0)
        except Exception as e:
            print(f"POST | {endpoint} | {list(auth_header.keys())[0]}: ERROR - {e}")
        # Test GET
        try:
            query_string = urllib.parse.urlencode({"query": str(payload)})
            get_url = f"{endpoint}?{query_string}"
            resp = requests.get(get_url, headers=headers, timeout=10)
            print(f"GET | {endpoint} | {list(auth_header.keys())[0]}: {resp.status_code} - {resp.text[:100]}")
            if resp.status_code != 404 and resp.status_code != 422:
                print(f"*** WORKING COMBINATION: GET | {endpoint} | {auth_header} ***")
                exit(0)
        except Exception as e:
            print(f"GET | {endpoint} | {list(auth_header.keys())[0]}: ERROR - {e}")
print("No working combination found.") 