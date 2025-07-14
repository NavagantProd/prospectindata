import os
import requests
from dotenv import load_dotenv

load_dotenv()
CORESIGNAL_API_KEY = os.getenv("CORESIGNAL_API_KEY", "")

endpoints = [
    "https://api.coresignal.com/cdapi/v1/company/search/es_dsl",
    "https://api.coresignal.com/cdapi/v1/company_multi_source/search/es_dsl",
    "https://api.coresignal.com/cdapi/v2/multi_source/company/search/es_dsl",
    "https://api.coresignal.com/cdapi/v2/company_multi_source/search/es_dsl",
    "https://api.coresignal.com/cdapi/v1/multi_source/company/search",
    "https://api.coresignal.com/cdapi/v2/multi_source/company/search",
    "https://api.coresignal.com/cdapi/v1/multi_source/company/search/es_dsl",
    "https://api.coresignal.com/api/v1/company/search",
    "https://api.coresignal.com/api/v2/company/search",
    "https://coresignal.com/api/v1/company/search",
]

headers = {"apikey": CORESIGNAL_API_KEY, "Content-Type": "application/json"}
payload = {"query": {"match_all": {}}, "size": 1}

print("Testing endpoint URL variations...")
for endpoint in endpoints:
    try:
        resp = requests.post(endpoint, headers=headers, json=payload, timeout=10)
        print(f"{endpoint}: {resp.status_code} - {resp.text[:100]}")
        if resp.status_code != 404:
            print(f"*** WORKING ENDPOINT FOUND: {endpoint} ***")
            break
    except Exception as e:
        print(f"{endpoint}: ERROR - {e}") 