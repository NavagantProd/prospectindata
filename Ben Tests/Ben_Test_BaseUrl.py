import os
import requests
from dotenv import load_dotenv

load_dotenv()
CORESIGNAL_API_KEY = os.getenv("CORESIGNAL_API_KEY", "")

base_urls = [
    "https://api.coresignal.com",
    "https://api.coresignal.com/api",
    "https://coresignal.com/api",
    "https://api-v2.coresignal.com",
    "https://company-api.coresignal.com",
]

path = "/cdapi/v2/company_multi_source/search/es_dsl"
headers = {"apikey": CORESIGNAL_API_KEY, "Content-Type": "application/json"}
payload = {"query": {"match_all": {}}}

print("Testing base URLs...")
for base_url in base_urls:
    full_url = f"{base_url}{path}"
    try:
        resp = requests.post(full_url, headers=headers, json=payload, timeout=10)
        print(f"Base URL {base_url}: {resp.status_code} - {resp.text[:100]}")
        if resp.status_code != 404 and resp.status_code != 422:
            print(f"*** WORKING BASE URL: {base_url} ***")
            break
    except Exception as e:
        print(f"Base URL {base_url}: ERROR - {e}") 