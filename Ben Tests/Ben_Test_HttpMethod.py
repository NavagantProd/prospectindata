import os
import requests
from dotenv import load_dotenv
import urllib.parse

load_dotenv()
CORESIGNAL_API_KEY = os.getenv("CORESIGNAL_API_KEY", "")

endpoint = "https://api.coresignal.com/cdapi/v2/company_multi_source/search/es_dsl"
headers = {"apikey": CORESIGNAL_API_KEY, "Content-Type": "application/json"}
payload = {"query": {"match_all": {}}}

print("Testing HTTP methods (POST vs GET)...")
# Test POST
try:
    resp = requests.post(endpoint, headers=headers, json=payload, timeout=10)
    print(f"POST: {resp.status_code} - {resp.text[:100]}")
    if resp.status_code != 404 and resp.status_code != 422:
        print("*** WORKING METHOD: POST ***")
except Exception as e:
    print(f"POST: ERROR - {e}")

# Test GET with query string
try:
    query_string = urllib.parse.urlencode({"query": str(payload)})
    get_url = f"{endpoint}?{query_string}"
    resp = requests.get(get_url, headers=headers, timeout=10)
    print(f"GET: {resp.status_code} - {resp.text[:100]}")
    if resp.status_code != 404 and resp.status_code != 422:
        print("*** WORKING METHOD: GET ***")
except Exception as e:
    print(f"GET: ERROR - {e}") 