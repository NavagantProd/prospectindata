import os
import requests
from dotenv import load_dotenv

load_dotenv()
CORESIGNAL_API_KEY = os.getenv("CORESIGNAL_API_KEY", "")

endpoint = "https://api.coresignal.com/cdapi/v2/company_multi_source/search/es_dsl"
payload = {"query": {"match_all": {}}}

auth_variants = [
    {"apikey": CORESIGNAL_API_KEY},
    {"Authorization": f"Bearer {CORESIGNAL_API_KEY}"},
    {"Authorization": f"ApiKey {CORESIGNAL_API_KEY}"},
    {"X-API-Key": CORESIGNAL_API_KEY},
    {"api-key": CORESIGNAL_API_KEY},
    {"token": CORESIGNAL_API_KEY},
]

print("Testing authentication header formats...")
for i, auth_header in enumerate(auth_variants):
    headers = {**auth_header, "Content-Type": "application/json"}
    try:
        resp = requests.post(endpoint, headers=headers, json=payload, timeout=10)
        print(f"Auth variant {i+1} ({list(auth_header.keys())[0]}): {resp.status_code} - {resp.text[:100]}")
        if resp.status_code != 404 and resp.status_code != 422:
            print(f"*** WORKING AUTH HEADER: {auth_header} ***")
            break
    except Exception as e:
        print(f"Auth variant {i+1}: ERROR - {e}") 