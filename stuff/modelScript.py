import requests
import json
import csv
import time
from tqdm import tqdm
import os
import logging

# READ:
# Basically, we try to target high level executives/decision makers. If we can't find them, 
# we try lower level executives. If there are none, we simply remove the job-title condition
# to try to get anyone

# We append the results (only the selected values, lmk if you don't have them) to the regular company results and download them (no display or anything) as a csv
API_KEY = 'w5jfmFnwtLAPRWH5UcB6D23XWEIlPneI'  # <-- Replace with your new API key from the API keys tab

# Jacob's suggestion: CEO, President, Founder, Co-Founder, Owner, Co-Founder
high_level_executive = ['CEO', 
                        'Chief Executive Officer',
                        'Chief Executive Officer (CEO)',
                        'President', 
                        'Founder',
                        'Co-Founder', 
                        'CoFounder', 
                        'Co Founder',
                        'Owner',
                        'Co-Owner',
                        'CoOwner',
                        'Co Owner']
# Generate the clauses
should_clauses = [
    {
        "match_phrase": {
            "member_experience_collection.title": title
        }
    }
    for title in high_level_executive
]

#################################################################################

# Usual call the API stuff

# =====================
# ENRICHMENT TO CSV
# =====================

def safe_company_id(company_id):
    try:
        return int(float(company_id))
    except Exception:
        return company_id

def get_employee_details(employee_id, api_key):
    # Use the correct endpoint and integer ID
    employee_id = int(float(employee_id))
    url = f"https://api.coresignal.com/cdapi/v2/employee_clean/collect?ids={employee_id}"
    headers = {
        'Content-Type': 'application/json',
        'apikey': api_key
    }
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        return None
    data = response.json()
    if isinstance(data, list) and data:
        data = data[0]
    # Safely extract fields
    name = data.get('full_name', '')
    # Try to get the most recent/current experience title
    title = ''
    profile_url = data.get('profile_url', '') or data.get('profile_urn', '')
    exp = data.get('member_experience_collection', [])
    if exp and isinstance(exp, list):
        # Find the current experience (date_to is None or 1000)
        for e in exp:
            if not e.get('date_to') or e.get('date_to') == '1000':
                title = e.get('title', '')
                break
        if not title:
            title = exp[0].get('title', '')
    return {
        'employee_name': name,
        'employee_title': title,
        'employee_profile_url': profile_url
    }

# Robust API request with retries and exponential backoff
def robust_request(method, url, headers=None, json_data=None, timeout=30, max_retries=5):
    delay = 1
    for attempt in range(1, max_retries + 1):
        try:
            if method == 'GET':
                resp = requests.get(url, headers=headers, timeout=timeout)
            elif method == 'POST':
                resp = requests.post(url, headers=headers, json=json_data, timeout=timeout)
            else:
                raise ValueError('Unsupported HTTP method')
            if resp.status_code == 200:
                return resp
            else:
                print(f"API {method} {url} failed (status {resp.status_code}), attempt {attempt}/{max_retries}")
                print(f"Response: {resp.text[:300]}")
        except Exception as e:
            print(f"API {method} {url} exception: {e}, attempt {attempt}/{max_retries}")
        if attempt < max_retries:
            time.sleep(delay)
            delay *= 2
    print(f"API {method} {url} failed after {max_retries} attempts.")
    return None

# --- Helper to flatten employee dicts ---
def flatten_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            items.append((new_key, json.dumps(v, ensure_ascii=False)))
        else:
            items.append((new_key, v))
    return dict(items)

# ========== LOGGING SETUP ==========
LOG_FILE = 'enrichment.log'
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG for maximum verbosity
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ========== CHECKPOINTING ==========
CHECKPOINT_FILE = 'enrichment_checkpoint.json'
def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                return set(json.load(f))
        except Exception as e:
            logger.warning(f"Failed to load checkpoint: {e}")
    return set()
def save_checkpoint(processed_company_ids):
    try:
        with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
            json.dump(list(processed_company_ids), f)
    except Exception as e:
        logger.error(f"Failed to save checkpoint: {e}")

def safe_stringify(value):
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return "" if value is None else str(value)

# --- Employee fetch logic ---
def get_employees_for_company(company_id, company_name=None, max_employees=20, company_domain=None, company_location=None):
    # Try original logic first
    employees = []
    # 1. Try strict search (existing logic)
    employees = _get_employees_strict(company_id, company_name, max_employees)
    if employees:
        return employees
    # 2. Fuzzy by name variants (first, last, full, lower)
    # 3. Fuzzy by title, location, domain, using employee_multi_source
    logger.info(f"Trying advanced fuzzy search for company {company_id} ...")
    api_key = os.environ.get('CORESIGNAL_API_KEY') or 'w5jfmFnwtLAPRWH5UcB6D23XWEIlPneI'
    headers = {'Content-Type': 'application/json', 'apikey': api_key}
    search_url = 'https://api.coresignal.com/cdapi/v2/employee_multi_source/search/es_dsl'
    queries = []
    # Fuzzy by company website
    if company_domain:
        queries.append({
            "query": {
                "bool": {
                    "must": [
                        {"nested": {"path": "experience", "query": {"bool": {"must": [
                            {"term": {"experience.company_website.exact": company_domain}},
                            {"term": {"experience.active_experience": 0}}
                        ]}}}}
                    ]
                }
            }
        })
    # Fuzzy by location
    if company_location:
        queries.append({
            "query": {
                "bool": {
                    "must": [
                        {"match": {"location_full": company_location}},
                        {"term": {"location_country": "France"}}  # Example, adjust as needed
                    ]
                }
            }
        })
    # Fuzzy by title
    for title in ["Head of Marketing", "CMO", "Chief Marketing Officer"]:
        queries.append({
            "query": {
                "bool": {
                    "must": [
                        {"query_string": {"query": f'\"{title}\"', "default_field": "active_experience_title"}}
                    ]
                }
            }
        })
    # Fuzzy by name (full, first, last)
    for name_field in ["full_name", "first_name", "last_name"]:
        for op in ["and", "or"]:
            queries.append({
                "query": {
                    "bool": {
                        "must": [
                            {"query_string": {"query": company_name or "", "default_field": name_field, "default_operator": op}}
                        ]
                    }
                }
            })
    # Try all queries
    for q in queries:
        logger.debug(f"Trying employee_multi_source query: {q}")
        resp = robust_request('POST', search_url, headers, q)
        if resp and resp.status_code == 200:
            try:
                people = resp.json()
                if people:
                    logger.info(f"Found {len(people)} employees with fuzzy query.")
                    return fetch_and_flatten_employees(people, company_id, headers, max_employees)
            except Exception as e:
                logger.warning(f"Error parsing employee_multi_source JSON: {e}")
    logger.warning(f"No employees found for company {company_id} after all fuzzy search strategies.")
    return []

# --- Helper to fetch and flatten employees ---
def fetch_and_flatten_employees(people, company_id, headers, max_employees=20):
    employees = []
    seen = set()
    for idx, emp_id in enumerate(people, 1):
        if len(employees) >= max_employees:
            break
        if emp_id in seen:
            continue
        seen.add(emp_id)
        logger.info(f"  Collecting employee {idx}/{len(people)} for company {company_id}: {emp_id}")
        emp_id_int = int(float(emp_id))
        collect_url = f"https://api.coresignal.com/cdapi/v2/employee_clean/collect?ids={emp_id_int}"
        emp_resp = robust_request('GET', collect_url, headers)
        if not emp_resp or emp_resp.status_code != 200:
            logger.warning(f"Failed to collect employee {emp_id} for company {company_id}")
            continue
        try:
            data = emp_resp.json()
            if isinstance(data, list) and data:
                data = data[0]
        except Exception as e:
            logger.warning(f"Error parsing employee JSON for {emp_id}: {e}")
            continue
        flat = flatten_dict(data)
        flat['enriched_company_id'] = company_id
        employees.append(flat)
        time.sleep(0.5)
    return employees

# --- Main enrichment loop ---
INPUT_CSV = 'enhanced_enrichment.csv'
OUTPUT_CSV = 'finalenrichment.csv'
COMPANY_ID_COLUMN = 'cs_company_id'


def main():
    logger.info("==============================")
    logger.info("Starting Company/Employee Enrichment Script")
    logger.info(f"Input CSV: {INPUT_CSV}")
    logger.info(f"Output CSV: {OUTPUT_CSV}")
    logger.info(f"Checkpoint file: {CHECKPOINT_FILE}")
    logger.info(f"Log file: {LOG_FILE}")
    logger.info("==============================")
    with open(INPUT_CSV, newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        rows = list(reader)
        company_ids = set(row[COMPANY_ID_COLUMN] for row in rows if row[COMPANY_ID_COLUMN])
        company_id_to_name = {row[COMPANY_ID_COLUMN]: row.get('company_name', None) for row in rows if row[COMPANY_ID_COLUMN]}
    processed_company_ids = load_checkpoint()
    all_emp_keys = set()
    logger.info(f"Loaded {len(company_ids)} companies. {len(processed_company_ids)} already processed. Starting enrichment.")
    company_cache = {}
    with tqdm(total=len(company_ids), desc='Companies') as pbar:
        for company_id in company_ids:
            if company_id in processed_company_ids:
                logger.debug(f"Skipping already processed company: {company_id}")
                pbar.update(1)
                continue
            logger.info(f"Processing company: {company_id} ({company_id_to_name.get(company_id)})")
            try:
                employees = get_employees_for_company(company_id, company_id_to_name.get(company_id), max_employees=20)
            except Exception as e:
                logger.error(f"Exception for company {company_id}: {e}")
                employees = []
            logger.info(f"  Found {len(employees)} employees for company {company_id}")
            if len(employees) == 0:
                logger.warning(f"No employees found for company {company_id}")
            else:
                logger.debug(f"Employee IDs for company {company_id}: {[emp.get('id', 'N/A') for emp in employees]}")
            company_cache[company_id] = employees
            for emp in employees:
                all_emp_keys.update(emp.keys())
            processed_company_ids.add(company_id)
            save_checkpoint(processed_company_ids)
            logger.debug(f"Checkpoint saved after company {company_id}")
            pbar.update(1)
            time.sleep(1)
    logger.info(f"Writing output to {OUTPUT_CSV} ...")
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as outfile:
        fieldnames = list(rows[0].keys()) + sorted(all_emp_keys)
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            company_id = row[COMPANY_ID_COLUMN]
            employees = company_cache.get(company_id, [])
            if employees:
                for emp in employees:
                    out_row = row.copy()
                    out_row.update(emp)
                    out_row = {k: safe_stringify(v) for k, v in out_row.items()}
                    writer.writerow(out_row)
            else:
                empty_emp = {k: '' for k in all_emp_keys}
                out_row = row.copy()
                out_row.update(empty_emp)
                out_row = {k: safe_stringify(v) for k, v in out_row.items()}
                writer.writerow(out_row)
    logger.info(f"Enrichment complete. Output written to {OUTPUT_CSV}")
    logger.info(f"Total companies processed: {len(company_ids)}")
    logger.info(f"Total employees written: {sum(len(company_cache[cid]) for cid in company_cache)}")
    logger.info("==============================")
    logger.info("Script finished. You can safely close this window.")

if __name__ == '__main__':
    main()