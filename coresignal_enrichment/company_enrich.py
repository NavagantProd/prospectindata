import os
import csv
import requests
import time
import logging
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv
import argparse

# Load CoreSignal API key
load_dotenv()
CORESIGNAL_API_KEY = os.getenv("CORESIGNAL_API_KEY", "")
if not CORESIGNAL_API_KEY:
    raise RuntimeError("CORESIGNAL_API_KEY environment variable not set.")

SEARCH_ENDPOINT = "https://api.coresignal.com/cdapi/v2/company_multi_source/search/es_dsl"
COLLECT_ENDPOINT = "https://api.coresignal.com/cdapi/v2/company_multi_source/collect"
HEADERS = {
    "apikey": CORESIGNAL_API_KEY,
    "Content-Type": "application/json"
}
RATE_LIMIT_DELAY = 1.0
MAX_RETRIES = 3
REQUEST_TIMEOUT = 30

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger("Company_Enrich")

def flatten_json(y: Dict[str, Any], parent_key: str = '', sep: str = '__') -> Dict[str, Any]:
    """Flatten nested JSON structure."""
    out = {}
    def flatten(x, name=''):
        if isinstance(x, dict):
            for a in x:
                flatten(x[a], f'{name}{a}{sep}')
        elif isinstance(x, list):
            for i, a in enumerate(x):
                flatten(a, f'{name}{i}{sep}')
        else:
            out[name[:-len(sep)]] = x
    flatten(y, parent_key)
    return out

def extract_domain(email: str) -> str:
    """Extract domain from email address."""
    if not email or '@' not in email:
        return ''
    return email.split('@')[1].strip().lower()

def normalize_domain(domain: str) -> str:
    """Normalize domain for search - remove www. prefix if present."""
    if domain.startswith('www.'):
        return domain[4:]
    return domain

def search_company_by_domain(domain: str) -> Optional[str]:
    """Search for company by domain using multiple search strategies."""
    normalized_domain = normalize_domain(domain)
    
    # Strategy 1: Exact match on website field
    payloads = [
        {
            "query": {
                "bool": {
                    "should": [
                        {"match": {"website.exact": normalized_domain}},
                        {"match": {"website.exact": f"www.{normalized_domain}"}},
                        {"match": {"website.exact": f"https://{normalized_domain}"}},
                        {"match": {"website.exact": f"http://{normalized_domain}"}}
                    ],
                    "minimum_should_match": 1
                }
            }
        },
        # Strategy 2: Fuzzy match on website field
        {
            "query": {
                "bool": {
                    "should": [
                        {"match": {"website": {"query": normalized_domain, "fuzziness": "AUTO"}}},
                        {"wildcard": {"website": f"*{normalized_domain}*"}}
                    ],
                    "minimum_should_match": 1
                }
            }
        },
        # Strategy 3: Match on name field (fallback)
        {
            "query": {
                "bool": {
                    "should": [
                        {"match": {"name": {"query": normalized_domain.replace('.', ' '), "fuzziness": "AUTO"}}}
                    ],
                    "minimum_should_match": 1
                }
            }
        }
    ]
    
    for i, payload in enumerate(payloads):
        logger.info(f"[SEARCH] Strategy {i+1} for domain '{domain}' | Payload: {payload}")
        
        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.post(SEARCH_ENDPOINT, headers=HEADERS, json=payload, timeout=REQUEST_TIMEOUT)
                logger.info(f"[SEARCH] Strategy {i+1} Attempt {attempt+1} | Response: {resp.status_code}")
                
                if resp.status_code == 200:
                    data = resp.json()
                    logger.info(f"[SEARCH] Response data: {data}")
                    
                    # Handle different response formats
                    if isinstance(data, list):
                        hits = data
                    elif isinstance(data, dict):
                        hits = data.get('hits') or data.get('results') or data.get('data') or []
                    else:
                        hits = []
                    
                    if hits:
                        first = hits[0]
                        if isinstance(first, dict):
                            company_id = first.get('id') or first.get('_id') or first.get('company_id')
                            if company_id:
                                logger.info(f"[SEARCH] Found company ID: {company_id} for domain: {domain}")
                                return str(company_id)
                        elif isinstance(first, (str, int)):
                            logger.info(f"[SEARCH] Found company ID: {first} for domain: {domain}")
                            return str(first)
                    
                    logger.info(f"[SEARCH] Strategy {i+1} returned no results for domain: {domain}")
                    break  # Try next strategy
                    
                elif resp.status_code == 429:
                    logger.warning("Rate limited, sleeping...")
                    time.sleep(5)
                elif resp.status_code == 403:
                    logger.error("Forbidden: Check API key permissions for multi-source company data.")
                    return None
                elif resp.status_code == 404:
                    logger.error(f"Not Found: Endpoint may be incorrect. Response: {resp.text}")
                    return None
                else:
                    logger.warning(f"Unexpected status code {resp.status_code}: {resp.text}")
                    
            except Exception as e:
                logger.error(f"Error during company search: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2)
        
        # Small delay between strategies
        time.sleep(0.5)
    
    logger.warning(f"[SEARCH] No company found for domain: {domain} after trying all strategies")
    return None

def collect_company(company_id: str) -> Optional[Dict[str, Any]]:
    """Collect full company details by ID."""
    url = f"{COLLECT_ENDPOINT}/{company_id}"
    
    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"[COLLECT] Attempt {attempt+1} | URL: {url}")
            resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            logger.info(f"[COLLECT] Response: {resp.status_code}")
            
            if resp.status_code == 200:
                data = resp.json()
                logger.info(f"[COLLECT] Successfully collected data for company ID: {company_id}")
                return data
                
            elif resp.status_code == 429:
                logger.warning("Rate limited, sleeping...")
                time.sleep(5)
            elif resp.status_code == 403:
                logger.error("Forbidden: Check API key permissions for multi-source company data.")
                break
            elif resp.status_code == 404:
                logger.error(f"Not Found: Company ID {company_id} may be incorrect.")
                break
            else:
                logger.warning(f"Unexpected status code {resp.status_code}: {resp.text}")
                
        except Exception as e:
            logger.error(f"Error during company collect: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(2)
    
    return None

def test_api_connectivity():
    """Test API connectivity with a known domain."""
    test_domains = ["google.com", "microsoft.com", "salesforce.com"]
    
    logger.info("Testing API connectivity...")
    for domain in test_domains:
        logger.info(f"Testing with domain: {domain}")
        company_id = search_company_by_domain(domain)
        if company_id:
            logger.info(f"✓ API working - found company ID {company_id} for {domain}")
            return True
        time.sleep(1)
    
    logger.error("✗ API connectivity test failed - no results for any test domains")
    return False

def main():
    parser = argparse.ArgumentParser(description="Enrich Mademarket recipients with CoreSignal company data")
    parser.add_argument('--input', default=os.path.join("Made_Market_Data", "mademarket_2025_ISTE.csv"), help='Input CSV file')
    parser.add_argument('--output', default=os.path.join("coresignal_enrichment", "company_enriched", "mademarket_2025_ISTE_company_enriched.csv"), help='Output enriched CSV file')
    parser.add_argument('--test-api', action='store_true', help='Test API connectivity first')
    parser.add_argument('--limit', type=int, default=5, help='Limit number of domains to process (for testing)')
    args = parser.parse_args()
    
    # Test API connectivity if requested
    if args.test_api:
        if not test_api_connectivity():
            logger.error("Exiting due to API connectivity issues")
            return
    
    input_file = args.input
    output_file = args.output
    
    # Read input
    logger.info(f"Reading input file: {input_file}")
    try:
        with open(input_file, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        logger.info(f"Read {len(rows)} rows from input file")
    except Exception as e:
        logger.error(f"Error reading input file: {e}")
        return
    
    # Get unique domains (limited for testing)
    domains = []
    seen = set()
    for row in rows:
        domain = extract_domain(row.get('recipient_email', ''))
        if domain and domain not in seen:
            domains.append(domain)
            seen.add(domain)
        if len(domains) >= args.limit:
            break
    
    logger.info(f"Processing first {len(domains)} unique domains: {domains}")
    
    # Query CoreSignal for each domain
    domain_to_company = {}
    successful_lookups = 0
    
    for i, domain in enumerate(domains):
        logger.info(f"Processing domain {i+1}/{len(domains)}: {domain}")
        
        company_id = search_company_by_domain(domain)
        if company_id:
            company_data = collect_company(company_id)
            if company_data:
                domain_to_company[domain] = flatten_json(company_data, parent_key='cs_company')
                successful_lookups += 1
                logger.info(f"✓ Successfully enriched domain: {domain}")
            else:
                domain_to_company[domain] = {}
                logger.warning(f"✗ Found company ID but failed to collect data for: {domain}")
        else:
            domain_to_company[domain] = {}
            logger.warning(f"✗ No company found for domain: {domain}")
        
        time.sleep(RATE_LIMIT_DELAY)
    
    logger.info(f"Successfully enriched {successful_lookups}/{len(domains)} domains")
    
    # Write enriched output
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            # Filter rows to only include those with processed domains
            filtered_rows = [row for row in rows if extract_domain(row.get('recipient_email', '')) in domains]
            
            if filtered_rows:
                fieldnames = list(filtered_rows[0].keys())
                
                # Add all company fields found
                all_company_fields = set()
                for v in domain_to_company.values():
                    all_company_fields.update(v.keys())
                fieldnames += sorted([f for f in all_company_fields if f not in fieldnames])
                
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
                for row in filtered_rows:
                    domain = extract_domain(row.get('recipient_email', ''))
                    company_data = domain_to_company.get(domain, {})
                    out_row = row.copy()
                    out_row.update(company_data)
                    writer.writerow(out_row)
                
                logger.info(f"✓ Wrote enriched CSV to {output_file}")
                logger.info(f"Output contains {len(filtered_rows)} rows with {len(all_company_fields)} company fields")
            else:
                logger.warning("No rows to write to output file")
                
    except Exception as e:
        logger.error(f"Error writing output file: {e}")
        return
    
    print(f"Enrichment complete:")
    print(f"  - Processed {len(domains)} unique domains")
    print(f"  - Successfully enriched {successful_lookups} companies")
    print(f"  - Output written to: {output_file}")

if __name__ == "__main__":
    main()