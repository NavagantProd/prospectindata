#!/usr/bin/env python3
"""
CoreSignal v2 API Employee Enrichment Pipeline - COMPLETE WORKING VERSION
Fixed all issues including missing methods, proper CSV handling, and API compatibility
"""

import requests
import pandas as pd
import json
import time
import logging
import argparse
import os
import sys
from typing import Dict, List, Optional, Tuple, Any, Union
from dataclasses import dataclass, asdict, field
from datetime import datetime
import re
from collections import defaultdict
from pathlib import Path
import tldextract

# Setup logging
def setup_logging(log_file: str = None):
    """Setup dual logging: INFO to console, DEBUG to file"""
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    
    handlers = [console_handler]
    if log_file:
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        handlers.append(file_handler)
    
    logging.basicConfig(level=logging.DEBUG, handlers=handlers)
    return logging.getLogger(__name__)

logger = setup_logging('coresignal_enrichment.log')

@dataclass
class EnrichmentStats:
    total_contacts: int = 0
    company_found: int = 0
    company_not_found: int = 0
    employee_found: int = 0
    employee_not_found: int = 0
    successful_enrichments: int = 0
    failed_enrichments: int = 0
    api_calls: int = 0
    cache_hits: int = 0
    processing_time: float = 0.0
    error_counts: Dict[str, int] = field(default_factory=dict)
    
    def summary(self) -> str:
        total = self.successful_enrichments + self.failed_enrichments
        if total == 0:
            return "No enrichments processed"
        
        success_rate = (self.successful_enrichments / total) * 100
        company_rate = (self.company_found / self.total_contacts) * 100 if self.total_contacts > 0 else 0
        employee_rate = (self.employee_found / self.total_contacts) * 100 if self.total_contacts > 0 else 0
        
        return f"""
CoreSignal API Enrichment Summary:
  Total Contacts: {self.total_contacts}
  Companies Found: {self.company_found} ({company_rate:.1f}%)
  Employees Found: {self.employee_found} ({employee_rate:.1f}%)
  Success Rate: {self.successful_enrichments}/{total} ({success_rate:.1f}%)
  API Calls Made: {self.api_calls}
  Cache Hits: {self.cache_hits}
  Processing Time: {self.processing_time:.2f}s
"""

@dataclass
class ContactData:
    # Core contact info
    name: str = ""
    email: str = ""
    company_name: str = ""
    company_website: str = ""
    
    # Company data fields
    company_id: str = ""
    company_display_name: str = ""
    company_canonical_url: str = ""
    company_website_url: str = ""
    company_industry: str = ""
    company_size: str = ""
    company_type: str = ""
    company_founded: str = ""
    company_employees: str = ""
    company_description: str = ""
    company_headquarters: str = ""
    company_specialties: str = ""
    company_logo_url: str = ""
    
    # Employee data fields
    employee_id: str = ""
    employee_full_name: str = ""
    employee_first_name: str = ""
    employee_last_name: str = ""
    employee_title: str = ""
    employee_headline: str = ""
    employee_url: str = ""
    employee_canonical_url: str = ""
    employee_location: str = ""
    employee_country: str = ""
    employee_industry: str = ""
    employee_connections_count: str = ""
    employee_summary: str = ""
    employee_experience: str = ""
    employee_education: str = ""
    employee_created: str = ""
    employee_last_updated: str = ""
    
    # Enrichment metadata
    enrichment_status: str = "pending"
    enrichment_error: str = ""
    company_match_score: float = 0.0
    employee_match_score: float = 0.0
    api_calls_made: int = 0
    
    def __post_init__(self):
        self.name = self.name.strip() if self.name else ""
        self.email = self.email.strip().lower() if self.email else ""
        self.company_name = self.company_name.strip() if self.company_name else ""
        self.company_website = self.company_website.strip() if self.company_website else ""
    
    def is_valid(self) -> bool:
        return bool(self.name and self.email)

def extract_domain_from_url(url: str) -> str:
    """Extract clean domain from URL"""
    if not url:
        return ""
    
    if not url.startswith(('http://', 'https://')):
        url = f"https://{url}"
    
    try:
        extracted = tldextract.extract(url)
        domain = f"{extracted.domain}.{extracted.suffix}"
        return domain.lower()
    except Exception:
        return ""

def get_email_domain(email: str) -> str:
    """Extract domain from email address"""
    if not email or '@' not in email:
        return ""
    try:
        domain = email.split('@')[1].lower().strip()
        if domain.startswith('mail.'):
            domain = domain[5:]
        return domain
    except (IndexError, AttributeError):
        return ""

class CoreSignalClient:
    def __init__(self, api_key: str, rate_limit: float = 1.0, timeout: int = 30):
        self.api_key = api_key
        self.base_url = "https://api.coresignal.com/cdapi/v2"
        self.timeout = timeout
        self.rate_limit = rate_limit
        self.last_call = 0
        
        self.session = requests.Session()
        self.session.headers.update({
            "apikey": api_key,
            "Content-Type": "application/json",
            "Accept": "application/json"
        })
        
        self.stats = EnrichmentStats()
        self.company_cache: Dict[str, Any] = {}
        self.employee_cache: Dict[str, Any] = {}

    def _wait(self):
        """Implement rate limiting"""
        elapsed = time.time() - self.last_call
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        self.last_call = time.time()

    def _request_with_retry(self, method: str, url: str, **kwargs) -> requests.Response:
        """Make HTTP request with retry logic"""
        max_retries = 3
        base_delay = 1.0

        for attempt in range(max_retries):
            try:
                self._wait()
                if 'timeout' not in kwargs:
                    kwargs['timeout'] = self.timeout

                logger.debug(f"API REQUEST: {method} {url}")
                if 'json' in kwargs:
                    logger.debug(f"REQUEST PAYLOAD: {json.dumps(kwargs['json'], indent=2)}")

                resp = self.session.request(method, url, **kwargs)
                self.stats.api_calls += 1

                logger.debug(f"RESPONSE STATUS: {resp.status_code}")
                if resp.text:
                    logger.debug(f"RESPONSE BODY: {resp.text[:500]}...")

                if resp.status_code == 200:
                    return resp
                elif resp.status_code == 422:
                    logger.error(f"API validation error (422): {resp.text}")
                    return resp
                elif resp.status_code == 429:
                    retry_after = int(resp.headers.get('Retry-After', base_delay * (2 ** attempt)))
                    logger.warning(f"Rate limited, waiting {retry_after}s")
                    time.sleep(retry_after)
                    continue
                elif resp.status_code == 401:
                    logger.error("Authentication failed - check API key")
                    return resp
                else:
                    logger.warning(f"HTTP {resp.status_code}: {resp.text}")
                    return resp

            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed: {e}")
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"Retrying in {delay}s...")
                    time.sleep(delay)
                    continue
                break

        return requests.Response()

    def search_companies(self, name: str, website: str = "") -> List[Dict]:
        """Search companies using CoreSignal v2 API"""
        if not name:
            return []
        
        url = f"{self.base_url}/company_base/search/filter"
        
        # Build search payload - only use valid parameters
        payload = {
            "name": name.strip()
        }
        
        if website:
            payload["website"] = website.strip()
        
        logger.debug(f"Company search payload: {payload}")
        resp = self._request_with_retry("POST", url, json=payload)
        
        if resp.status_code == 200:
            try:
                data = resp.json()
                logger.debug(f"Company search response: {data}")
                
                # Handle different response formats
                if isinstance(data, list):
                    return data[:10]  # Limit results
                elif isinstance(data, dict):
                    if 'hits' in data:
                        return data['hits'][:10]
                    elif 'data' in data:
                        return data['data'][:10]
                    else:
                        return [data]
                else:
                    return []
            except Exception as e:
                logger.error(f"Failed to parse company search response: {e}")
                return []
        else:
            logger.warning(f"Company search failed with status {resp.status_code}: {resp.text}")
            return []

    def search_employees(self, name: str = "", company_name: str = "", 
                        location: str = "", title: str = "") -> List[Dict]:
        """Search employees using CoreSignal v2 API"""
        url = f"{self.base_url}/member/search/filter"
        
        # Build search payload - only use valid parameters
        payload = {}
        
        if name:
            payload["name"] = name.strip()
        if company_name:
            payload["experience_company_name"] = company_name.strip()
        if location:
            payload["location"] = location.strip()
        if title:
            payload["title"] = title.strip()
        
        if not payload:
            return []
        
        logger.debug(f"Employee search payload: {payload}")
        resp = self._request_with_retry("POST", url, json=payload)
        
        if resp.status_code == 200:
            try:
                data = resp.json()
                logger.debug(f"Employee search response: {data}")
                
                # Handle different response formats
                if isinstance(data, list):
                    return data[:10]  # Limit results
                elif isinstance(data, dict):
                    if 'hits' in data:
                        return data['hits'][:10]
                    elif 'data' in data:
                        return data['data'][:10]
                    else:
                        return [data]
                else:
                    return []
            except Exception as e:
                logger.error(f"Failed to parse employee search response: {e}")
                return []
        else:
            logger.warning(f"Employee search failed with status {resp.status_code}: {resp.text}")
            return []

    def search_employees_by_name_and_company(self, name: str, company_id: str = "", 
                                           email: str = "", first_name: str = "", 
                                           last_name: str = "", company_name: str = "") -> List[Dict]:
        """Search employees by name and company - missing method implementation"""
        # Use the existing search_employees method
        return self.search_employees(
            name=name,
            company_name=company_name,
            location="",
            title=""
        )

    def get_company_by_id(self, company_id: str) -> Optional[Dict]:
        """Get company details by ID"""
        if not company_id:
            return None
        
        # Check cache first
        if company_id in self.company_cache:
            self.stats.cache_hits += 1
            return self.company_cache[company_id]
        
        url = f"{self.base_url}/company_base/collect/{company_id}"
        resp = self._request_with_retry("GET", url)
        
        if resp.status_code == 200:
            try:
                data = resp.json()
                self.company_cache[company_id] = data
                return data
            except Exception as e:
                logger.error(f"Failed to parse company data: {e}")
                return None
        else:
            logger.warning(f"Company collect failed with status {resp.status_code}")
            return None

    def get_employee_by_id(self, employee_id: str) -> Optional[Dict]:
        """Get employee details by ID"""
        if not employee_id:
            return None
        
        # Check cache first
        if employee_id in self.employee_cache:
            self.stats.cache_hits += 1
            return self.employee_cache[employee_id]
        
        url = f"{self.base_url}/member/collect/{employee_id}"
        resp = self._request_with_retry("GET", url)
        
        if resp.status_code == 200:
            try:
                data = resp.json()
                self.employee_cache[employee_id] = data
                return data
            except Exception as e:
                logger.error(f"Failed to parse employee data: {e}")
                return None
        else:
            logger.warning(f"Employee collect failed with status {resp.status_code}")
            return None

def fill_company_fields(contact: ContactData, company_data: Dict) -> None:
    """Fill company fields from API response"""
    contact.company_id = str(company_data.get('id', ''))
    contact.company_display_name = company_data.get('name', '')
    contact.company_canonical_url = company_data.get('canonical_url', '')
    contact.company_website_url = company_data.get('website', '')
    contact.company_industry = company_data.get('industry', '')
    contact.company_size = company_data.get('size', '')
    contact.company_type = company_data.get('type', '')
    contact.company_founded = str(company_data.get('founded', ''))
    contact.company_employees = str(company_data.get('employees', ''))
    contact.company_description = company_data.get('description', '')
    contact.company_logo_url = company_data.get('logo_url', '')
    
    # Handle headquarters
    hq = company_data.get('hq', {})
    if isinstance(hq, dict):
        hq_parts = [hq.get('city', ''), hq.get('state', ''), hq.get('country', '')]
        contact.company_headquarters = ', '.join(filter(None, hq_parts))
    
    # Handle specialties
    specialties = company_data.get('specialties', [])
    if isinstance(specialties, list):
        contact.company_specialties = ', '.join(specialties)

def fill_employee_fields(contact: ContactData, employee_data: Dict) -> None:
    """Fill employee fields from API response"""
    contact.employee_id = str(employee_data.get('id', ''))
    contact.employee_full_name = employee_data.get('name', '')
    contact.employee_first_name = employee_data.get('first_name', '')
    contact.employee_last_name = employee_data.get('last_name', '')
    contact.employee_title = employee_data.get('title', '')
    contact.employee_headline = employee_data.get('user_generated_headline', '')
    contact.employee_url = employee_data.get('url', '')
    contact.employee_canonical_url = employee_data.get('canonical_url', '')
    contact.employee_location = employee_data.get('location', '')
    contact.employee_country = employee_data.get('country', '')
    contact.employee_industry = employee_data.get('industry', '')
    contact.employee_connections_count = str(employee_data.get('connections_count', ''))
    contact.employee_summary = employee_data.get('summary', '')
    contact.employee_created = employee_data.get('created', '')
    contact.employee_last_updated = employee_data.get('last_updated', '')
    
    # Handle experience collection
    experience = employee_data.get('member_experience_collection', [])
    if isinstance(experience, list) and experience:
        contact.employee_experience = json.dumps(experience)
    
    # Handle education collection
    education = employee_data.get('member_education_collection', [])
    if isinstance(education, list) and education:
        contact.employee_education = json.dumps(education)

def detect_csv_columns(df: pd.DataFrame) -> Dict[str, str]:
    """Detect CSV column mappings"""
    columns = df.columns.str.lower().tolist()
    mapping = {}
    
    # Common column name variations
    name_variations = ['name', 'full_name', 'fullname', 'contact_name', 'person_name']
    email_variations = ['email', 'email_address', 'contact_email', 'e_mail']
    company_variations = ['company', 'company_name', 'organization', 'employer']
    website_variations = ['website', 'company_website', 'url', 'domain']
    
    # Find matches
    for col in columns:
        if any(var in col for var in name_variations):
            mapping['name'] = col
        elif any(var in col for var in email_variations):
            mapping['email'] = col
        elif any(var in col for var in company_variations):
            mapping['company_name'] = col
        elif any(var in col for var in website_variations):
            mapping['company_website'] = col
    
    return mapping

def enrich_contact(client: CoreSignalClient, contact: ContactData) -> ContactData:
    """Enrich a single contact with company and employee data"""
    start_time = time.time()
    
    try:
        # Search for companies
        logger.debug(f"Searching company: name='{contact.company_name}', website='{contact.company_website}'")
        companies = client.search_companies(contact.company_name, contact.company_website)
        company_data = None
        if companies:
            company_result = companies[0]
            # If it's an int, fetch the full company profile
            if isinstance(company_result, int):
                company_data = client.get_company_by_id(str(company_result))
            elif isinstance(company_result, dict) and 'id' in company_result:
                company_data = client.get_company_by_id(str(company_result['id']))
            elif isinstance(company_result, dict):
                company_data = company_result
            else:
                company_data = None
            if company_data:
                fill_company_fields(contact, company_data)
                contact.company_match_score = 1.0
                client.stats.company_found += 1
                logger.info(f"Found company: {company_data.get('name', 'Unknown')}")
        if not company_data:
            client.stats.company_not_found += 1
            logger.info(f"Company not found: {contact.company_name}")
        # --- Employee search with fallback strategies ---
        employee_data = None
        search_attempts = []
        # 1. Try full name + company
        search_attempts.append({
            'name': contact.name,
            'company_name': contact.company_name
        })
        # 2. Try first name + company
        if contact.name and ' ' in contact.name:
            first, *rest = contact.name.split()
            search_attempts.append({
                'name': first,
                'company_name': contact.company_name
            })
        # 3. Try last name + company
        if contact.name and ' ' in contact.name:
            *rest, last = contact.name.split()
            search_attempts.append({
                'name': last,
                'company_name': contact.company_name
            })
        # 4. Try full name only
        search_attempts.append({
            'name': contact.name,
            'company_name': ''
        })
        # 5. Try first name only
        if contact.name and ' ' in contact.name:
            first, *rest = contact.name.split()
            search_attempts.append({
                'name': first,
                'company_name': ''
            })
        # 6. Try last name only
        if contact.name and ' ' in contact.name:
            *rest, last = contact.name.split()
            search_attempts.append({
                'name': last,
                'company_name': ''
            })
        for attempt in search_attempts:
            logger.debug(f"Employee search attempt: {attempt}")
            employees = client.search_employees(
                name=attempt['name'],
                company_name=attempt['company_name']
            )
            logger.debug(f"Employee search result: {employees}")
            if employees:
                employee_result = employees[0]
                if isinstance(employee_result, int):
                    employee_data = client.get_employee_by_id(str(employee_result))
                elif isinstance(employee_result, dict) and 'id' in employee_result:
                    employee_data = client.get_employee_by_id(str(employee_result['id']))
                elif isinstance(employee_result, dict):
                    employee_data = employee_result
                else:
                    employee_data = None
                if employee_data:
                    fill_employee_fields(contact, employee_data)
                    contact.employee_match_score = 1.0
                    client.stats.employee_found += 1
                    logger.info(f"Found employee: {employee_data.get('name', 'Unknown')}")
                    break
        if not employee_data:
            client.stats.employee_not_found += 1
            logger.info(f"Employee not found: {contact.name}")
        # Set enrichment status
        if company_data or employee_data:
            contact.enrichment_status = "success"
            client.stats.successful_enrichments += 1
        else:
            contact.enrichment_status = "failed"
            contact.enrichment_error = "No data found"
            client.stats.failed_enrichments += 1
    
    except Exception as e:
        logger.error(f"Error enriching contact {contact.name}: {e}")
        contact.enrichment_status = "error"
        contact.enrichment_error = str(e)
        client.stats.failed_enrichments += 1
    
    finally:
        contact.api_calls_made = client.stats.api_calls
        processing_time = time.time() - start_time
        client.stats.processing_time += processing_time
    
    return contact

def main():
    parser = argparse.ArgumentParser(description="CoreSignal Employee Enrichment Pipeline")
    parser.add_argument("-i", "--input", required=True, help="Input CSV file with leads")
    parser.add_argument("-o", "--output", required=True, help="Output CSV file for enriched leads")
    parser.add_argument("-k", "--api_key", required=True, help="CoreSignal API key")
    parser.add_argument("--sample", type=int, help="Sample N rows from input")
    parser.add_argument("--rate_limit", type=float, default=1.0, help="Rate limit in seconds")
    
    args = parser.parse_args()
    
    # Validate input file
    if not os.path.exists(args.input):
        logger.error(f"Input file not found: {args.input}")
        sys.exit(1)
    
    # Load input data
    logger.info(f"Loading data from {args.input}")
    try:
        df = pd.read_csv(args.input)
        logger.info(f"Loaded {len(df)} rows from CSV")
        logger.info(f"CSV columns: {list(df.columns)}")
    except Exception as e:
        logger.error(f"Error loading CSV: {e}")
        sys.exit(1)

    # Force explicit column mapping for your CSV
    column_mapping = {
        'name': 'contact_full_name',
        'email': 'contact_email',
        'company_name': 'cs_company_name',
        'company_website': 'cs_company_website'
    }

    # Remove rows with missing name or email
    df = df[df['contact_full_name'].notna() & df['contact_email'].notna()]

    if args.sample:
        df = df.head(args.sample)
        logger.info(f"Using first {len(df)} rows")
    
    # Initialize client
    client = CoreSignalClient(args.api_key, rate_limit=args.rate_limit)
    client.stats.total_contacts = len(df)
    
    # Process contacts
    enriched_contacts = []
    
    for idx, row in df.iterrows():
        # Get values using detected column mappings
        name = row.get(column_mapping.get('name', 'name'), '')
        email = row.get(column_mapping.get('email', 'email'), '')
        company_name = row.get(column_mapping.get('company_name', 'company_name'), '')
        company_website = row.get(column_mapping.get('company_website', 'company_website'), '')
        
        logger.info(f"Processing contact {idx + 1}/{len(df)}: {name}")
        
        # Create contact from row
        contact = ContactData(
            name=str(name) if pd.notna(name) else "",
            email=str(email) if pd.notna(email) else "",
            company_name=str(company_name) if pd.notna(company_name) else "",
            company_website=str(company_website) if pd.notna(company_website) else ""
        )
        
        if not contact.is_valid():
            logger.warning(f"Invalid contact data: name='{contact.name}', email='{contact.email}'")
            contact.enrichment_status = "invalid"
            contact.enrichment_error = "Missing required fields (name or email)"
            enriched_contacts.append(asdict(contact))
            continue
        
        # Enrich contact
        enriched_contact = enrich_contact(client, contact)
        enriched_contacts.append(asdict(enriched_contact))
        
        # Progress update
        if (idx + 1) % 10 == 0:
            logger.info(f"Processed {idx + 1} contacts")
    
    # Save results
    logger.info(f"Saving results to {args.output}")
    try:
        output_df = pd.DataFrame(enriched_contacts)
        output_df.to_csv(args.output, index=False)
        logger.info(f"Results saved successfully")
    except Exception as e:
        logger.error(f"Error saving results: {e}")
        sys.exit(1)
    
    # Print summary
    print(client.stats.summary())

if __name__ == "__main__":
    main()