import requests
import json
import csv
import pandas as pd
import time
import re
from typing import Dict, List, Optional, Any
import logging
from urllib.parse import urlparse

# TODO: Replace with the actual base URL of your API
BASE_URL = 'https://mademarket.co/api'
# Set your API key here
HEADERS = {
    'X-MM-API-KEY': '4DKARTF6YJR7',
    'Content-Type': 'application/json',
}

COMPANY_NAME = 'AssessPrep'
DISTRIBUTION_NAME = '2025 ISTE'

# --- USER CONFIG ---
CORESIGNAL_API_KEY = "rBLZ9TafWmj4OAiUn4Pb18unIv8XEUkt"
# --- LOGGING ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- UTILITY FUNCTIONS ---
def safe_str(value: Any) -> str:
    # Handle None
    if value is None:
        return ""
    # Handle numpy/pandas arrays or Series
    if hasattr(value, 'size') and hasattr(value, '__getitem__'):
        try:
            if value.size == 0:
                return ""
            if value.size == 1:
                value = value.item()
            else:
                return ', '.join([safe_str(v) for v in value])
        except Exception:
            pass
    # Handle lists
    if isinstance(value, list):
        if len(value) == 0:
            return ""
        return ', '.join([safe_str(v) for v in value])
    # Handle NaN
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()

def normalize_company_name(name: str) -> str:
    """Normalize company name for better matching."""
    if not name:
        return ""
    # Remove common suffixes and normalize
    name = safe_str(name).lower()
    suffixes = [' inc', ' llc', ' ltd', ' corp', ' corporation', ' company', ' co']
    for suffix in suffixes:
        if name.endswith(suffix):
            name = name[:-len(suffix)]
    return name.strip()

def extract_domain_from_url(url: str) -> str:
    """Extract domain from URL, handling various formats."""
    if not url:
        return ""
    url = safe_str(url)
    # Remove protocol and www
    url = re.sub(r'^https?://(www\.)?', '', url)
    # Remove path and query parameters
    url = url.split('/')[0]
    return url.lower().strip()

def safe_join_list(data_list: List, max_items: int = 10) -> str:
    """Safely join a list of items, handling None and empty values."""
    if not data_list:
        return ""
    # Filter out None and empty values
    valid_items = [safe_str(item) for item in data_list if item]
    # Limit to max_items
    valid_items = valid_items[:max_items]
    return ' | '.join(valid_items)

def safe_get_nested(data: Dict, *keys, default=""):
    """Safely get nested dictionary values."""
    current = data
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return default
    return safe_str(current) if current is not None else default

# --- MADEMARKET DATA EXTRACTION ---
def get_distribution_id(distribution_name):
    url = f"{BASE_URL}/v2/distributions/report_recipients.json"
    logger.info(f"[MadeMarket] GET {url}")
    response = requests.get(url, headers=HEADERS)
    logger.info(f"[MadeMarket] Response status: {response.status_code}")
    response.raise_for_status()
    data = response.json()
    for rec in data.get('distributions_recipients', []):
        dist = rec.get('distribution', {})
        if dist.get('name', '').lower() == distribution_name.lower():
            return dist.get('id')
    return None

def get_distribution_recipients(distribution_id):
    url = f"{BASE_URL}/v2/distributions/report_recipients.json"
    params = {"distribution_ids[]": distribution_id}
    logger.info(f"[MadeMarket] GET {url} | params: {params}")
    response = requests.get(url, headers=HEADERS, params=params)
    logger.info(f"[MadeMarket] Response status: {response.status_code}")
    response.raise_for_status()
    data = response.json()
    return data.get('distributions_recipients', [])

def get_contact_details(contact_id, contact_cache):
    if not contact_id:
        return None
    if contact_id in contact_cache:
        return contact_cache[contact_id]
    url = f"{BASE_URL}/v2/contacts/{contact_id}.json"
    logger.info(f"[MadeMarket] GET {url}")
    response = requests.get(url, headers=HEADERS)
    logger.info(f"[MadeMarket] Response status: {response.status_code}")
    if response.status_code != 200:
        return None
    data = response.json()
    contact_cache[contact_id] = data.get('contact', data)
    return contact_cache[contact_id]

def get_firm_details(firm_detail_id, firm_cache):
    if not firm_detail_id:
        return None
    if firm_detail_id in firm_cache:
        return firm_cache[firm_detail_id]
    url = f"{BASE_URL}/v2/firm_details/{firm_detail_id}.json"
    logger.info(f"[MadeMarket] GET {url}")
    response = requests.get(url, headers=HEADERS)
    logger.info(f"[MadeMarket] Response status: {response.status_code}")
    if response.status_code != 200:
        return None
    data = response.json()
    firm_cache[firm_detail_id] = data.get('firm_detail', data)
    return firm_cache[firm_detail_id]

def flatten_mademarket_row(recipient, contact, firm):
    # Flatten all relevant fields for CSV
    row = {
        # Recipient fields
        'recipient_email': safe_str(recipient.get('email')),
        'recipient_view_count': recipient.get('view_count', 0),
        'recipient_is_bounced': recipient.get('is_bounced', False),
        'email_opened': 1 if recipient.get('view_count', 0) > 0 else 0,
        'distribution_id': safe_get_nested(recipient, 'distribution', 'id'),
        'distribution_name': safe_get_nested(recipient, 'distribution', 'name'),
        'distribution_sent_at': safe_get_nested(recipient, 'distribution', 'sent_at'),
    }
    # Contact fields
    if contact:
        for k, v in contact.items():
            row[f'contact_{k}'] = safe_str(v)
    # Firm fields
    if firm:
        for k, v in firm.items():
            row[f'firm_{k}'] = safe_str(v)
    return row

# --- CORESIGNAL ENRICHER ---
class CoreSignalEnricher:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.coresignal.com/cdapi/v2"
        self.headers = {
            "apikey": api_key,
            "Content-Type": "application/json"
        }
        self.rate_limit_delay = 1  # seconds between requests
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
    def make_request(self, url: str, params: Dict = None, method: str = "GET") -> Optional[Dict]:
        if not self.api_key:
            logger.error("[CoreSignal] API key is missing! Aborting request.")
            return None
        logger.info(f"[CoreSignal] {method} {url} | params: {params}")
        logger.info(f"[CoreSignal] Request headers: {self.session.headers}")
        try:
            if method == "POST":
                response = self.session.post(url, json=params, timeout=30)
            else:
                response = self.session.get(url, params=params, timeout=30)
            logger.info(f"[CoreSignal] Response {response.status_code}")
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                logger.error(f"[CoreSignal] 401 Unauthorized. Check your API key and 'apikey' header format. Response: {response.text}")
                raise Exception("CoreSignal API 401 Unauthorized. Check your API key and 'apikey' header format.")
            elif response.status_code == 429:
                logger.warning("Rate limit hit, waiting 60 seconds...")
                time.sleep(60)
                return self.make_request(url, params, method)
            else:
                logger.error(f"API request failed: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Request error: {e}")
            return None

    def search_person(self, email: str = None, name: str = None, company: str = None) -> Optional[Dict]:
        """Search for person (member) with multiple strategies using v2 API and correct payload as per docs."""
        strategies = []
        # Strategy 1: Search by email (most accurate)
        if email and safe_str(email):
            strategies.append(("email", {"email": safe_str(email)}))
        # Strategy 2: Search by full name and company name
        if name and company and safe_str(name) and safe_str(company):
            strategies.append(("name_company", {"full_name": safe_str(name), "company_name": safe_str(company)}))
        # Strategy 3: Search by full name only
        if name and safe_str(name):
            strategies.append(("name_only", {"full_name": safe_str(name)}))
        for strategy_name, params in strategies:
            try:
                url = f"{self.base_url}/member/search/filter"
                logger.info(f"[CoreSignal][PersonSearch] Strategy: {strategy_name} | Payload: {params}")
                data = self.make_request(url, params, method="POST")
                logger.info(f"[CoreSignal][PersonSearch] Response for {strategy_name}: {str(data)[:500]}..." if data and len(str(data)) > 500 else f"[CoreSignal][PersonSearch] Response for {strategy_name}: {data}")
                if isinstance(data, list) and len(data) > 0:
                    logger.info(f"Person found using strategy: {strategy_name}")
                    time.sleep(self.rate_limit_delay)
                    return data[0]
                elif isinstance(data, dict) and data:
                    logger.info(f"Person found using strategy: {strategy_name}")
                    time.sleep(self.rate_limit_delay)
                    return data
                elif data is not None:
                    logger.warning(f"[CoreSignal][PersonSearch] Unexpected response type: {type(data)} | value: {data}")
            except Exception as e:
                logger.error(f"Error in person search strategy {strategy_name}: {e}")
                continue
        logger.info(f"No person found for email: {email}, name: {name}, company: {company}")
        return None

    def search_company(self, company_name: str = None, website: str = None) -> Optional[Dict]:
        """Search for company with multiple strategies using v2 API."""
        strategies = []
        # Strategy 1: Search by website domain
        if website and safe_str(website):
            domain = extract_domain_from_url(website)
            if domain:
                strategies.append(("website", {"website": domain}))
        # Strategy 2: Search by company name
        if company_name and safe_str(company_name):
            normalized_name = normalize_company_name(company_name)
            if normalized_name:
                strategies.append(("name", {"name": normalized_name}))
                # Also try original name
                strategies.append(("name_original", {"name": safe_str(company_name)}))
        for strategy_name, params in strategies:
            try:
                url = f"{self.base_url}/company_base/search/filter"
                data = self.make_request(url, params, method="POST")
                if data and isinstance(data, list) and len(data) > 0:
                    logger.info(f"Company found using strategy: {strategy_name}")
                    time.sleep(self.rate_limit_delay)
                    return data[0]
                elif isinstance(data, int):
                    logger.info(f"Company ID found using strategy: {strategy_name}: {data}")
                    return data
                elif data and isinstance(data, dict) and data:
                    logger.info(f"Company found using strategy: {strategy_name}")
                    time.sleep(self.rate_limit_delay)
                    return data
                else:
                    logger.warning(f"Unexpected company search result type: {type(data)} | value: {data}")
            except Exception as e:
                logger.error(f"Error in company search strategy {strategy_name}: {e}")
                continue
        logger.info(f"No company found for name: {company_name}, website: {website}")
        return None

    def get_person_details(self, person_id: str) -> Optional[Dict]:
        """Get detailed person (member) information using v2 API."""
        try:
            url = f"{self.base_url}/member/collect/{person_id}"
            data = self.make_request(url)
            if data:
                time.sleep(self.rate_limit_delay)
                return data
            return None
        except Exception as e:
            logger.error(f"Error getting person details: {e}")
            return None

    def get_company_details(self, company_id: str) -> Optional[Dict]:
        """Get detailed company information using v2 Multi-source API."""
        try:
            url = f"{self.base_url}/company_multi_source/collect/{company_id}"
            data = self.make_request(url)
            if data:
                time.sleep(self.rate_limit_delay)
                return data
            return None
        except Exception as e:
            logger.error(f"Error getting company details: {e}")
            return None

    def extract_person_fields(self, person_data: Dict) -> Dict:
        """Extract all possible person fields from Coresignal data."""
        enriched_data = {}
        
        # Basic identifiers
        enriched_data.update({
            'cs_person_id': safe_str(person_data.get('id')),
            'cs_person_parent_id': safe_str(person_data.get('parent_id')),
            'cs_person_public_profile_id': safe_str(person_data.get('public_profile_id')),
            'cs_person_professional_network_url': safe_str(person_data.get('professional_network_url')),
        })
        
        # Personal information
        enriched_data.update({
            'cs_person_full_name': safe_str(person_data.get('full_name')),
            'cs_person_first_name': safe_str(person_data.get('first_name')),
            'cs_person_first_name_initial': safe_str(person_data.get('first_name_initial')),
            'cs_person_middle_name': safe_str(person_data.get('middle_name')),
            'cs_person_last_name': safe_str(person_data.get('last_name')),
            'cs_person_last_name_initial': safe_str(person_data.get('last_name_initial')),
            'cs_person_maiden_name': safe_str(person_data.get('maiden_name')),
            'cs_person_nickname': safe_str(person_data.get('nickname')),
            'cs_person_headline': safe_str(person_data.get('headline')),
            'cs_person_summary': safe_str(person_data.get('summary')),
            'cs_person_location': safe_str(person_data.get('location')),
            'cs_person_country': safe_str(person_data.get('country')),
            'cs_person_city': safe_str(person_data.get('city')),
            'cs_person_state': safe_str(person_data.get('state')),
            'cs_person_zip_code': safe_str(person_data.get('zip_code')),
            'cs_person_industry': safe_str(person_data.get('industry')),
            'cs_person_skills': safe_join_list(person_data.get('skills', []), 20),
            'cs_person_languages': safe_join_list(person_data.get('languages', []), 10),
            'cs_person_certifications': safe_join_list(person_data.get('certifications', []), 10),
            'cs_person_volunteer_experience': safe_join_list(person_data.get('volunteer_experience', []), 5),
            'cs_person_honors_awards': safe_join_list(person_data.get('honors_awards', []), 10),
            'cs_person_patents': safe_join_list(person_data.get('patents', []), 10),
            'cs_person_publications': safe_join_list(person_data.get('publications', []), 10),
            'cs_person_courses': safe_join_list(person_data.get('courses', []), 10),
            'cs_person_projects': safe_join_list(person_data.get('projects', []), 10),
            'cs_person_test_scores': safe_join_list(person_data.get('test_scores', []), 5),
            'cs_person_organizations': safe_join_list(person_data.get('organizations', []), 10),
            'cs_person_people_also_viewed': safe_join_list(person_data.get('people_also_viewed', []), 10),
            'cs_person_connections': safe_str(person_data.get('connections')),
            'cs_person_followers': safe_str(person_data.get('followers')),
            'cs_person_profile_picture_url': safe_str(person_data.get('profile_picture_url')),
            'cs_person_banner_url': safe_str(person_data.get('banner_url')),
        })
        
        # Current position
        current_position = person_data.get('current_position', {})
        enriched_data.update({
            'cs_person_current_title': safe_str(current_position.get('title')),
            'cs_person_current_company': safe_str(current_position.get('company')),
            'cs_person_current_company_id': safe_str(current_position.get('company_id')),
            'cs_person_current_location': safe_str(current_position.get('location')),
            'cs_person_current_start_date': safe_str(current_position.get('start_date')),
            'cs_person_current_end_date': safe_str(current_position.get('end_date')),
            'cs_person_current_description': safe_str(current_position.get('description')),
        })
        
        # Experience (most recent 5)
        experience = person_data.get('experience', [])
        for i, exp in enumerate(experience[:5]):
            enriched_data.update({
                f'cs_person_exp_{i+1}_title': safe_str(exp.get('title')),
                f'cs_person_exp_{i+1}_company': safe_str(exp.get('company')),
                f'cs_person_exp_{i+1}_company_id': safe_str(exp.get('company_id')),
                f'cs_person_exp_{i+1}_location': safe_str(exp.get('location')),
                f'cs_person_exp_{i+1}_start_date': safe_str(exp.get('start_date')),
                f'cs_person_exp_{i+1}_end_date': safe_str(exp.get('end_date')),
                f'cs_person_exp_{i+1}_description': safe_str(exp.get('description')),
            })
        
        # Education (most recent 3)
        education = person_data.get('education', [])
        for i, edu in enumerate(education[:3]):
            enriched_data.update({
                f'cs_person_edu_{i+1}_school': safe_str(edu.get('school')),
                f'cs_person_edu_{i+1}_degree': safe_str(edu.get('degree')),
                f'cs_person_edu_{i+1}_field_of_study': safe_str(edu.get('field_of_study')),
                f'cs_person_edu_{i+1}_start_date': safe_str(edu.get('start_date')),
                f'cs_person_edu_{i+1}_end_date': safe_str(edu.get('end_date')),
                f'cs_person_edu_{i+1}_description': safe_str(edu.get('description')),
            })
        
        # Metadata
        enriched_data.update({
            'cs_person_created_at': safe_str(person_data.get('created_at')),
            'cs_person_updated_at': safe_str(person_data.get('updated_at')),
            'cs_person_checked_at': safe_str(person_data.get('checked_at')),
            'cs_person_changed_at': safe_str(person_data.get('changed_at')),
            'cs_person_is_deleted': safe_str(person_data.get('is_deleted')),
            'cs_person_is_parent': safe_str(person_data.get('is_parent')),
        })
        
        return enriched_data

    def extract_company_fields(self, company_data: Dict) -> Dict:
        """Extract all possible company fields from Coresignal data."""
        enriched_data = {}
        
        # Basic identifiers
        enriched_data.update({
            'cs_company_id': safe_str(company_data.get('id')),
            'cs_company_parent_id': safe_str(company_data.get('parent_id')),
            'cs_company_professional_network_url': safe_str(company_data.get('professional_network_url')),
        })
        
        # Company information
        enriched_data.update({
            'cs_company_name': safe_str(company_data.get('name')),
            'cs_company_legal_name': safe_str(company_data.get('legal_name')),
            'cs_company_alternative_names': safe_join_list(company_data.get('alternative_names', []), 10),
            'cs_company_website': safe_str(company_data.get('website')),
            'cs_company_description': safe_str(company_data.get('description')),
            'cs_company_summary': safe_str(company_data.get('summary')),
            'cs_company_industry': safe_str(company_data.get('industry')),
            'cs_company_type': safe_str(company_data.get('type')),
            'cs_company_size': safe_str(company_data.get('size')),
            'cs_company_size_range': safe_str(company_data.get('size_range')),
            'cs_company_employees_count': safe_str(company_data.get('employees_count')),
            'cs_company_founded_year': safe_str(company_data.get('founded')),
            'cs_company_headquarters': safe_str(company_data.get('headquarters')),
            'cs_company_locations': safe_join_list(company_data.get('locations', []), 20),
            'cs_company_specialties': safe_join_list(company_data.get('specialties', []), 20),
            'cs_company_technologies': safe_join_list(company_data.get('technologies', []), 20),
            'cs_company_stock_symbol': safe_str(company_data.get('stock_symbol')),
            'cs_company_revenue': safe_str(company_data.get('revenue')),
            'cs_company_revenue_range': safe_str(company_data.get('revenue_range')),
            'cs_company_funding': safe_str(company_data.get('total_funding')),
            'cs_company_funding_rounds': safe_str(company_data.get('funding_rounds')),
            'cs_company_investors': safe_join_list(company_data.get('investors', []), 20),
            'cs_company_phone': safe_str(company_data.get('phone')),
            'cs_company_email': safe_str(company_data.get('email')),
            'cs_company_logo_url': safe_str(company_data.get('logo_url')),
            'cs_company_banner_url': safe_str(company_data.get('banner_url')),
        })
        
        # Social media
        social_media = company_data.get('social_media', {})
        if social_media:
            enriched_data.update({
                'cs_company_linkedin_url': safe_str(social_media.get('linkedin')),
                'cs_company_twitter_url': safe_str(social_media.get('twitter')),
                'cs_company_facebook_url': safe_str(social_media.get('facebook')),
                'cs_company_instagram_url': safe_str(social_media.get('instagram')),
                'cs_company_youtube_url': safe_str(social_media.get('youtube')),
            })
        
        # Metadata
        enriched_data.update({
            'cs_company_created_at': safe_str(company_data.get('created_at')),
            'cs_company_updated_at': safe_str(company_data.get('updated_at')),
            'cs_company_checked_at': safe_str(company_data.get('checked_at')),
            'cs_company_changed_at': safe_str(company_data.get('changed_at')),
            'cs_company_is_deleted': safe_str(company_data.get('is_deleted')),
            'cs_company_is_parent': safe_str(company_data.get('is_parent')),
        })
        
        return enriched_data

    def enrich_person_data(self, row: pd.Series) -> Dict:
        """Enrich person data with comprehensive Coresignal information."""
        enriched_data = {}
        
        # Get basic info from row
        email = safe_str(row.get('contact_email'))
        name = safe_str(row.get('contact_full_name'))
        company = safe_str(row.get('firm_name'))
        
        # Search for person
        person_data = self.search_person(email=email, name=name, company=company)
        
        if person_data:
            # Get detailed person information
            person_id = person_data.get('id')
            if person_id:
                detailed_person = self.get_person_details(person_id)
                if detailed_person:
                    person_data.update(detailed_person)
            
            # Extract all possible fields
            enriched_data.update(self.extract_person_fields(person_data))
            logger.info(f"Person enriched: {name} ({email})")
        else:
            logger.info(f"No person data found for: {name} ({email})")
        
        return enriched_data

    def enrich_company_data(self, row: pd.Series) -> Dict:
        """Enrich company data with comprehensive Coresignal information."""
        enriched_data = {}
        company_name = safe_str(row.get('firm_name'))
        website = safe_str(row.get('firm_website'))
        try:
            company_data = self.search_company(company_name=company_name, website=website)
            company_id = None
            if isinstance(company_data, dict):
                company_id = company_data.get('id')
            elif isinstance(company_data, int):
                company_id = company_data
            if company_id:
                detailed_company = self.get_company_details(company_id)
                if detailed_company:
                    if isinstance(company_data, dict):
                        company_data.update(detailed_company)
                    else:
                        company_data = detailed_company
            if isinstance(company_data, dict):
                enriched_data.update(self.extract_company_fields(company_data))
                logger.info(f"Company enriched: {company_name}")
            else:
                logger.info(f"No company data found for: {company_name}")
        except Exception as e:
            logger.error(f"Error enriching company data for {company_name}: {e}")
        return enriched_data

    def process_csv(self, input_file: str, output_file: str):
        """Process CSV file with comprehensive enrichment."""
        try:
            df = pd.read_csv(input_file)
            logger.info(f"Loaded {len(df)} rows from {input_file}")
            
            enriched_rows = []
            person_matches = 0
            company_matches = 0
            
            for index, row in df.iterrows():
                logger.info(f"Processing row {index + 1}/{len(df)}: {safe_str(row.get('contact_email', 'N/A'))}")
                
                # Start with original row data
                enriched_row = row.to_dict()
                
                # Enrich person data
                person_enrichment = self.enrich_person_data(row)
                logger.info(f"[PersonEnrichment] For {row.get('contact_email')}: {person_enrichment}")
                enriched_row.update(person_enrichment)
                if person_enrichment.get('cs_person_id'):
                    person_matches += 1
                else:
                    logger.warning(f"[PersonEnrichment] No person enrichment fields found for row {index + 1} ({row.get('contact_email')})")
                
                # Enrich company data
                company_enrichment = self.enrich_company_data(row)
                enriched_row.update(company_enrichment)
                if company_enrichment.get('cs_company_id'):
                    company_matches += 1
                
                enriched_rows.append(enriched_row)
                
                # Rate limiting
                time.sleep(self.rate_limit_delay)
            
            # Create enriched DataFrame
            enriched_df = pd.DataFrame(enriched_rows)
            enriched_df.to_csv(output_file, index=False)
            
            logger.info(f"Enriched data saved to {output_file}")
            logger.info(f"Summary:")
            logger.info(f"  Total rows processed: {len(enriched_rows)}")
            logger.info(f"  Person matches found: {person_matches}")
            logger.info(f"  Company matches found: {company_matches}")
            logger.info(f"  Person match rate: {person_matches/len(enriched_rows)*100:.1f}%")
            logger.info(f"  Company match rate: {company_matches/len(enriched_rows)*100:.1f}%")
            
        except Exception as e:
            logger.error(f"Error processing CSV: {e}")
            raise

# --- PIPELINE MAIN ---
def test_distribution_exists():
    """Test if the distribution exists and list all available distributions."""
    logger.info("Testing MadeMarket API connection and listing distributions...")
    url = f"{BASE_URL}/v2/distributions/report_recipients.json"
    logger.info(f"[MadeMarket] GET {url}")
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        logger.info(f"[MadeMarket] Response status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            distributions = []
            for rec in data.get('distributions_recipients', []):
                dist = rec.get('distribution', {})
                dist_name = dist.get('name', 'Unknown')
                dist_id = dist.get('id')
                distributions.append(f"'{dist_name}' (ID: {dist_id})")
            
            logger.info(f"Found {len(distributions)} distributions:")
            for dist in distributions:
                logger.info(f"  - {dist}")
            
            # Check if our target distribution exists
            target_exists = any(DISTRIBUTION_NAME.lower() in dist.lower() for dist in distributions)
            if target_exists:
                logger.info(f"✅ Distribution '{DISTRIBUTION_NAME}' found!")
            else:
                logger.warning(f"❌ Distribution '{DISTRIBUTION_NAME}' NOT found!")
                logger.info("Available distributions that might match:")
                for dist in distributions:
                    if 'iste' in dist.lower() or '2025' in dist:
                        logger.info(f"  - {dist}")
        else:
            logger.error(f"MadeMarket API error: {response.status_code} - {response.text}")
    except Exception as e:
        logger.error(f"Error testing MadeMarket API: {e}")

def pipeline_main():
    try:
        # Test distribution first
        test_distribution_exists()
        
        logger.info("Step 1: Generating distribution report...")
        distribution_id = get_distribution_id(DISTRIBUTION_NAME)
        if not distribution_id:
            logger.error(f"Distribution '{DISTRIBUTION_NAME}' not found")
            return
        recipients = get_distribution_recipients(distribution_id)
        if not recipients:
            logger.error("No recipients found for distribution")
            return
        firm_cache = {}
        contact_cache = {}
        rows = []
        for rec in recipients:
            contact_id = rec.get('contact', {}).get('id')
            firm_detail_id = rec.get('contact', {}).get('firm_detail_id')
            contact = get_contact_details(contact_id, contact_cache)
            firm = get_firm_details(firm_detail_id, firm_cache)
            row = flatten_mademarket_row(rec, contact, firm)
            rows.append(row)
        df = pd.DataFrame(rows)
        try:
            df.to_csv("distribution_report.csv", index=False)
            logger.info(f"Distribution report saved with {len(rows)} rows")
        except PermissionError:
            logger.error("[ERROR] Could not write to 'distribution_report.csv'. Please close the file if it is open in another program and try again.")
            return
        logger.info("Step 2: Enriching with CoreSignal data...")
        enricher = CoreSignalEnricher(CORESIGNAL_API_KEY)
        enricher.process_csv("distribution_report.csv", "enriched_data.csv")
        logger.info("Pipeline completed successfully!")
    except Exception as e:
        logger.error(f"Error in pipeline: {e}")
        raise

if __name__ == "__main__":
    pipeline_main() 