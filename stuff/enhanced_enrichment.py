import pandas as pd
import logging
import requests
import time
from typing import Dict, List, Optional, Any
import json
import difflib

# --- CONFIGURATION ---
CORESIGNAL_EMPLOYEE_API_KEY = "PTbCfCGO366FUpf10kPDGzeROW9zMIyR"
CORESIGNAL_BASE_URL = "https://api.coresignal.com/cdapi/v2"

# --- LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def safe_str(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    if isinstance(val, list):
        return ' | '.join(map(safe_str, val))
    return str(val).strip()

def flatten_list_of_dicts(lst, prefix, fields, max_items=10):
    out = {}
    for i, item in enumerate(lst[:max_items]):
        for f in fields:
            out[f"{prefix}_{i+1}_{f}"] = safe_str(item.get(f, ""))
    return out

def string_similarity(a, b):
    if not a or not b:
        return 0.0
    return difflib.SequenceMatcher(None, a.lower(), b.lower()).ratio()

def company_match(candidate_exp, target_company):
    if not target_company or not candidate_exp:
        return False
    target = target_company.lower().replace(',', '').replace('.', '').strip()
    for exp in candidate_exp:
        comp = exp.get('company_name', '')
        if comp:
            comp_clean = comp.lower().replace(',', '').replace('.', '').strip()
            if target in comp_clean or comp_clean in target:
                return True
            if string_similarity(target, comp_clean) >= 0.8:
                return True
    return False

class CoreSignalEmployeeEnricher:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = CORESIGNAL_BASE_URL
        self.headers = {
            "apikey": api_key,
            "Content-Type": "application/json"
        }
        self.rate_limit_delay = 2.0  # seconds between requests
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.employee_search_url = f"{self.base_url}/employee_clean/search/es_dsl"
        self.employee_collect_url = f"{self.base_url}/employee_clean/collect"
        
    def make_request(self, url: str, method: str = "GET", **kwargs) -> Optional[Dict]:
        """Make a robust API request with error handling."""
        if not self.api_key:
            logger.error("[CoreSignal Employee] API key is missing! Aborting request.")
            return None
        logger.info(f"[CoreSignal Employee] {method} {url}")
        logger.info(f"[CoreSignal Employee] Request headers: {self.session.headers}")
        try:
            if method == "GET":
                response = self.session.get(url, timeout=30, **kwargs)
            else:
                response = self.session.post(url, timeout=30, **kwargs)
            logger.info(f"[CoreSignal Employee] Response {response.status_code}")
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                logger.error(f"[CoreSignal Employee] 401 Unauthorized. Check your API key. Response: {response.text}")
                raise Exception("CoreSignal Employee API 401 Unauthorized. Check your API key.")
            elif response.status_code == 429:
                logger.warning("Rate limit hit, waiting 60 seconds...")
                time.sleep(60)
                return self.make_request(url, method, **kwargs)
            else:
                logger.error(f"API request failed: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Request error: {e}")
            return None

    def normalize_company_name(self, name: str) -> str:
        if not name:
            return ""
        name = name.lower().strip()
        suffixes = [
            ' inc', ' llc', ' ltd', ' corp', ' corporation', ' company', ' co', '.', ',', ' inc.', ' llc.', ' ltd.', ' corp.', ' corporation.', ' company.', ' co.'
        ]
        for suffix in suffixes:
            if name.endswith(suffix):
                name = name[: -len(suffix)]
        return name.strip()

    def string_similarity(self, a: str, b: str) -> float:
        if not a or not b:
            return 0.0
        return difflib.SequenceMatcher(None, a.lower(), b.lower()).ratio()

    def company_match(self, candidate_exp: list, target_company: str) -> bool:
        """Fuzzy company matching using string similarity."""
        if not target_company or not candidate_exp:
            return False
        target = target_company.lower().replace(',', '').replace('.', '').strip()
        for exp in candidate_exp:
            comp = exp.get('company_name', '')
            if comp:
                comp_clean = comp.lower().replace(',', '').replace('.', '').strip()
                # Check substring first (faster)
                if target in comp_clean or comp_clean in target:
                    return True
                # Then check fuzzy similarity
                similarity = self.string_similarity(target, comp_clean)
                if similarity >= 0.8:  # 80% similarity threshold for companies
                    return True
        return False

    def additional_field_match(self, candidate: dict, target_name: str, target_company: str) -> dict:
        """Check additional fields for disambiguation: location, title, etc."""
        match_info = {
            'location_match': False,
            'title_match': False,
            'education_match': False,
            'score': 0.0
        }
        
        # Location matching (if available in your target data)
        candidate_location = candidate.get('location_full', '')
        if candidate_location:
            # Could compare with target location if available
            pass
        
        # Title matching - check if candidate's active title is relevant
        candidate_title = candidate.get('active_experience_title', '')
        if candidate_title and target_company:
            # Check if title suggests they work at the target company
            title_lower = candidate_title.lower()
            company_lower = target_company.lower()
            if any(word in title_lower for word in company_lower.split()):
                match_info['title_match'] = True
                match_info['score'] += 0.1
        
        # Education matching - check if education is relevant to the industry
        education = candidate.get('education', [])
        if education:
            for edu in education:
                institution = edu.get('institution_name', '')
                if institution and target_company:
                    # Check if education institution is related to the company
                    similarity = self.string_similarity(institution, target_company)
                    if similarity >= 0.7:
                        match_info['education_match'] = True
                        match_info['score'] += 0.05
                        break
        
        return match_info

    def get_best_available_match(self, candidates: list, target_name: str, target_company: str = None) -> tuple:
        """Get the best available match even if not perfect, with confidence score."""
        best_match = None
        best_score = 0.0
        best_info = {}
        
        for candidate_id in candidates[:15]:  # Check top 15
            candidate = self.get_employee_details(str(candidate_id))
            if not candidate:
                continue
                
            cand_name = candidate.get('full_name', '')
            cand_exp = candidate.get('experience', [])
            
            # Calculate match score
            name_sim = self.string_similarity(target_name, cand_name)
            company_ok = self.company_match(cand_exp, target_company) if target_company else True
            
            # Additional field matching
            additional_info = self.additional_field_match(candidate, target_name, target_company)
            
            # Calculate total score
            score = name_sim + additional_info['score']
            if company_ok:
                score += 0.2  # Bonus for company match
            
            if score > best_score:
                best_score = score
                best_match = candidate_id
                best_info = {
                    'name': cand_name,
                    'name_similarity': name_sim,
                    'company_match': company_ok,
                    'additional_score': additional_info['score'],
                    'total_score': score
                }
        
        return best_match, best_score, best_info

    def search_employee(self, full_name: str = None, company_name: str = None, use_fallback: bool = True, company_group: str = None) -> Optional[str]:
        """Search for employee with batch optimization and fallback strategies."""
        if not full_name:
            logger.warning("[SEARCH] No full_name provided for employee search.")
            return None
        # Build ES-DSL query
        must_clauses = [{"match": {"full_name": full_name}}]
        if company_name:
            must_clauses.append({"match": {"experience.company_name": company_name}})
        dsl_query = {"query": {"bool": {"must": must_clauses}}, "sort": ["_score"]}
        url = f"{self.employee_search_url}?size=25"
        logger.info(f"[SEARCH] Query: {json.dumps(dsl_query)}")
        try:
            response = self.session.post(url, json=dsl_query, timeout=30)
            logger.info(f"[SEARCH] Response {response.status_code}")
            if response.status_code != 200:
                logger.warning(f"[SEARCH] API error: {response.text}")
                return None
            results = response.json()
            if not isinstance(results, list) or not results:
                logger.info(f"[SEARCH] No results for: {full_name}")
                return None
            # Check top 10 results for strong matches first
            for idx, candidate_id in enumerate(results[:10]):
                logger.info(f"[SEARCH] Checking candidate {idx+1}: ID={candidate_id}")
                candidate = self.get_employee_details(str(candidate_id))
                if not candidate:
                    logger.info(f"[SEARCH] Candidate {candidate_id} - no data")
                    continue
                cand_name = candidate.get('full_name', '')
                cand_exp = candidate.get('experience', [])
                cand_company = cand_exp[0].get('company_name', '') if cand_exp else ''
                name_sim = self.string_similarity(full_name, cand_name)
                company_ok = self.company_match(cand_exp, company_name) if company_name else True
                additional_info = self.additional_field_match(candidate, full_name, company_name)
                total_score = name_sim + additional_info['score']
                logger.info(f"[SEARCH] Candidate: name='{cand_name}', company='{cand_company}'")
                logger.info(f"[SEARCH] Scores: name_sim={name_sim:.2f}, company_match={company_ok}, additional={additional_info['score']:.2f}, total={total_score:.2f}")
                if name_sim >= 0.85 and company_ok:
                    logger.info(f"[SEARCH] STRONG MATCH: {cand_name} (ID: {candidate_id})")
                    return str(candidate_id)
                else:
                    logger.info(f"[SEARCH] Not a strong match")
            # Fallback: get best available match if enabled
            if use_fallback:
                logger.info(f"[SEARCH] No strong match found, using fallback strategy...")
                best_match, best_score, best_info = self.get_best_available_match(results, full_name, company_name)
                if best_match and best_score >= 0.6:
                    logger.warning(f"[SEARCH] FALLBACK MATCH: {best_info['name']} (ID: {best_match})")
                    logger.warning(f"[SEARCH] Confidence: {best_score:.2f} - This may not be the exact person!")
                    return str(best_match)
                else:
                    logger.info(f"[SEARCH] No suitable fallback match found (best score: {best_score:.2f})")
            logger.info(f"[SEARCH] No match found for: {full_name} at {company_name}")
            return None
        except Exception as e:
            logger.error(f"[SEARCH] Exception: {e}")
            return None

    def get_employee_details(self, employee_id: str) -> Optional[Dict]:
        """Get detailed employee information using the Employee Clean API."""
        try:
            url = f"{self.employee_collect_url}?ids={employee_id}"
            logger.info(f"[API] Fetching employee details for ID: {employee_id} at {url}")
            data = self.make_request(url)
            if isinstance(data, list) and data:
                data = data[0]
            if data:
                logger.info(f"[API] Received data for employee {employee_id}: {str(data)[:500]}..." if len(str(data)) > 500 else f"[API] Received data for employee {employee_id}: {data}")
                time.sleep(self.rate_limit_delay)
                return data
            else:
                logger.warning(f"[API] No data returned for employee {employee_id}")
            return None
        except Exception as e:
            logger.error(f"Error getting employee details for ID {employee_id}: {e}")
            return None

    def safe_str(self, value: Any) -> str:
        """Safely convert value to string, handling None and special types."""
        if value is None:
            return ""
        if hasattr(value, 'size') and hasattr(value, '__getitem__'):
            try:
                if value.size == 0:
                    return ""
                if value.size == 1:
                    value = value.item()
                else:
                    return ', '.join([self.safe_str(v) for v in value])
            except Exception:
                pass
        if isinstance(value, list):
            if len(value) == 0:
                return ""
            return ', '.join([self.safe_str(v) for v in value])
        try:
            if pd.isna(value):
                return ""
        except Exception:
            pass
        return str(value).strip()

    def safe_join_list(self, data_list: List, max_items: int = 10) -> str:
        """Safely join a list of items, handling None and empty values."""
        if not data_list:
            return ""
        valid_items = [self.safe_str(item) for item in data_list if item]
        valid_items = valid_items[:max_items]
        return ' | '.join(valid_items)

    def safe_get_nested(self, data: Dict, *keys, default=""):
        """Safely get nested dictionary values."""
        current = data
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return default
        return self.safe_str(current) if current is not None else default

    def extract_employee_fields(self, employee_data: Dict) -> Dict:
        """Extract all possible employee fields from CoreSignal Employee API data, including all top-level fields and all collections, regardless of name."""
        enriched_data = {}
        
        # Metadata
        enriched_data.update({
            'cs_employee_created_at': self.safe_str(employee_data.get('created_at')),
            'cs_employee_updated_at': self.safe_str(employee_data.get('updated_at')),
            'cs_employee_checked_at': self.safe_str(employee_data.get('checked_at')),
            'cs_employee_changed_at': self.safe_str(employee_data.get('changed_at')),
            'cs_employee_experience_change_last_identified_at': self.safe_str(employee_data.get('experience_change_last_identified_at')),
            'cs_employee_is_deleted': self.safe_str(employee_data.get('is_deleted')),
            'cs_employee_is_parent': self.safe_str(employee_data.get('is_parent')),
        })
        
        # Identifiers and URLs
        enriched_data.update({
            'cs_employee_id': self.safe_str(employee_data.get('id')),
            'cs_employee_parent_id': self.safe_str(employee_data.get('parent_id')),
            'cs_employee_public_profile_id': self.safe_str(employee_data.get('public_profile_id')),
            'cs_employee_professional_network_url': self.safe_str(employee_data.get('professional_network_url')),
            'cs_employee_historical_ids': self.safe_join_list(employee_data.get('historical_ids', []), 10),
            'cs_employee_professional_network_shorthand_names': self.safe_join_list(employee_data.get('professional_network_shorthand_names', []), 10),
        })
        
        # Employee information
        enriched_data.update({
            'cs_employee_full_name': self.safe_str(employee_data.get('full_name')),
            'cs_employee_first_name': self.safe_str(employee_data.get('first_name')),
            'cs_employee_first_name_initial': self.safe_str(employee_data.get('first_name_initial')),
            'cs_employee_middle_name': self.safe_str(employee_data.get('middle_name')),
            'cs_employee_middle_name_initial': self.safe_str(employee_data.get('middle_name_initial')),
            'cs_employee_last_name': self.safe_str(employee_data.get('last_name')),
            'cs_employee_last_name_initial': self.safe_str(employee_data.get('last_name_initial')),
            'cs_employee_picture_url': self.safe_str(employee_data.get('picture_url')),
            'cs_employee_connections_count': self.safe_str(employee_data.get('connections_count')),
            'cs_employee_followers_count': self.safe_str(employee_data.get('followers_count')),
            'cs_employee_interests': self.safe_join_list(employee_data.get('interests', []), 20),
        })
        
        # Professional contact information
        enriched_data.update({
            'cs_employee_primary_professional_email': self.safe_str(employee_data.get('primary_professional_email')),
            'cs_employee_primary_professional_email_status': self.safe_str(employee_data.get('primary_professional_email_status')),
        })
        
        # Professional emails collection
        professional_emails = employee_data.get('professional_emails_collection', [])
        for i, email_data in enumerate(professional_emails[:5]):  # Limit to 5 emails
            enriched_data.update({
                f'cs_employee_professional_email_{i+1}': self.safe_str(email_data.get('professional_email')),
                f'cs_employee_professional_email_{i+1}_status': self.safe_str(email_data.get('professional_email_status')),
                f'cs_employee_professional_email_{i+1}_priority': self.safe_str(email_data.get('order_of_priority')),
            })
        
        # Location
        enriched_data.update({
            'cs_employee_location_country': self.safe_str(employee_data.get('location_country')),
            'cs_employee_location_country_iso2': self.safe_str(employee_data.get('location_country_iso2')),
            'cs_employee_location_country_iso3': self.safe_str(employee_data.get('location_country_iso3')),
            'cs_employee_location_full': self.safe_str(employee_data.get('location_full')),
            'cs_employee_location_regions': self.safe_join_list(employee_data.get('location_regions', []), 10),
        })
        
        # Active experience overview
        enriched_data.update({
            'cs_employee_headline': self.safe_str(employee_data.get('headline')),
            'cs_employee_generated_headline': self.safe_str(employee_data.get('generated_headline')),
            'cs_employee_summary': self.safe_str(employee_data.get('summary')),
            'cs_employee_services': self.safe_str(employee_data.get('services')),
            'cs_employee_is_working': self.safe_str(employee_data.get('is_working')),
            'cs_employee_active_experience_company_id': self.safe_str(employee_data.get('active_experience_company_id')),
            'cs_employee_active_experience_title': self.safe_str(employee_data.get('active_experience_title')),
            'cs_employee_active_experience_description': self.safe_str(employee_data.get('active_experience_description')),
            'cs_employee_active_experience_department': self.safe_str(employee_data.get('active_experience_department')),
            'cs_employee_active_experience_management_level': self.safe_str(employee_data.get('active_experience_management_level')),
            'cs_employee_is_decision_maker': self.safe_str(employee_data.get('is_decision_maker')),
        })
        
        # Skills
        enriched_data.update({
            'cs_employee_inferred_skills': self.safe_join_list(employee_data.get('inferred_skills', []), 30),
            'cs_employee_historical_skills': self.safe_join_list(employee_data.get('historical_skills', []), 20),
        })
        
        # Experience duration
        enriched_data.update({
            'cs_employee_total_experience_duration_months': self.safe_str(employee_data.get('total_experience_duration_months')),
        })
        
        # Experience duration breakdown by department
        dept_breakdown = employee_data.get('total_experience_duration_months_breakdown_department', [])
        for i, dept in enumerate(dept_breakdown[:5]):  # Limit to 5 departments
            enriched_data.update({
                f'cs_employee_dept_{i+1}_name': self.safe_str(dept.get('department')),
                f'cs_employee_dept_{i+1}_duration_months': self.safe_str(dept.get('total_experience_duration_months')),
            })
        
        # Experience duration breakdown by management level
        mgmt_breakdown = employee_data.get('total_experience_duration_months_breakdown_management_level', [])
        for i, mgmt in enumerate(mgmt_breakdown[:5]):  # Limit to 5 management levels
            enriched_data.update({
                f'cs_employee_mgmt_{i+1}_level': self.safe_str(mgmt.get('management_level')),
                f'cs_employee_mgmt_{i+1}_duration_months': self.safe_str(mgmt.get('total_experience_duration_months')),
            })
        
        # Full experience information
        experience = employee_data.get('experience', [])
        for i, exp in enumerate(experience[:10]):  # Limit to 10 experiences
            enriched_data.update({
                f'cs_employee_exp_{i+1}_active': self.safe_str(exp.get('active_experience')),
                f'cs_employee_exp_{i+1}_title': self.safe_str(exp.get('position_title')),
                f'cs_employee_exp_{i+1}_department': self.safe_str(exp.get('department')),
                f'cs_employee_exp_{i+1}_management_level': self.safe_str(exp.get('management_level')),
                f'cs_employee_exp_{i+1}_location': self.safe_str(exp.get('location')),
                f'cs_employee_exp_{i+1}_date_from': self.safe_str(exp.get('date_from')),
                f'cs_employee_exp_{i+1}_date_from_year': self.safe_str(exp.get('date_from_year')),
                f'cs_employee_exp_{i+1}_date_from_month': self.safe_str(exp.get('date_from_month')),
                f'cs_employee_exp_{i+1}_date_to': self.safe_str(exp.get('date_to')),
                f'cs_employee_exp_{i+1}_date_to_year': self.safe_str(exp.get('date_to_year')),
                f'cs_employee_exp_{i+1}_date_to_month': self.safe_str(exp.get('date_to_month')),
                f'cs_employee_exp_{i+1}_duration_months': self.safe_str(exp.get('duration_months')),
                f'cs_employee_exp_{i+1}_description': self.safe_str(exp.get('description')),
            })
            
            # Workplace details for each experience
            workplace = exp.get('workplace', {})
            if workplace:
                enriched_data.update({
                    f'cs_employee_exp_{i+1}_company_id': self.safe_str(workplace.get('company_id')),
                    f'cs_employee_exp_{i+1}_company_name': self.safe_str(workplace.get('company_name')),
                    f'cs_employee_exp_{i+1}_company_type': self.safe_str(workplace.get('company_type')),
                    f'cs_employee_exp_{i+1}_company_founded_year': self.safe_str(workplace.get('company_founded_year')),
                    f'cs_employee_exp_{i+1}_company_size_range': self.safe_str(workplace.get('company_size_range')),
                    f'cs_employee_exp_{i+1}_company_employees_count': self.safe_str(workplace.get('company_employees_count')),
                    f'cs_employee_exp_{i+1}_company_categories_keywords': self.safe_join_list(workplace.get('company_categories_and_keywords', []), 15),
                    f'cs_employee_exp_{i+1}_company_employees_change_yearly_percentage': self.safe_str(workplace.get('company_employees_count_change_yearly_percentage')),
                    f'cs_employee_exp_{i+1}_company_industry': self.safe_str(workplace.get('company_industry')),
                    f'cs_employee_exp_{i+1}_company_last_updated_at': self.safe_str(workplace.get('company_last_updated_at')),
                    f'cs_employee_exp_{i+1}_company_is_b2b': self.safe_str(workplace.get('company_is_b2b')),
                    f'cs_employee_exp_{i+1}_order_in_profile': self.safe_str(workplace.get('order_in_profile')),
                })
                
                # Social media for workplace
                enriched_data.update({
                    f'cs_employee_exp_{i+1}_company_followers_count': self.safe_str(workplace.get('company_followers_count')),
                    f'cs_employee_exp_{i+1}_company_website': self.safe_str(workplace.get('company_website')),
                    f'cs_employee_exp_{i+1}_company_facebook_url': self.safe_join_list(workplace.get('company_facebook_url', []), 5),
                    f'cs_employee_exp_{i+1}_company_twitter_url': self.safe_join_list(workplace.get('company_twitter_url', []), 5),
                    f'cs_employee_exp_{i+1}_company_linkedin_url': self.safe_str(workplace.get('company_linkedin_url')),
                })
                
                # Financials for workplace
                enriched_data.update({
                    f'cs_employee_exp_{i+1}_company_annual_revenue_source_1': self.safe_str(workplace.get('annual_revenue_source_1')),
                    f'cs_employee_exp_{i+1}_company_annual_revenue_currency_source_1': self.safe_str(workplace.get('annual_revenue_currency_source_1')),
                    f'cs_employee_exp_{i+1}_company_annual_revenue_source_5': self.safe_str(workplace.get('annual_revenue_source_5')),
                    f'cs_employee_exp_{i+1}_company_annual_revenue_currency_source_5': self.safe_str(workplace.get('annual_revenue_currency_source_5')),
                    f'cs_employee_exp_{i+1}_company_last_funding_round_date': self.safe_str(workplace.get('company_last_funding_round_date')),
                    f'cs_employee_exp_{i+1}_company_last_funding_round_amount_raised': self.safe_str(workplace.get('company_last_funding_round_amount_raised')),
                })
                
                # Stock ticker for workplace
                stock_tickers = workplace.get('stock_ticker', [])
                for j, ticker in enumerate(stock_tickers[:3]):  # Limit to 3 tickers
                    enriched_data.update({
                        f'cs_employee_exp_{i+1}_company_stock_exchange_{j+1}': self.safe_str(ticker.get('exchange')),
                        f'cs_employee_exp_{i+1}_company_stock_ticker_{j+1}': self.safe_str(ticker.get('ticker')),
                    })
                
                # Workplace locations
                enriched_data.update({
                    f'cs_employee_exp_{i+1}_company_hq_full_address': self.safe_str(workplace.get('company_hq_full_address')),
                    f'cs_employee_exp_{i+1}_company_hq_country': self.safe_str(workplace.get('company_hq_country')),
                    f'cs_employee_exp_{i+1}_company_hq_regions': self.safe_join_list(workplace.get('company_hq_regions', []), 10),
                    f'cs_employee_exp_{i+1}_company_hq_country_iso2': self.safe_str(workplace.get('company_hq_country_iso2')),
                    f'cs_employee_exp_{i+1}_company_hq_country_iso3': self.safe_str(workplace.get('company_hq_country_iso3')),
                    f'cs_employee_exp_{i+1}_company_hq_city': self.safe_str(workplace.get('company_hq_city')),
                    f'cs_employee_exp_{i+1}_company_hq_state': self.safe_str(workplace.get('company_hq_state')),
                    f'cs_employee_exp_{i+1}_company_hq_street': self.safe_str(workplace.get('company_hq_street')),
                    f'cs_employee_exp_{i+1}_company_hq_zipcode': self.safe_str(workplace.get('company_hq_zipcode')),
                })
        
        # Education
        enriched_data.update({
            'cs_employee_last_graduation_date': self.safe_str(employee_data.get('last_graduation_date')),
            'cs_employee_education_degrees': self.safe_join_list(employee_data.get('education_degrees', []), 10),
        })
        
        education = employee_data.get('education', [])
        for i, edu in enumerate(education[:5]):  # Limit to 5 education entries
            enriched_data.update({
                f'cs_employee_edu_{i+1}_degree': self.safe_str(edu.get('degree')),
                f'cs_employee_edu_{i+1}_description': self.safe_str(edu.get('description')),
                f'cs_employee_edu_{i+1}_institution_url': self.safe_str(edu.get('institution_url')),
                f'cs_employee_edu_{i+1}_institution_name': self.safe_str(edu.get('institution_name')),
                f'cs_employee_edu_{i+1}_institution_full_address': self.safe_str(edu.get('institution_full_address')),
                f'cs_employee_edu_{i+1}_institution_country_iso2': self.safe_str(edu.get('institution_country_iso2')),
                f'cs_employee_edu_{i+1}_institution_country_iso3': self.safe_str(edu.get('institution_country_iso3')),
                f'cs_employee_edu_{i+1}_institution_regions': self.safe_join_list(edu.get('institution_regions', []), 10),
                f'cs_employee_edu_{i+1}_institution_city': self.safe_str(edu.get('institution_city')),
                f'cs_employee_edu_{i+1}_institution_state': self.safe_str(edu.get('institution_state')),
                f'cs_employee_edu_{i+1}_institution_street': self.safe_str(edu.get('institution_street')),
                f'cs_employee_edu_{i+1}_institution_zipcode': self.safe_str(edu.get('institution_zipcode')),
                f'cs_employee_edu_{i+1}_date_from_year': self.safe_str(edu.get('date_from_year')),
                f'cs_employee_edu_{i+1}_date_to_year': self.safe_str(edu.get('date_to_year')),
                f'cs_employee_edu_{i+1}_activities_and_societies': self.safe_str(edu.get('activities_and_societies')),
                f'cs_employee_edu_{i+1}_order_in_profile': self.safe_str(edu.get('order_in_profile')),
            })
        
        # Salary information
        enriched_data.update({
            'cs_employee_projected_base_salary_p25': self.safe_str(employee_data.get('projected_base_salary_p25')),
            'cs_employee_projected_base_salary_median': self.safe_str(employee_data.get('projected_base_salary_median')),
            'cs_employee_projected_base_salary_p75': self.safe_str(employee_data.get('projected_base_salary_p75')),
            'cs_employee_projected_base_salary_period': self.safe_str(employee_data.get('projected_base_salary_period')),
            'cs_employee_projected_base_salary_currency': self.safe_str(employee_data.get('projected_base_salary_currency')),
            'cs_employee_projected_base_salary_updated_at': self.safe_str(employee_data.get('projected_base_salary_updated_at')),
        })
        
        # Additional salary
        additional_salary = employee_data.get('projected_additional_salary', [])
        for i, add_sal in enumerate(additional_salary[:5]):  # Limit to 5 additional salary types
            enriched_data.update({
                f'cs_employee_additional_salary_{i+1}_type': self.safe_str(add_sal.get('projected_additional_salary_type')),
                f'cs_employee_additional_salary_{i+1}_p25': self.safe_str(add_sal.get('projected_additional_salary_p25')),
                f'cs_employee_additional_salary_{i+1}_median': self.safe_str(add_sal.get('projected_additional_salary_median')),
                f'cs_employee_additional_salary_{i+1}_p75': self.safe_str(add_sal.get('projected_additional_salary_p75')),
            })
        
        enriched_data.update({
            'cs_employee_projected_additional_salary_period': self.safe_str(employee_data.get('projected_additional_salary_period')),
            'cs_employee_projected_additional_salary_currency': self.safe_str(employee_data.get('projected_additional_salary_currency')),
            'cs_employee_projected_additional_salary_updated_at': self.safe_str(employee_data.get('projected_additional_salary_updated_at')),
        })
        
        # Total salary
        enriched_data.update({
            'cs_employee_projected_total_salary_p25': self.safe_str(employee_data.get('projected_total_salary_p25')),
            'cs_employee_projected_total_salary_median': self.safe_str(employee_data.get('projected_total_salary_median')),
            'cs_employee_projected_total_salary_p75': self.safe_str(employee_data.get('projected_total_salary_p75')),
            'cs_employee_projected_total_salary_period': self.safe_str(employee_data.get('projected_total_salary_period')),
            'cs_employee_projected_total_salary_currency': self.safe_str(employee_data.get('projected_total_salary_currency')),
            'cs_employee_projected_total_salary_updated_at': self.safe_str(employee_data.get('projected_total_salary_updated_at')),
        })
        
        # Profile field changes
        profile_changes = employee_data.get('profile_root_field_changes_summary', [])
        for i, change in enumerate(profile_changes[:10]):  # Limit to 10 changes
            enriched_data.update({
                f'cs_employee_profile_change_{i+1}_field': self.safe_str(change.get('field_name')),
                f'cs_employee_profile_change_{i+1}_type': self.safe_str(change.get('change_type')),
                f'cs_employee_profile_change_{i+1}_date': self.safe_str(change.get('last_changed_at')),
            })
        
        collection_changes = employee_data.get('profile_collection_field_changes_summary', [])
        for i, change in enumerate(collection_changes[:10]):  # Limit to 10 collection changes
            enriched_data.update({
                f'cs_employee_collection_change_{i+1}_field': self.safe_str(change.get('field_name')),
                f'cs_employee_collection_change_{i+1}_date': self.safe_str(change.get('last_changed_at')),
            })
        
        # Recent experience changes
        recent_started = employee_data.get('experience_recently_started', [])
        for i, exp in enumerate(recent_started[:5]):  # Limit to 5 recent starts
            enriched_data.update({
                f'cs_employee_recent_start_{i+1}_company_id': self.safe_str(exp.get('company_id')),
                f'cs_employee_recent_start_{i+1}_company_name': self.safe_str(exp.get('company_name')),
                f'cs_employee_recent_start_{i+1}_company_url': self.safe_str(exp.get('company_url')),
                f'cs_employee_recent_start_{i+1}_company_shorthand_name': self.safe_str(exp.get('company_shorthand_name')),
                f'cs_employee_recent_start_{i+1}_date_from': self.safe_str(exp.get('date_from')),
                f'cs_employee_recent_start_{i+1}_date_to': self.safe_str(exp.get('date_to')),
                f'cs_employee_recent_start_{i+1}_title': self.safe_str(exp.get('title')),
                f'cs_employee_recent_start_{i+1}_identification_date': self.safe_str(exp.get('identification_date')),
            })
        
        recent_closed = employee_data.get('experience_recently_closed', [])
        for i, exp in enumerate(recent_closed[:5]):  # Limit to 5 recent closes
            enriched_data.update({
                f'cs_employee_recent_close_{i+1}_company_id': self.safe_str(exp.get('company_id')),
                f'cs_employee_recent_close_{i+1}_company_name': self.safe_str(exp.get('company_name')),
                f'cs_employee_recent_close_{i+1}_company_url': self.safe_str(exp.get('company_url')),
                f'cs_employee_recent_close_{i+1}_company_shorthand_name': self.safe_str(exp.get('company_shorthand_name')),
                f'cs_employee_recent_close_{i+1}_date_from': self.safe_str(exp.get('date_from')),
                f'cs_employee_recent_close_{i+1}_date_to': self.safe_str(exp.get('date_to')),
                f'cs_employee_recent_close_{i+1}_title': self.safe_str(exp.get('title')),
                f'cs_employee_recent_close_{i+1}_identification_date': self.safe_str(exp.get('identification_date')),
            })
        
        # Recommendations
        enriched_data.update({
            'cs_employee_recommendations_count': self.safe_str(employee_data.get('recommendations_count')),
        })
        
        recommendations = employee_data.get('recommendations', [])
        for i, rec in enumerate(recommendations[:5]):  # Limit to 5 recommendations
            enriched_data.update({
                f'cs_employee_recommendation_{i+1}_text': self.safe_str(rec.get('recommendation')),
                f'cs_employee_recommendation_{i+1}_referee_name': self.safe_str(rec.get('referee_full_name')),
                f'cs_employee_recommendation_{i+1}_referee_url': self.safe_str(rec.get('referee_url')),
                f'cs_employee_recommendation_{i+1}_order': self.safe_str(rec.get('order_in_profile')),
            })
        
        # Activity
        activity = employee_data.get('activity', [])
        for i, act in enumerate(activity[:10]):  # Limit to 10 activities
            enriched_data.update({
                f'cs_employee_activity_{i+1}_url': self.safe_str(act.get('activity_url')),
                f'cs_employee_activity_{i+1}_title': self.safe_str(act.get('title')),
                f'cs_employee_activity_{i+1}_action': self.safe_str(act.get('action')),
                f'cs_employee_activity_{i+1}_order': self.safe_str(act.get('order_in_profile')),
            })
        
        # Awards
        awards = employee_data.get('awards', [])
        for i, award in enumerate(awards[:10]):  # Limit to 10 awards
            enriched_data.update({
                f'cs_employee_award_{i+1}_title': self.safe_str(award.get('title')),
                f'cs_employee_award_{i+1}_issuer': self.safe_str(award.get('issuer')),
                f'cs_employee_award_{i+1}_description': self.safe_str(award.get('description')),
                f'cs_employee_award_{i+1}_date': self.safe_str(award.get('date')),
                f'cs_employee_award_{i+1}_date_year': self.safe_str(award.get('date_year')),
                f'cs_employee_award_{i+1}_date_month': self.safe_str(award.get('date_month')),
                f'cs_employee_award_{i+1}_order': self.safe_str(award.get('order_in_profile')),
            })
        
        # Courses
        courses = employee_data.get('courses', [])
        for i, course in enumerate(courses[:10]):  # Limit to 10 courses
            enriched_data.update({
                f'cs_employee_course_{i+1}_organizer': self.safe_str(course.get('organizer')),
                f'cs_employee_course_{i+1}_title': self.safe_str(course.get('title')),
                f'cs_employee_course_{i+1}_order': self.safe_str(course.get('order_in_profile')),
            })
        
        # Certifications
        certifications = employee_data.get('certifications', [])
        for i, cert in enumerate(certifications[:10]):  # Limit to 10 certifications
            enriched_data.update({
                f'cs_employee_cert_{i+1}_title': self.safe_str(cert.get('title')),
                f'cs_employee_cert_{i+1}_issuer': self.safe_str(cert.get('issuer')),
                f'cs_employee_cert_{i+1}_issuer_url': self.safe_str(cert.get('issuer_url')),
                f'cs_employee_cert_{i+1}_credential_id': self.safe_str(cert.get('credential_id')),
                f'cs_employee_cert_{i+1}_certificate_url': self.safe_str(cert.get('certificate_url')),
                f'cs_employee_cert_{i+1}_date_from': self.safe_str(cert.get('date_from')),
                f'cs_employee_cert_{i+1}_date_from_year': self.safe_str(cert.get('date_from_year')),
                f'cs_employee_cert_{i+1}_date_from_month': self.safe_str(cert.get('date_from_month')),
                f'cs_employee_cert_{i+1}_date_to': self.safe_str(cert.get('date_to')),
                f'cs_employee_cert_{i+1}_date_to_year': self.safe_str(cert.get('date_to_year')),
                f'cs_employee_cert_{i+1}_date_to_month': self.safe_str(cert.get('date_to_month')),
                f'cs_employee_cert_{i+1}_order': self.safe_str(cert.get('order_in_profile')),
            })
        
        # Languages
        languages = employee_data.get('languages', [])
        for i, lang in enumerate(languages[:10]):  # Limit to 10 languages
            enriched_data.update({
                f'cs_employee_language_{i+1}_name': self.safe_str(lang.get('language')),
                f'cs_employee_language_{i+1}_proficiency': self.safe_str(lang.get('proficiency')),
                f'cs_employee_language_{i+1}_order': self.safe_str(lang.get('order_in_profile')),
            })
        
        # Patents
        enriched_data.update({
            'cs_employee_patents_count': self.safe_str(employee_data.get('patents_count')),
            'cs_employee_patents_topics': self.safe_join_list(employee_data.get('patents_topics', []), 20),
        })
        
        patents = employee_data.get('patents', [])
        for i, patent in enumerate(patents[:10]):  # Limit to 10 patents
            enriched_data.update({
                f'cs_employee_patent_{i+1}_title': self.safe_str(patent.get('title')),
                f'cs_employee_patent_{i+1}_status': self.safe_str(patent.get('status')),
                f'cs_employee_patent_{i+1}_description': self.safe_str(patent.get('description')),
                f'cs_employee_patent_{i+1}_url': self.safe_str(patent.get('patent_url')),
                f'cs_employee_patent_{i+1}_date': self.safe_str(patent.get('date')),
                f'cs_employee_patent_{i+1}_date_year': self.safe_str(patent.get('date_year')),
                f'cs_employee_patent_{i+1}_date_month': self.safe_str(patent.get('date_month')),
                f'cs_employee_patent_{i+1}_number': self.safe_str(patent.get('patent_number')),
                f'cs_employee_patent_{i+1}_order': self.safe_str(patent.get('order_in_profile')),
            })
        
        # Publications
        enriched_data.update({
            'cs_employee_publications_count': self.safe_str(employee_data.get('publications_count')),
            'cs_employee_publications_topics': self.safe_join_list(employee_data.get('publications_topics', []), 20),
        })
        
        publications = employee_data.get('publications', [])
        for i, pub in enumerate(publications[:10]):  # Limit to 10 publications
            enriched_data.update({
                f'cs_employee_publication_{i+1}_title': self.safe_str(pub.get('title')),
                f'cs_employee_publication_{i+1}_description': self.safe_str(pub.get('description')),
                f'cs_employee_publication_{i+1}_url': self.safe_str(pub.get('publication_url')),
                f'cs_employee_publication_{i+1}_publisher_names': self.safe_join_list(pub.get('publisher_names', []), 10),
                f'cs_employee_publication_{i+1}_date': self.safe_str(pub.get('date')),
                f'cs_employee_publication_{i+1}_date_year': self.safe_str(pub.get('date_year')),
                f'cs_employee_publication_{i+1}_date_month': self.safe_str(pub.get('date_month')),
                f'cs_employee_publication_{i+1}_order': self.safe_str(pub.get('order_in_profile')),
            })
        
        # Projects
        enriched_data.update({
            'cs_employee_projects_count': self.safe_str(employee_data.get('projects_count')),
            'cs_employee_projects_topics': self.safe_join_list(employee_data.get('projects_topics', []), 20),
        })
        
        projects = employee_data.get('projects', [])
        for i, proj in enumerate(projects[:10]):  # Limit to 10 projects
            enriched_data.update({
                f'cs_employee_project_{i+1}_name': self.safe_str(proj.get('name')),
                f'cs_employee_project_{i+1}_description': self.safe_str(proj.get('description')),
                f'cs_employee_project_{i+1}_url': self.safe_str(proj.get('project_url')),
                f'cs_employee_project_{i+1}_date_from': self.safe_str(proj.get('date_from')),
                f'cs_employee_project_{i+1}_date_from_year': self.safe_str(proj.get('date_from_year')),
                f'cs_employee_project_{i+1}_date_from_month': self.safe_str(proj.get('date_from_month')),
                f'cs_employee_project_{i+1}_date_to': self.safe_str(proj.get('date_to')),
                f'cs_employee_project_{i+1}_date_to_year': self.safe_str(proj.get('date_to_year')),
                f'cs_employee_project_{i+1}_date_to_month': self.safe_str(proj.get('date_to_month')),
                f'cs_employee_project_{i+1}_order': self.safe_str(proj.get('order_in_profile')),
            })
        
        # Organizations
        organizations = employee_data.get('organizations', [])
        for i, org in enumerate(organizations[:10]):  # Limit to 10 organizations
            enriched_data.update({
                f'cs_employee_org_{i+1}_name': self.safe_str(org.get('organization_name')),
                f'cs_employee_org_{i+1}_position': self.safe_str(org.get('position')),
                f'cs_employee_org_{i+1}_description': self.safe_str(org.get('description')),
                f'cs_employee_org_{i+1}_date_from': self.safe_str(org.get('date_from')),
                f'cs_employee_org_{i+1}_date_from_year': self.safe_str(org.get('date_from_year')),
                f'cs_employee_org_{i+1}_date_from_month': self.safe_str(org.get('date_from_month')),
                f'cs_employee_org_{i+1}_date_to': self.safe_str(org.get('date_to')),
                f'cs_employee_org_{i+1}_date_to_year': self.safe_str(org.get('date_to_year')),
                f'cs_employee_org_{i+1}_date_to_month': self.safe_str(org.get('date_to_month')),
                f'cs_employee_org_{i+1}_order': self.safe_str(org.get('order_in_profile')),
            })
        
        return enriched_data

    def enrich_employee_data(self, row: pd.Series, row_index: int = None) -> Dict:
        """Enrich employee data with fallback and warning flags."""
        enriched_data = {}
        
        # Extract employee information
        full_name = row.get('contact_full_name', '')
        company_name = row.get('contact_firm_name', '')
        employee_id = row.get('cs_employee_id', '')
        
        if row_index:
            logger.info(f"[CSV] Processing row {row_index}/216: {row.get('recipient_email', 'N/A')}")
        
        if not full_name:
            logger.warning(f"[ENRICH] No full name found for row {row_index}. Skipping enrichment.")
            return enriched_data
        
        # Check if we already have a valid employee ID
        if employee_id and str(employee_id).strip() and str(employee_id) != 'nan':
            logger.info(f"[ENRICH] Using existing employee ID: {employee_id}")
            employee_data = self.get_employee_details(str(employee_id))
            if employee_data:
                enriched_data = self.extract_employee_fields(employee_data)
                enriched_data['cs_employee_match_confidence'] = 'existing_id'
                enriched_data['cs_employee_match_warning'] = ''
                logger.info(f"[ENRICH] Successfully enriched employee: {full_name} (ID: {employee_id})")
            else:
                logger.warning(f"[ENRICH] Failed to get employee details for ID: {employee_id}")
                enriched_data['cs_employee_match_confidence'] = 'failed'
                enriched_data['cs_employee_match_warning'] = f'Failed to get details for ID: {employee_id}'
        else:
            # Search for employee
            logger.info(f"[ENRICH] No valid employee ID for row {row_index}. Attempting search for: {full_name} at {company_name}")
            
            # Use fallback strategy
            employee_id = self.search_employee(full_name=full_name, company_name=company_name, use_fallback=True)
            
            if employee_id:
                logger.info(f"[ENRICH] Attempting to enrich employee: {full_name} (ID: {employee_id})")
                employee_data = self.get_employee_details(employee_id)
                
                if employee_data:
                    enriched_data = self.extract_employee_fields(employee_data)
                    
                    # Add match confidence and warning flags
                    if employee_id in row.get('cs_employee_id', ''):
                        enriched_data['cs_employee_match_confidence'] = 'strong_match'
                        enriched_data['cs_employee_match_warning'] = ''
                    else:
                        enriched_data['cs_employee_match_confidence'] = 'fallback_match'
                        enriched_data['cs_employee_match_warning'] = 'This match may not be exact - review recommended'
                    
                    logger.info(f"[ENRICH] Successfully enriched employee: {full_name} (ID: {employee_id})")
                else:
                    logger.warning(f"[ENRICH] Failed to get employee details for found ID: {employee_id}")
                    enriched_data['cs_employee_match_confidence'] = 'failed'
                    enriched_data['cs_employee_match_warning'] = f'Found ID {employee_id} but failed to get details'
            else:
                logger.warning(f"[ENRICH] No employee found for: {full_name} at {company_name}. Skipping enrichment.")
                enriched_data['cs_employee_match_confidence'] = 'no_match'
                enriched_data['cs_employee_match_warning'] = 'No suitable match found in CoreSignal database'
        
        return enriched_data

    def process_csv(self, input_file: str, output_file: str):
        """Process CSV file with comprehensive employee enrichment."""
        try:
            df = pd.read_csv(input_file)
            logger.info(f"[CSV] Loaded {len(df)} rows from {input_file}")
            enriched_rows = []
            employee_matches = 0
            for index, row in df.iterrows():
                logger.info(f"[CSV] Processing row {index + 1}/{len(df)}: {self.safe_str(row.get('contact_email', 'N/A'))}")
                enriched_row = row.to_dict()
                try:
                    employee_enrichment = self.enrich_employee_data(row, row_index=index+1)
                    enriched_row.update(employee_enrichment)
                    if employee_enrichment.get('cs_employee_id'):
                        employee_matches += 1
                        logger.info(f"[CSV] Employee data added for row {index + 1}")
                    else:
                        logger.info(f"[CSV] No employee data added for row {index + 1}")
                except Exception as e:
                    logger.error(f"[ERROR] Exception processing row {index + 1} ({self.safe_str(row.get('contact_email', 'N/A'))}): {e}")
                    logger.error(f"[ERROR] Full row data: {row.to_dict()}")
                enriched_rows.append(enriched_row)
                time.sleep(self.rate_limit_delay)
            enriched_df = pd.DataFrame(enriched_rows)
            enriched_df.to_csv(output_file, index=False)
            logger.info(f"[CSV] Employee enriched data saved to {output_file}")
            logger.info(f"[CSV] Summary:")
            logger.info(f"  Total rows processed: {len(enriched_rows)}")
            logger.info(f"  Employee matches found: {employee_matches}")
            logger.info(f"  Employee match rate: {employee_matches/len(enriched_rows)*100:.1f}%")
        except Exception as e:
            logger.error(f"Error processing CSV: {e}")
            raise

def main():
    """Main function to run the employee enrichment process."""
    try:
        logger.info("Starting CoreSignal Employee enrichment process...")
        
        # Initialize the enricher
        enricher = CoreSignalEmployeeEnricher(CORESIGNAL_EMPLOYEE_API_KEY)
        
        # Process the enriched CSV to add employee data
        input_file = "enriched_data.csv"
        output_file = "enhanced_enrichment.csv"
        
        logger.info(f"Processing {input_file} to add employee data...")
        enricher.process_csv(input_file, output_file)
        
        logger.info("Employee enrichment completed successfully!")
        
    except Exception as e:
        logger.error(f"Error in employee enrichment process: {e}")
        raise

if __name__ == "__main__":
    main() 