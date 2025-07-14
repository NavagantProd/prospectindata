"""
Data enrichment engine for CoreSignal pipeline - FIXED VERSION
"""
import json
import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
import pandas as pd
from fuzzywuzzy import fuzz
import tldextract
from coresignal_client import CoreSignalClient
from config import OUTPUT_SCHEMA

logger = logging.getLogger(__name__)

@dataclass
class ContactRecord:
    """Represents a contact record with all enrichment fields"""
    # Core contact info
    name: str = ""
    email: str = ""
    company_name: str = ""
    company_website: str = ""
    
    # Company enrichment fields
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
    
    # Employee enrichment fields
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

    def is_valid(self) -> bool:
        return bool(self.name and self.email)

class EnrichmentEngine:
    """Main enrichment engine"""
    
    def __init__(self, client: CoreSignalClient):
        self.client = client
        
    def extract_domain(self, url_or_email: str) -> str:
        """Extract clean domain from URL or email"""
        if not url_or_email:
            return ""
        
        if '@' in url_or_email:
            # Email domain
            domain = url_or_email.split('@')[1].lower().strip()
            if domain.startswith('mail.'):
                domain = domain[5:]
            return domain
        else:
            # Website URL
            if not url_or_email.startswith(('http://', 'https://')):
                url_or_email = f"https://{url_or_email}"
            try:
                extracted = tldextract.extract(url_or_email)
                return f"{extracted.domain}.{extracted.suffix}".lower()
            except:
                return ""

    def calculate_name_similarity(self, name1: str, name2: str) -> float:
        """Calculate similarity between two names"""
        if not name1 or not name2:
            return 0.0
        
        # Normalize names
        name1 = name1.lower().strip()
        name2 = name2.lower().strip()
        
        # Exact match
        if name1 == name2:
            return 1.0
        
        # Fuzzy match
        return fuzz.ratio(name1, name2) / 100.0

    def find_best_company_match(self, contact: ContactRecord) -> Tuple[Optional[Dict], float]:
        """Find the best company match for a contact using multiple search strategies and log all attempts"""
        best_match = None
        best_score = 0.0
        search_attempts = []
        # Strategy 1: Name + Website
        if contact.company_name and contact.company_website:
            search_attempts.append((contact.company_name, contact.company_website))
        # Strategy 2: Name only
        if contact.company_name:
            search_attempts.append((contact.company_name, ""))
        # Strategy 3: Website only
        if contact.company_website:
            search_attempts.append(("", contact.company_website))
        # Strategy 4: Fuzzy name (if name is long enough)
        if contact.company_name and len(contact.company_name) > 4:
            fuzzy_name = contact.company_name[:max(5, len(contact.company_name)//2)]
            search_attempts.append((fuzzy_name, ""))
        for name, website in search_attempts:
            logger.info(f"[COMPANY SEARCH] Trying name='{name}' website='{website}'")
            companies = self.client.search_companies(name, website)
            if companies:
                for company in companies:
                    if isinstance(company, int):
                        company = self.client.get_company_details(company)
                        if not isinstance(company, dict):
                            logger.warning(f"Company ID {company} could not be resolved to a dict.")
                            continue
                    if not isinstance(company, dict):
                        logger.warning(f"Company search result is not a dict: {company}")
                        continue
                    score = self.calculate_name_similarity(contact.company_name, company.get('name', ''))
                    # Boost score if website matches
                    if contact.company_website and company.get('website'):
                        contact_domain = self.extract_domain(contact.company_website)
                        company_domain = self.extract_domain(company['website'])
                        if contact_domain and company_domain and contact_domain == company_domain:
                            score += 0.3
                    if score > best_score:
                        best_score = score
                        best_match = company
            if best_score >= 0.7:
                break  # Good enough match found
        logger.info(f"[COMPANY SEARCH] Best match score: {best_score}")
        return best_match, best_score

    def find_best_employee_match(self, contact: ContactRecord, company_data: Dict = None) -> Tuple[Optional[Dict], float]:
        """Find the best employee match using multiple search strategies and log all attempts"""
        if not company_data:
            return None, 0.0
        company_name = company_data.get('name', '')
        search_strategies = []
        # Strategy 1: name + company
        if contact.name and company_name:
            search_strategies.append({'name': contact.name, 'experience_company_name': company_name})
        # Strategy 2: name + company + title
        if contact.name and company_name and contact.employee_title:
            search_strategies.append({'name': contact.name, 'experience_company_name': company_name, 'title': contact.employee_title})
        # Strategy 3: name + company + location
        if contact.name and company_name and contact.employee_location:
            search_strategies.append({'name': contact.name, 'experience_company_name': company_name, 'location': contact.employee_location})
        # Strategy 4: name only
        if contact.name:
            search_strategies.append({'name': contact.name})
        for strategy in search_strategies:
            logger.info(f"[EMPLOYEE SEARCH] Trying: {strategy}")
            member_results = self.client.search_members(**strategy)
            logger.debug(f"[DEBUG] Member search results: {member_results}")
            member_ids = []
            for entry in member_results:
                if isinstance(entry, dict) and 'id' in entry:
                    member_ids.append(entry['id'])
                elif isinstance(entry, int):
                    member_ids.append(entry)
            if member_ids:
                member_id = member_ids[0]
                logger.info(f"[EMPLOYEE SEARCH] Using member_id: {member_id}")
                profile = self.client.get_member_details(member_id)
                logger.debug(f"[DEBUG] Member profile: {json.dumps(profile, indent=2, default=str)}")
                if profile:
                    return profile, 1.0
        return None, 0.0

    def _verify_company_experience(self, member: Dict, company_name: str, company_id: str) -> bool:
        """Verify that the member actually worked at the target company"""
        experience = member.get('experience', []) or []
        for exp in experience:
            if not isinstance(exp, dict):
                continue
            exp_company_name = exp.get('company_name', '')
            exp_company_id = str(exp.get('company_id', ''))
            # Check by company name similarity
            if exp_company_name:
                similarity = self.calculate_name_similarity(company_name, exp_company_name)
                if similarity > 0.8:
                    return True
            # Check by company ID if available
            if company_id and exp_company_id and company_id == exp_company_id:
                return True
        return False

    def filter_active_items(self, items: List[Dict]) -> List[Dict]:
        """Filter out deleted items and return only active ones"""
        if not items:
            return []
        
        active_items = []
        for item in items:
            if isinstance(item, dict):
                # Skip deleted items
                if item.get('deleted') == 1 or item.get('deleted') == '1':
                    continue
                # Skip items with redacted/malformed data
                if any(field and '***' in str(field) for field in item.values()):
                    continue
                active_items.append(item)
        
        return active_items

    def flatten_collection(self, collection: List[Dict], prefix: str, max_items: int = 10) -> Dict[str, Any]:
        """Flatten a collection of dictionaries into individual fields"""
        flattened = {}
        
        if not collection:
            return flattened
        
        # Filter active items first
        active_items = self.filter_active_items(collection)
        
        # Add JSON representation of the full collection
        if active_items:
            flattened[f"{prefix}_json"] = json.dumps(active_items, ensure_ascii=False)
            flattened[f"{prefix}_count"] = len(active_items)
        
        # Flatten individual items
        for i, item in enumerate(active_items[:max_items]):
            for key, value in item.items():
                if key == 'deleted':  # Skip deleted flag since we already filtered
                    continue
                field_name = f"{prefix}_{i+1}_{key}"
                if isinstance(value, (dict, list)):
                    flattened[field_name] = json.dumps(value, ensure_ascii=False) if value else ""
                else:
                    flattened[field_name] = str(value) if value is not None else ""
        
        return flattened

    def enrich_company_data(self, contact: ContactRecord, company_data: Dict) -> None:
        """Populate company fields from API data with maximum extraction and explicit high-value fields"""
        logger.debug(f"[DEBUG] Raw company_data: {json.dumps(company_data, indent=2, default=str)}")
        # --- CORE COMPANY FIELDS ---
        contact.company_id = str(company_data.get('id', ''))
        contact.company_display_name = company_data.get('name', '')
        # company_name.exact
        contact.__dict__['company_name_exact'] = company_data.get('company_name', {}).get('exact', '') if isinstance(company_data.get('company_name'), dict) else company_data.get('company_name', '')
        # website.domain_only
        website = company_data.get('website', {})
        if isinstance(website, dict):
            contact.__dict__['company_website_domain_only'] = website.get('domain_only', '')
        else:
            contact.__dict__['company_website_domain_only'] = ''
        # industry.exact
        industry = company_data.get('industry', {})
        if isinstance(industry, dict):
            contact.__dict__['company_industry_exact'] = industry.get('exact', '')
        else:
            contact.__dict__['company_industry_exact'] = company_data.get('industry', '')
        # size_range, employees_count, founded_year, is_public, ipo_date
        for field in ['size_range', 'employees_count', 'founded_year', 'is_public', 'ipo_date']:
            contact.__dict__[f'company_{field}'] = company_data.get(field, '')
        # stock_ticker.ticker
        stock_ticker = company_data.get('stock_ticker', [])
        if isinstance(stock_ticker, list) and stock_ticker and isinstance(stock_ticker[0], dict):
            contact.__dict__['company_stock_ticker_ticker'] = stock_ticker[0].get('ticker', '')
        else:
            contact.__dict__['company_stock_ticker_ticker'] = ''
        # revenue_annual.source_1_annual_revenue
        revenue_annual = company_data.get('revenue_annual', {})
        if isinstance(revenue_annual, dict):
            s1 = revenue_annual.get('source_1_annual_revenue', {})
            if isinstance(s1, dict):
                contact.__dict__['company_revenue_annual_source_1_annual_revenue'] = s1.get('annual_revenue', '')
            else:
                contact.__dict__['company_revenue_annual_source_1_annual_revenue'] = ''
        # revenue_annual_range.source_4_annual_revenue_range_from
        revenue_annual_range = company_data.get('revenue_annual_range', {})
        if isinstance(revenue_annual_range, dict):
            s4 = revenue_annual_range.get('source_4_annual_revenue_range', {})
            if isinstance(s4, dict):
                contact.__dict__['company_revenue_annual_range_source_4_annual_revenue_range_from'] = s4.get('annual_revenue_range_from', '')
            else:
                contact.__dict__['company_revenue_annual_range_source_4_annual_revenue_range_from'] = ''
        # hq_country_iso2, hq_region, hq_city, hq_state
        for field in ['hq_country_iso2', 'hq_region', 'hq_city', 'hq_state']:
            contact.__dict__[f'company_{field}'] = company_data.get(field, '')
        # company_locations_full.location_address
        locs = company_data.get('company_locations_full', [])
        if isinstance(locs, list) and locs and isinstance(locs[0], dict):
            contact.__dict__['company_locations_full_location_address'] = locs[0].get('location_address', '')
        else:
            contact.__dict__['company_locations_full_location_address'] = ''
        # --- FINANCIAL & WEB METRICS ---
        # income_statements.net_income, income_statements.ebitda_margin
        income_statements = company_data.get('income_statements', [])
        if isinstance(income_statements, list) and income_statements and isinstance(income_statements[0], dict):
            contact.__dict__['company_income_statements_net_income'] = income_statements[0].get('net_income', '')
            contact.__dict__['company_income_statements_ebitda_margin'] = income_statements[0].get('ebitda_margin', '')
        else:
            contact.__dict__['company_income_statements_net_income'] = ''
            contact.__dict__['company_income_statements_ebitda_margin'] = ''
        for field in ['total_website_visits_monthly', 'bounce_rate', 'pages_per_visit']:
            contact.__dict__[f'company_{field}'] = company_data.get(field, '')
        # visits_breakdown_by_country.country
        visits_country = company_data.get('visits_breakdown_by_country', [])
        if isinstance(visits_country, list) and visits_country and isinstance(visits_country[0], dict):
            contact.__dict__['company_visits_breakdown_by_country_country'] = visits_country[0].get('country', '')
        else:
            contact.__dict__['company_visits_breakdown_by_country_country'] = ''
        # --- GROWTH & MOMENTUM ---
        # employees_count_change.change_yearly_percentage
        emp_count_change = company_data.get('employees_count_change', {})
        if isinstance(emp_count_change, dict):
            contact.__dict__['company_employees_count_change_yearly_percentage'] = emp_count_change.get('change_yearly_percentage', '')
        # total_website_visits_change.change_monthly_percentage
        web_visits_change = company_data.get('total_website_visits_change', {})
        if isinstance(web_visits_change, dict):
            contact.__dict__['company_total_website_visits_change_monthly_percentage'] = web_visits_change.get('change_monthly_percentage', '')
        # active_job_postings_count_change.change_monthly_percentage
        job_postings_change = company_data.get('active_job_postings_count_change', {})
        if isinstance(job_postings_change, dict):
            contact.__dict__['company_active_job_postings_count_change_monthly_percentage'] = job_postings_change.get('change_monthly_percentage', '')
        # --- ENGAGEMENT SIGNALS ---
        for field in ['followers_count_professional_network', 'followers_count_twitter']:
            contact.__dict__[f'company_{field}'] = company_data.get(field, '')
        # company_updates.date, description, reactions_count, comments_count
        updates = company_data.get('company_updates', [])
        for i in range(5):
            if isinstance(updates, list) and len(updates) > i and isinstance(updates[i], dict):
                for subfield in ['date', 'description', 'reactions_count', 'comments_count']:
                    contact.__dict__[f'company_updates_{i+1}_{subfield}'] = updates[i].get(subfield, '')
            else:
                for subfield in ['date', 'description', 'reactions_count', 'comments_count']:
                    contact.__dict__[f'company_updates_{i+1}_{subfield}'] = ''
        # --- FUNDING & M&A ---
        for field in ['last_funding_round_name', 'last_funding_round_announced_date', 'last_funding_round_amount_raised']:
            contact.__dict__[f'company_{field}'] = company_data.get(field, '')
        # funding_rounds.name
        funding_rounds = company_data.get('funding_rounds', [])
        for i in range(5):
            if isinstance(funding_rounds, list) and len(funding_rounds) > i and isinstance(funding_rounds[i], dict):
                contact.__dict__[f'company_funding_rounds_{i+1}_name'] = funding_rounds[i].get('name', '')
            else:
                contact.__dict__[f'company_funding_rounds_{i+1}_name'] = ''
        # parent_company_information.parent_company_name
        parent_info = company_data.get('parent_company_information', {})
        if isinstance(parent_info, dict):
            contact.__dict__['company_parent_company_information_parent_company_name'] = parent_info.get('parent_company_name', '')
        else:
            contact.__dict__['company_parent_company_information_parent_company_name'] = ''
        # acquired_by_summary.acquirer_name
        acq_info = company_data.get('acquired_by_summary', {})
        if isinstance(acq_info, dict):
            contact.__dict__['company_acquired_by_summary_acquirer_name'] = acq_info.get('acquirer_name', '')
        else:
            contact.__dict__['company_acquired_by_summary_acquirer_name'] = ''
        # technologies_used.technology
        techs = company_data.get('technologies_used', [])
        for i in range(5):
            if isinstance(techs, list) and len(techs) > i and isinstance(techs[i], dict):
                contact.__dict__[f'company_technologies_used_{i+1}_technology'] = techs[i].get('technology', '')
            else:
                contact.__dict__[f'company_technologies_used_{i+1}_technology'] = ''
        # num_news_articles
        contact.__dict__['company_num_news_articles'] = company_data.get('num_news_articles', '')
        # --- MAXIMUM EXTRACTION (existing logic) ---
        for key, value in company_data.items():
            field_name = f"company_{key}"
            if isinstance(value, list):
                if value and isinstance(value[0], dict):
                    collection_fields = self.flatten_collection(value, field_name, max_items=10)
                    for cf_name, cf_value in collection_fields.items():
                        contact.__dict__[cf_name] = cf_value
                    contact.__dict__[f"{field_name}_json"] = json.dumps(value, ensure_ascii=False)
                    logger.info(f"Extracted {len(collection_fields)} fields from collection {field_name}")
                else:
                    contact.__dict__[field_name] = '|'.join(str(v) for v in value if v is not None)
            elif isinstance(value, dict):
                contact.__dict__[f"{field_name}_json"] = json.dumps(value, ensure_ascii=False)
                for sub_key, sub_value in value.items():
                    contact.__dict__[f"{field_name}_{sub_key}"] = str(sub_value) if sub_value is not None else ""
            else:
                contact.__dict__[field_name] = str(value) if value is not None else ""
        logger.debug(f"[DEBUG] Company fields added: {len([k for k in contact.__dict__.keys() if k.startswith('company_')])}")

    def enrich_employee_data(self, contact: ContactRecord, employee_data: Dict) -> None:
        """Populate employee fields from API data with maximum extraction and explicit high-value fields"""
        logger.debug(f"[DEBUG] Raw employee_data: {json.dumps(employee_data, indent=2, default=str)}")
        # --- PERSON CORE FIELDS ---
        contact.employee_id = str(employee_data.get('public_profile_id', employee_data.get('id', '')))
        # full_name.exact
        full_name = employee_data.get('full_name', {})
        if isinstance(full_name, dict):
            contact.__dict__['employee_full_name_exact'] = full_name.get('exact', '')
        else:
            contact.__dict__['employee_full_name_exact'] = employee_data.get('full_name', '')
        # headline, location_full.exact, location_country_iso2, connections_count, followers_count, primary_professional_email.exact
        for field in ['headline', 'location_country_iso2', 'connections_count', 'followers_count', 'active_experience_title', 'active_experience_company_id', 'total_experience_duration_months', 'last_graduation_date', 'recommendations_count', 'experience_change_last_identified_at']:
            contact.__dict__[f'employee_{field}'] = employee_data.get(field, '')
        # location_full.exact
        loc_full = employee_data.get('location_full', {})
        if isinstance(loc_full, dict):
            contact.__dict__['employee_location_full_exact'] = loc_full.get('exact', '')
        else:
            contact.__dict__['employee_location_full_exact'] = employee_data.get('location_full', '')
        # primary_professional_email.exact
        email_prof = employee_data.get('primary_professional_email', {})
        if isinstance(email_prof, dict):
            contact.__dict__['employee_primary_professional_email_exact'] = email_prof.get('exact', '')
        else:
            contact.__dict__['employee_primary_professional_email_exact'] = employee_data.get('primary_professional_email', '')
        # --- CAREER & EDUCATION ---
        # experience.position_title.exact, experience.duration_months, experience.company_name.exact
        experience = employee_data.get('experience', [])
        for i in range(5):
            if isinstance(experience, list) and len(experience) > i and isinstance(experience[i], dict):
                pos = experience[i]
                # position_title.exact
                pt = pos.get('position_title', {})
                if isinstance(pt, dict):
                    contact.__dict__[f'employee_experience_{i+1}_position_title_exact'] = pt.get('exact', '')
                else:
                    contact.__dict__[f'employee_experience_{i+1}_position_title_exact'] = pos.get('position_title', '')
                # duration_months
                contact.__dict__[f'employee_experience_{i+1}_duration_months'] = pos.get('duration_months', '')
                # company_name.exact
                cn = pos.get('company_name', {})
                if isinstance(cn, dict):
                    contact.__dict__[f'employee_experience_{i+1}_company_name_exact'] = cn.get('exact', '')
                else:
                    contact.__dict__[f'employee_experience_{i+1}_company_name_exact'] = pos.get('company_name', '')
            else:
                contact.__dict__[f'employee_experience_{i+1}_position_title_exact'] = ''
                contact.__dict__[f'employee_experience_{i+1}_duration_months'] = ''
                contact.__dict__[f'employee_experience_{i+1}_company_name_exact'] = ''
        # education.institution_name.exact, education.degree.exact
        education = employee_data.get('education', [])
        for i in range(3):
            if isinstance(education, list) and len(education) > i and isinstance(education[i], dict):
                inst = education[i]
                # institution_name.exact
                iname = inst.get('institution_name', {})
                if isinstance(iname, dict):
                    contact.__dict__[f'employee_education_{i+1}_institution_name_exact'] = iname.get('exact', '')
                else:
                    contact.__dict__[f'employee_education_{i+1}_institution_name_exact'] = inst.get('institution_name', '')
                # degree.exact
                deg = inst.get('degree', {})
                if isinstance(deg, dict):
                    contact.__dict__[f'employee_education_{i+1}_degree_exact'] = deg.get('exact', '')
                else:
                    contact.__dict__[f'employee_education_{i+1}_degree_exact'] = inst.get('degree', '')
            else:
                contact.__dict__[f'employee_education_{i+1}_institution_name_exact'] = ''
                contact.__dict__[f'employee_education_{i+1}_degree_exact'] = ''
        # --- SKILLS & INTERESTS ---
        for field in ['inferred_skills', 'historical_skills']:
            val = employee_data.get(field, [])
            contact.__dict__[f'employee_{field}'] = '|'.join(str(s) for s in val) if isinstance(val, list) else ''
        # --- RECENT ACTIVITY & TRIGGERS ---
        # recommendations_count already above
        # experience_change_last_identified_at already above
        # experience_recently_started.date_from, experience_recently_closed.date_to
        exp_started = employee_data.get('experience_recently_started', [])
        if isinstance(exp_started, list) and exp_started and isinstance(exp_started[0], dict):
            contact.__dict__['employee_experience_recently_started_date_from'] = exp_started[0].get('date_from', '')
        else:
            contact.__dict__['employee_experience_recently_started_date_from'] = ''
        exp_closed = employee_data.get('experience_recently_closed', [])
        if isinstance(exp_closed, list) and exp_closed and isinstance(exp_closed[0], dict):
            contact.__dict__['employee_experience_recently_closed_date_to'] = exp_closed[0].get('date_to', '')
        else:
            contact.__dict__['employee_experience_recently_closed_date_to'] = ''
        # --- MAXIMUM EXTRACTION (existing logic) ---
        for key, value in employee_data.items():
            field_name = f"employee_{key}"
            if isinstance(value, list):
                if value and isinstance(value[0], dict):
                    collection_fields = self.flatten_collection(value, field_name, max_items=10)
                    for cf_name, cf_value in collection_fields.items():
                        contact.__dict__[cf_name] = cf_value
                    contact.__dict__[f"{field_name}_json"] = json.dumps(value, ensure_ascii=False)
                    logger.info(f"Extracted {len(collection_fields)} fields from collection {field_name}")
                else:
                    contact.__dict__[field_name] = '|'.join(str(v) for v in value if v is not None)
            elif isinstance(value, dict):
                contact.__dict__[f"{field_name}_json"] = json.dumps(value, ensure_ascii=False)
                for sub_key, sub_value in value.items():
                    contact.__dict__[f"{field_name}_{sub_key}"] = str(sub_value) if sub_value is not None else ""
            else:
                contact.__dict__[field_name] = str(value) if value is not None else ""
        logger.debug(f"[DEBUG] Employee fields added: {len([k for k in contact.__dict__.keys() if k.startswith('employee_')])}")

    def enrich_contact(self, contact: ContactRecord) -> ContactRecord:
        initial_api_calls = self.client.stats.total_calls
        try:
            # Input validation for company_name
            if not contact.company_name or not isinstance(contact.company_name, str) or str(contact.company_name).lower() == 'nan':
                logger.warning(f"Skipping contact {contact.name}: invalid or missing company_name '{contact.company_name}'")
                contact.enrichment_status = "invalid_input"
                contact.enrichment_error = "Missing or invalid company_name"
                return contact
            logger.info(f"Enriching contact: {contact.name} at {contact.company_name}")
            # Step 1: Find company first (never use IDs from CSV)
            company_match, company_score = self.find_best_company_match(contact)
            logger.debug(f"[DEBUG] Company match: {company_match}, score: {company_score}")
            contact.company_match_score = company_score
            company_data = None
            if company_match:
                logger.debug(f"Found company match with score {company_score:.2f}")
                if isinstance(company_match, int):
                    detailed_company = self.client.get_company_details(company_match)
                    if isinstance(detailed_company, dict):
                        company_data = detailed_company
                    else:
                        logger.warning(f"Company ID {company_match} could not be resolved to a dict.")
                        company_data = None
                elif isinstance(company_match, dict):
                    if 'id' in company_match and len(company_match) < 10:
                        detailed_company = self.client.get_company_details(company_match['id'])
                        company_data = detailed_company if detailed_company else company_match
                    else:
                        company_data = company_match
                else:
                    logger.warning(f"Company match is not a dict or int: {company_match}")
                    company_data = None
                if company_data:
                    self.enrich_company_data(contact, company_data)
            else:
                logger.debug("No company match found")
            # Step 2: Find employee ONLY within the found company
            employee_match = None
            employee_score = 0.0
            if company_data:
                employee_match, employee_score = self.find_best_employee_match(contact, company_data)
                logger.debug(f"[DEBUG] Employee match: {employee_match}, score: {employee_score}")
            contact.employee_match_score = employee_score
            if employee_match:
                logger.debug(f"Found employee match with score {employee_score:.2f}")
                if isinstance(employee_match, int):
                    detailed_employee = self.client.get_member_details(employee_match)
                    if isinstance(detailed_employee, dict):
                        employee_match = detailed_employee
                    else:
                        logger.warning(f"Employee ID {employee_match} could not be resolved to a dict.")
                        employee_match = None
                elif isinstance(employee_match, dict):
                    if 'id' in employee_match and len(employee_match) < 15:
                        detailed_employee = self.client.get_member_details(employee_match['id'])
                        if detailed_employee:
                            employee_match = detailed_employee
                        else:
                            logger.warning(f"Employee match is not a dict or int: {employee_match}")
                            employee_match = None
                if employee_match:
                    self.enrich_employee_data(contact, employee_match)
                else:
                    logger.debug("No employee match found in company")
            else:
                logger.debug("Skipping employee search - no company found")
            # Set final status
            if company_data and employee_match:
                contact.enrichment_status = "success"
            elif company_data:
                contact.enrichment_status = "partial_success"
                contact.enrichment_error = "Company found but employee not found in company"
            elif contact.enrichment_status != "invalid_input":
                contact.enrichment_status = "no_matches"
                contact.enrichment_error = "No company match found"
            # After enrichment, robustly backfill all OUTPUT_SCHEMA fields
            for field in OUTPUT_SCHEMA:
                if not getattr(contact, field, None) and field in contact.__dict__:
                    # If field is present but empty, try to backfill from other possible locations
                    # (e.g., for company fields, check company_data; for employee fields, check employee_data)
                    # This is a placeholder for more advanced logic if needed
                    pass
                if field not in contact.__dict__ or contact.__dict__[field] in [None, '', [], {}]:
                    contact.__dict__[field] = ''
            # Log missing fields for this contact
            missing_fields = [f for f in OUTPUT_SCHEMA if contact.__dict__.get(f, '') == '']
            if missing_fields:
                logger.info(f"[ENRICHMENT] Contact '{contact.name}' missing fields: {missing_fields}")
        except Exception as e:
            logger.error(f"Error enriching contact {contact.name}: {e}")
            contact.enrichment_status = "error"
            contact.enrichment_error = str(e)
        finally:
            contact.api_calls_made = self.client.stats.total_calls - initial_api_calls
        return contact