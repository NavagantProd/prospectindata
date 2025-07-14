#!/usr/bin/env python3
"""
CoreSignal Lead Enrichment Script

SETUP INSTRUCTIONS:
1. Install dependencies:
   pip install requests pandas python-dotenv

2. Set environment variable:
   export CORESIGNAL_API_KEY="your_api_key_here"
   OR create a .env file with:
   CORESIGNAL_API_KEY=your_api_key_here

3. Prepare leads.csv with at least these columns:
   - contact_email (for person search)
   - contact_firm_name or firm_name (for company search)
   - contact_first_name, contact_last_name (optional, improves person matching)

4. Run the script:
   python coresignal_enrichment.py

The script will:
- Search for companies using Multi-Source Company API (more comprehensive data)
- Search for individuals using Employee API
- Enrich data with comprehensive company and person profiles
- Handle rate limiting, retries, and pagination
- Output enriched data to enriched.csv

FEATURES:
- Uses CoreSignal v2 API endpoints
- Robust error handling with exponential backoff
- Progress tracking and logging
- Handles missing values gracefully
- Normalizes and validates API responses
- Preserves all original lead data
"""

import os
import sys
import csv
import json
import time
import logging
import requests
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Any, Union
from dotenv import load_dotenv
import random
import re
import argparse
from fuzzywuzzy import fuzz
import tldextract
import dateutil.parser
import numpy as np
import collections.abc
from difflib import SequenceMatcher
import math

# Load environment variables
load_dotenv()

# Ensure config.py is importable regardless of working directory
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('coresignal_enrichment.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Now import OUTPUT_SCHEMA
try:
    from config import OUTPUT_SCHEMA
except ImportError as e:
    logger.error(f"Could not import OUTPUT_SCHEMA from config.py: {e}")
    sys.exit(1)

# Configuration
API_KEY = os.getenv('CORESIGNAL_API_KEY')
BASE_URL = "https://api.coresignal.com/cdapi"
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
INITIAL_RETRY_DELAY = 1
MAX_RETRY_DELAY = 60
RATE_LIMIT_DELAY = 2  # Base delay between requests

# API endpoints (updated to match working pipeline)
ENDPOINTS = {
    'company_search': f'{BASE_URL}/company_base/search/filter',
    'company_collect': f'{BASE_URL}/company_base/collect',
    'person_search': f'{BASE_URL}/member/search/filter',
    'person_collect': f'{BASE_URL}/member/collect',
}

# --- ENRICHMENT FUNCTIONS ---
def extract_domain(url_or_email: str) -> str:
    """Extract clean domain from URL or email"""
    if not url_or_email:
        return ""
    if '@' in url_or_email:
        domain = url_or_email.split('@')[1].lower().strip()
        if domain.startswith('mail.'):
            domain = domain[5:]
        return domain
    else:
        if not url_or_email.startswith(('http://', 'https://')):
            url_or_email = f"https://{url_or_email}"
        try:
            extracted = tldextract.extract(url_or_email)
            return f"{extracted.domain}.{extracted.suffix}".lower()
        except Exception as e:
            logger.error(f"Error extracting domain from {url_or_email}: {e}")
            return ""

def calculate_name_similarity(name1: str, name2: str) -> float:
    """Calculate similarity between two names"""
    if not name1 or not name2:
        return 0.0
    name1 = name1.lower().strip()
    name2 = name2.lower().strip()
    if name1 == name2:
        return 1.0
    return fuzz.ratio(name1, name2) / 100.0

def flatten_dict(d: dict, prefix: str = "") -> dict:
    """Recursively flatten a nested dictionary or list into a flat dict with dynamic keys."""
    flat = {}
    try:
        if isinstance(d, dict):
            for k, v in d.items():
                new_prefix = f"{prefix}_{k}" if prefix else k
                if isinstance(v, dict):
                    flat.update(flatten_dict(v, new_prefix))
                elif isinstance(v, list):
                    if all(isinstance(item, dict) for item in v):
                        for idx, item in enumerate(v, 1):
                            try:
                                flat.update(flatten_dict(item, f"{new_prefix}_{idx}"))
                            except Exception as e:
                                logger.error(f"Error flattening list item at {new_prefix}_{idx}: {e}")
                    else:
                        try:
                            flat[new_prefix] = '|'.join(str(item) for item in v if item is not None)
                        except Exception as e:
                            logger.error(f"Error joining array at {new_prefix}: {e}")
                else:
                    flat[new_prefix] = str(v) if v is not None else ''
        elif isinstance(d, list):
            for idx, item in enumerate(d, 1):
                try:
                    flat.update(flatten_dict(item, f"{prefix}_{idx}"))
                except Exception as e:
                    logger.error(f"Error flattening top-level list at {prefix}_{idx}: {e}")
        else:
            flat[prefix] = str(d) if d is not None else ''
    except Exception as e:
        logger.error(f"Error flattening dict at {prefix}: {e}")
    return flat

def flatten_multisource_json(d, parent_key='', sep='_', max_items=5):
    """Recursively flatten a nested dict/list structure for multi-source CoreSignal JSON."""
    items = {}
    if isinstance(d, dict):
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.update(flatten_multisource_json(v, new_key, sep, max_items))
            elif isinstance(v, list):
                for i, item in enumerate(v[:max_items], 1):
                    if isinstance(item, dict):
                        items.update(flatten_multisource_json(item, f"{new_key}_{i}", sep, max_items))
                    else:
                        items[f"{new_key}_{i}"] = item
                if all(isinstance(item, (str, int, float, bool, type(None))) for item in v):
                    # Also join all values for a summary field
                    items[new_key] = ", ".join(str(x) for x in v if x is not None)
            else:
                items[new_key] = v
    else:
        items[parent_key] = d
    return items

def flatten_all_fields(d, parent_key='', sep='_', max_items=5):
    """Recursively flatten a nested dict/list structure for any JSON, including all arrays and dicts."""
    items = {}
    if isinstance(d, dict):
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.update(flatten_all_fields(v, new_key, sep, max_items))
            elif isinstance(v, list):
                # For lists of dicts, flatten each up to max_items
                if all(isinstance(item, dict) for item in v):
                    for i, item in enumerate(v[:max_items], 1):
                        items.update(flatten_all_fields(item, f"{new_key}_{i}", sep, max_items))
                # For lists of primitives, join as string and also keep first N
                else:
                    items[new_key] = '|'.join(str(item) for item in v if item is not None)
                    for i, item in enumerate(v[:max_items], 1):
                        items[f"{new_key}_{i}"] = item
            else:
                items[new_key] = v
    elif isinstance(d, list):
        for i, item in enumerate(d[:max_items], 1):
            items.update(flatten_all_fields(item, f"{parent_key}_{i}", sep, max_items))
    else:
        items[parent_key] = d
    return items

def find_best_company_match(api, company_name: str, company_website: str) -> dict:
    """Find the best company match using multiple strategies and fuzzy matching."""
    best_match = None
    best_score = 0.0
    search_attempts = []
    # Strategy 1: Name + Website
    if company_name and company_website:
        search_attempts.append((company_name, company_website))
    # Strategy 2: Name only
    if company_name:
        search_attempts.append((company_name, ""))
    # Strategy 3: Website only
    if company_website:
        search_attempts.append(("", company_website))
    # Strategy 4: Fuzzy name (if name is long enough)
    if company_name and len(company_name) > 4:
        fuzzy_name = company_name[:max(5, len(company_name)//2)]
        search_attempts.append((fuzzy_name, ""))
    for name, website in search_attempts:
        logger.info(f"[COMPANY SEARCH] Trying name='{name}' website='{website}'")
        companies = []
        try:
            result = api.search_company(name, website)
            if result:
                companies = [result]
        except Exception as e:
            logger.error(f"Error during company search: {e}")
        for company in companies:
            score = calculate_name_similarity(company_name, company.get('name', ''))
            # Boost score if website matches
            if company_website and company.get('website'):
                contact_domain = extract_domain(company_website)
                company_domain = extract_domain(company['website'])
                if contact_domain and company_domain and contact_domain == company_domain:
                    score += 0.3
            logger.info(f"[COMPANY SEARCH] Candidate: {company.get('name','')} | Score: {score}")
            if score > best_score:
                best_score = score
                best_match = company
        if best_score >= 0.7:
            break  # Good enough match found
    logger.info(f"[COMPANY SEARCH] Best match score: {best_score}")
    return best_match

def find_best_employee_match(api, full_name: str, company_name: str) -> dict:
    """Find the best employee match using multiple strategies."""
    best_match = None
    best_score = 0.0
    search_attempts = []
    if full_name and company_name:
        search_attempts.append((full_name, company_name))
    if full_name:
        search_attempts.append((full_name, ""))
    for name, company in search_attempts:
        logger.info(f"[EMPLOYEE SEARCH] Trying name='{name}' company='{company}'")
        try:
            result = api.search_person(name, company)
            if result:
                best_match = result
                best_score = 1.0
                break
        except Exception as e:
            logger.error(f"Error during employee search: {e}")
    logger.info(f"[EMPLOYEE SEARCH] Best match score: {best_score}")
    return best_match

def smart_postprocess(row: dict) -> dict:
    """Try to convert non-actionable or empty fields into usable data using creative logic."""
    # 1. Parse experience durations
    for i in range(1, 6):
        dur_key = f"employee_experience_{i}_duration_months"
        if dur_key in row and (not row[dur_key] or not str(row[dur_key]).isdigit()):
            exp_str = row.get(f"employee_experience_{i}_duration", "")
            if exp_str:
                years = months = 0
                if 'year' in exp_str:
                    try:
                        years = int(exp_str.split('year')[0].strip())
                    except Exception:
                        years = 0
                if 'month' in exp_str:
                    try:
                        months = int(exp_str.split('month')[0].split()[-2].strip()) if 'month' in exp_str else 0
                    except Exception:
                        months = 0
                row[dur_key] = str(years * 12 + months)
    # 2. Parse dates to year
    for key in list(row):
        if key.endswith('_date') or key.endswith('_announced_date'):
            try:
                if row[key]:
                    dt = dateutil.parser.parse(row[key], fuzzy=True)
                    row[key + '_year'] = str(dt.year)
            except Exception:
                pass
    # 3. Convert string numbers
    for key in list(row):
        if isinstance(row[key], str) and row[key].replace(',', '').replace('.', '').isdigit():
            try:
                row[key + '_numeric'] = str(float(row[key].replace(',', '')))
            except Exception:
                pass
    return row

def extract_member_collections(member_json: dict) -> dict:
    """Extract and map member (employee) fields from both base and multi-source JSON to schema fields."""
    mapped = {}
    if not member_json:
        return mapped
    # Detect multi-source vs base by presence of multi-source keys
    is_multisource = 'experience' in member_json or 'full_name' in member_json
    # --- Common fields ---
    mapped['employee_id'] = member_json.get('id', '')
    mapped['employee_full_name'] = member_json.get('full_name', '') or member_json.get('name', '')
    mapped['employee_first_name'] = member_json.get('first_name', '')
    mapped['employee_last_name'] = member_json.get('last_name', '')
    mapped['employee_headline'] = member_json.get('headline', '') or member_json.get('user_generated_headline', '')
    mapped['employee_url'] = member_json.get('linkedin_url', '') or member_json.get('url', '')
    mapped['employee_canonical_url'] = member_json.get('canonical_url', '')
    mapped['employee_location'] = member_json.get('location_full', '') or member_json.get('location', '')
    mapped['employee_location_full_exact'] = member_json.get('location_full', '') or member_json.get('location', '')
    mapped['employee_country'] = member_json.get('location_country', '') or member_json.get('country', '')
    mapped['employee_connections_count'] = member_json.get('connections_count', '')
    mapped['employee_followers_count'] = member_json.get('followers_count', '')
    mapped['employee_summary'] = member_json.get('summary', '')
    mapped['employee_created'] = member_json.get('created_at', '') or member_json.get('created', '')
    mapped['employee_last_updated'] = member_json.get('last_updated_at', '') or member_json.get('last_updated', '')
    mapped['employee_industry'] = member_json.get('industry', '')
    mapped['employee_recommendations_count'] = member_json.get('recommendations_count', '')
    mapped['employee_inferred_skills'] = ', '.join(member_json.get('inferred_skills', []))
    mapped['employee_historical_skills'] = ', '.join(member_json.get('historical_skills', []))
    mapped['employee_last_graduation_date'] = member_json.get('last_graduation_date', '')
    mapped['employee_total_experience_duration_months'] = member_json.get('total_experience_duration_months', '')
    # --- Multi-source specific fields ---
    if is_multisource:
        # Experience (up to 5)
        exp = member_json.get('experience', [])
        for i, e in enumerate(exp[:5], 1):
            mapped[f'employee_experience_{i}_position_title_exact'] = e.get('title', '')
            mapped[f'employee_experience_{i}_company_name_exact'] = e.get('company_name', '')
            mapped[f'employee_experience_{i}_duration'] = e.get('duration', '')
            mapped[f'employee_experience_{i}_duration_months'] = e.get('duration_months', '')
            mapped[f'employee_experience_{i}_date_from'] = e.get('date_from', '')
            mapped[f'employee_experience_{i}_date_to'] = e.get('date_to', '')
        # Education (up to 3)
        edu = member_json.get('education', [])
        for i, e in enumerate(edu[:3], 1):
            mapped[f'employee_education_{i}_institution_name_exact'] = e.get('institution_name', '')
            mapped[f'employee_education_{i}_degree_exact'] = e.get('degree', '')
        mapped['employee_primary_professional_email_exact'] = member_json.get('primary_professional_email', '')
        mapped['employee_active_experience_title'] = member_json.get('active_experience_title', '')
        mapped['employee_active_experience_company_id'] = member_json.get('active_experience_company_id', '')
    else:
        # Base endpoint fields
        exp = [e for e in member_json.get('member_experience_collection', []) if not e.get('deleted')]
        for i, e in enumerate(exp[:5], 1):
            mapped[f'employee_experience_{i}_position_title_exact'] = e.get('title', '')
            mapped[f'employee_experience_{i}_company_name_exact'] = e.get('company_name', '')
            mapped[f'employee_experience_{i}_duration'] = e.get('duration', '')
            mapped[f'employee_experience_{i}_duration_months'] = ''  # Will be postprocessed
            mapped[f'employee_experience_{i}_date_from'] = e.get('date_from', '')
            mapped[f'employee_experience_{i}_date_to'] = e.get('date_to', '')
        edu = [e for e in member_json.get('member_education_collection', []) if not e.get('deleted')]
        for i, e in enumerate(edu[:3], 1):
            mapped[f'employee_education_{i}_institution_name_exact'] = e.get('title', '')
            mapped[f'employee_education_{i}_degree_exact'] = e.get('subtitle', '')
    return mapped

def extract_company_collections(company_json: dict) -> dict:
    """Extract and map company fields from both base and multi-source JSON to schema fields. Now fully comprehensive for all actionable fields in OUTPUT_SCHEMA."""
    mapped = {}
    if not company_json:
        return mapped
    # Helper for flattening arrays and dicts
    def get_list(json_obj, key):
        v = json_obj.get(key, [])
        return v if isinstance(v, list) else ([v] if v else [])
    # Dynamic mapping for every field in OUTPUT_SCHEMA
    for field in OUTPUT_SCHEMA:
        # Handle flattened arrays (e.g., company_updates_1_date, company_funding_rounds_2_name, etc.)
        if any(field.startswith(prefix) for prefix in ["company_updates_", "company_funding_rounds_", "company_technologies_used_", "company_locations_", "company_competitors_", "company_phone_number_", "company_email_", "company_visits_country_", "company_similar_website_", "company_linkedin_followers_", "company_employees_", "company_key_executive_", "company_top_previous_company_", "company_top_next_company_"]):
            # Parse index and subfield
            parts = field.split('_')
            try:
                idx = int(parts[-2]) - 1
                base = '_'.join(parts[:-2])
                subfield = parts[-1]
                arr = get_list(company_json, base)
                if idx < len(arr):
                    val = arr[idx].get(subfield, "") if isinstance(arr[idx], dict) else arr[idx]
                else:
                    val = ""
                mapped[field] = val
            except Exception:
                mapped[field] = ""
        else:
            # Direct or nested dict mapping
            keys = field.split('_')
            val = company_json
            for k in keys:
                if isinstance(val, dict) and k in val:
                    val = val[k]
                else:
                    val = ""
                    break
            mapped[field] = val if val not in [None, [], {}] else ""
    # Log any JSON fields not mapped
    for k in company_json:
        if k not in mapped:
            logger.debug(f"[ENRICH] Unmapped company field: {k}")
    return mapped

def enrich_lead(api, lead: dict) -> dict:
    """Enrich a single lead with robust company and employee finding, flattening, and field population."""
    enriched = lead.copy()
    contact_email = lead.get('contact_email', '').strip()
    contact_full_name = lead.get('contact_full_name', '').strip()
    contact_firm_name = lead.get('contact_firm_name', '').strip()
    cs_company_website = lead.get('cs_company_website', '').strip()
    # --- Company search ---
    company_data = None
    try:
        best_company = find_best_company_match(api, contact_firm_name, cs_company_website)
        if best_company:
            company_id = best_company.get('id')
            if company_id:
                logger.info(f"[ENRICHMENT] Collecting company by id: {company_id}")
                company_data = api.collect_company(company_id=str(company_id))
    except Exception as e:
        logger.error(f"Error during company enrichment: {e}")
    # --- Extract and flatten ALL company fields ---
    company_flat = flatten_all_fields(company_data, parent_key='company') if company_data else {}
    company_flat = postprocess_flattened_for_schema(company_flat, OUTPUT_SCHEMA)
    company_map = extract_company_collections(company_data) if company_data else {}
    # --- Employee search (only if company found) ---
    person_data = None
    try:
        if contact_full_name and company_data:
            best_person = find_best_employee_match(api, contact_full_name, contact_firm_name)
            if best_person:
                person_id = best_person.get('id')
                if person_id:
                    logger.info(f"[ENRICHMENT] Collecting person by id: {person_id}")
                    person_data = api.collect_person(person_id=str(person_id))
        # Fallback: try name only
        if not person_data and contact_full_name:
            logger.info(f"[ENRICHMENT] [Fallback] Attempting person search by name only: '{contact_full_name}'")
            best_person = find_best_employee_match(api, contact_full_name, "")
            if best_person:
                person_id = best_person.get('id')
                if person_id:
                    person_data = api.collect_person(person_id=str(person_id))
    except Exception as e:
        logger.error(f"Error during person enrichment: {e}")
    person_flat = flatten_all_fields(person_data, parent_key='employee') if person_data else {}
    person_flat = postprocess_flattened_for_schema(person_flat, OUTPUT_SCHEMA)
    person_map = extract_member_collections(person_data) if person_data else {}
    # --- Schema-driven population ---
    for field in OUTPUT_SCHEMA:
        val = None
        sources = [company_map, person_map, company_flat, person_flat, enriched]
        key_variants = [field]
        # --- Add dynamic collection/array pattern matching ---
        m = re.match(r'(company|employee)_(\w+?)_(\d+)_(\w+)', field)
        if m:
            prefix, coll, idx, sub = m.groups()
            key_variants.append(f"{prefix}_{coll}_{idx}_{sub}")
            key_variants.append(f"{prefix}_{coll}_collection_{idx}_{sub}")
            key_variants.append(f"{prefix}_{prefix}_{coll}_collection_{idx}_{sub}")
            key_variants.append(f"cs_{prefix}_{coll}_collection_{idx}_{sub}")
        # --- Add common aliases and flattening patterns ---
        # Remove company_ or employee_ prefix for base keys
        if field.startswith("company_"):
            base = field[len("company_"):]
            key_variants.append(base)
            # Common alias patterns
            if base == "display_name":
                key_variants.append("name")
            if base == "website_url":
                key_variants.append("website")
            if base == "logo_url":
                key_variants.append("logo")
            if base == "industry":
                key_variants.append("industry")
            if base == "description":
                key_variants.append("description")
            if base == "size":
                key_variants.append("size")
            if base == "founded":
                key_variants.append("founded_year")
            if base == "headquarters":
                key_variants.append("headquarters_city")
                key_variants.append("headquarters_new_address")
            # Flattening patterns for collections
            if "updates_" in base:
                m = re.match(r"updates_(\d+)_(.*)", base)
                if m:
                    idx, sub = m.groups()
                    key_variants.append(f"company_company_updates_collection_{idx}_{sub}")
            if "locations_" in base:
                m = re.match(r"locations_(\d+)_(.*)", base)
                if m:
                    idx, sub = m.groups()
                    key_variants.append(f"company_company_locations_collection_{idx}_location_{sub}")
                    key_variants.append(f"company_company_locations_collection_{idx}_{sub}")
            if "specialties_" in base:
                m = re.match(r"specialties_(\d+)_(.*)", base)
                if m:
                    idx, sub = m.groups()
                    key_variants.append(f"company_company_specialties_collection_{idx}_{sub}")
            if "featured_employees_" in base:
                m = re.match(r"featured_employees_(\d+)_(.*)", base)
                if m:
                    idx, sub = m.groups()
                    key_variants.append(f"company_company_featured_employees_collection_{idx}_{sub}")
            if "funding_rounds_" in base:
                m = re.match(r"funding_rounds_(\d+)_(.*)", base)
                if m:
                    idx, sub = m.groups()
                    key_variants.append(f"company_company_funding_rounds_collection_{idx}_{sub}")
            if "also_viewed_" in base:
                m = re.match(r"also_viewed_(\d+)_(.*)", base)
                if m:
                    idx, sub = m.groups()
                    key_variants.append(f"company_company_also_viewed_collection_{idx}_{sub}")
            if "crunchbase_info_" in base:
                m = re.match(r"crunchbase_info_(\d+)_(.*)", base)
                if m:
                    idx, sub = m.groups()
                    key_variants.append(f"company_company_crunchbase_info_collection_{idx}_{sub}")
            if "featured_investors_" in base:
                m = re.match(r"featured_investors_(\d+)_(.*)", base)
                if m:
                    idx, sub = m.groups()
                    key_variants.append(f"company_company_featured_investors_collection_{idx}_{sub}")
        if field.startswith("employee_"):
            base = field[len("employee_"):]
            key_variants.append(base)
            # Common alias patterns
            if base == "full_name":
                key_variants.append("name")
            if base == "headline":
                key_variants.append("user_generated_headline")
            if base == "url":
                key_variants.append("professional_network_url")
            if base == "canonical_url":
                key_variants.append("canonical_url")
            if base == "location":
                key_variants.append("location_full")
            # Flattening patterns for collections
            if "experience_" in base:
                m = re.match(r"experience_(\d+)_(.*)", base)
                if m:
                    idx, sub = m.groups()
                    key_variants.append(f"employee_member_experience_collection_{idx}_{sub}")
            if "education_" in base:
                m = re.match(r"education_(\d+)_(.*)", base)
                if m:
                    idx, sub = m.groups()
                    key_variants.append(f"employee_member_education_collection_{idx}_{sub}")
        found = False
        closest = None
        closest_val = None
        max_ratio = 0.0
        used_key = None
        # Try all sources and key variants
        for src in sources:
            for k in key_variants:
                if k in src and src[k] not in [None, '', [], {}]:
                    val = src[k]
                    found = True
                    used_key = k
                    break
            if found:
                break
            # Find closest key for debugging and fallback
            for key in src.keys():
                ratio = SequenceMatcher(None, key.lower(), field.lower()).ratio()
                if ratio > max_ratio:
                    max_ratio = ratio
                    closest = key
                    closest_val = src[key]
        # --- NEW: Always check debug/flattened dicts as last resort ---
        if val in [None, '', [], {}]:
            for flat in [company_flat, person_flat]:
                if field in flat and flat[field] not in [None, '', [], {}]:
                    val = flat[field]
                    used_key = field
                    logger.info(f"[ENRICH][USED_KEY][DEBUG_FLAT] Field '{field}' populated from debug flat key '{field}'")
                    break
        # --- Fuzzy fallback in debug/flattened dicts ---
        if val in [None, '', [], {}]:
            for flat in [company_flat, person_flat]:
                for key in flat.keys():
                    ratio = SequenceMatcher(None, key.lower(), field.lower()).ratio()
                    if ratio > 0.8 and flat[key] not in [None, '', [], {}]:
                        val = flat[key]
                        used_key = key
                        logger.warning(f"[ENRICH][MISSING][DEBUG_FLAT_FUZZY] Field '{field}' not found, using closest debug flat key '{key}' (similarity {ratio:.2f})")
                        break
                if val not in [None, '', [], {}]:
                    break
        # --- End new debug/flat fallback ---
        if val in [None, '', [], {}]:
            if max_ratio > 0.90 and closest_val not in [None, '', [], {}]:
                val = closest_val
                logger.warning(f"[ENRICH][MISSING][FUZZY] Field '{field}' not found, using closest key '{closest}' (similarity {max_ratio:.2f})")
                used_key = closest
            elif closest:
                logger.warning(f"[ENRICH][MISSING][ALIAS] Field '{field}' not found, closest match: '{closest}' (similarity {max_ratio:.2f})")
            else:
                logger.warning(f"[ENRICH][MISSING] Field '{field}' not found in any enrichment source for lead: {enriched.get('name', '')} / {enriched.get('email', '')}")
            if val in [None, '', [], {}]:
                val = ''
        if used_key:
            logger.info(f"[ENRICH][USED_KEY] Field '{field}' populated from key '{used_key}'")
        enriched[field] = val
    # --- Add ALL flattened fields for debug CSV ---
    enriched['company_raw_json'] = company_data if company_data else ''
    enriched['employee_raw_json'] = person_data if person_data else ''
    # Add all company/person flat fields not in OUTPUT_SCHEMA
    for k, v in {**company_flat, **person_flat}.items():
        if k not in OUTPUT_SCHEMA:
            enriched[k] = v
    # Log unmapped fields for review
    if company_data:
        for k in company_flat:
            if k not in OUTPUT_SCHEMA:
                logger.debug(f"[ENRICH] Unmapped company field: {k}")
    if person_data:
        for k in person_flat:
            if k not in OUTPUT_SCHEMA:
                logger.debug(f"[ENRICH] Unmapped person field: {k}")
    return enriched

def postprocess_flattened_for_schema(flat, schema):
    """Map collection keys in the flattened dict to schema-expected keys."""
    collection_map = {
        "company_company_updates_collection": "company_updates",
        "company_company_funding_rounds_collection": "company_funding_rounds",
        "company_company_locations_collection": "company_locations",
        "company_company_competitors_collection": "company_competitors",
        "company_company_technologies_used_collection": "company_technologies_used",
        "company_company_key_executive_collection": "company_key_executive",
        "company_company_top_previous_company_collection": "company_top_previous_company",
        "company_company_top_next_company_collection": "company_top_next_company",
        "company_company_phone_number_collection": "company_phone_number",
        "company_company_email_collection": "company_email",
        "company_company_visits_country_collection": "company_visits_country",
        "company_company_similar_website_collection": "company_similar_website",
        "company_company_linkedin_followers_collection": "company_linkedin_followers",
        "company_company_employees_collection": "company_employees",
        # Add more as needed
    }
    for flat_key in list(flat.keys()):
        for coll_prefix, schema_prefix in collection_map.items():
            if flat_key.startswith(coll_prefix):
                rest = flat_key[len(coll_prefix):]
                if rest.startswith('_'):
                    rest = rest[1:]
                schema_key = f"{schema_prefix}_{rest}"
                if schema_key in schema:
                    flat[schema_key] = flat[flat_key]
    return flat

def main():
    parser = argparse.ArgumentParser(description="CoreSignal Lead Enrichment Script")
    parser.add_argument('--sample', action='store_true', help='Process only the first row of leads.csv')
    parser.add_argument('--row', type=int, help='Process only the specified row index (0-based) of leads.csv')
    parser.add_argument('--postprocess-report', action='store_true', help='Print a report of fields that were enhanced or could be enhanced')
    parser.add_argument('--debug-csv', action='store_true', help='Output a debug CSV with all flattened fields for inspection')
    parser.add_argument('--local-json', action='store_true', help='Use local JSON files for enrichment (companycurl.txt, curlc2.txt)')
    parser.add_argument('--n', type=int, default=None, help='Number of rows to process from leads.csv')
    args = parser.parse_args()
    if not API_KEY:
        logger.error("CORESIGNAL_API_KEY environment variable not set")
        sys.exit(1)
    input_file = 'leads.csv'
    if not os.path.exists(input_file):
        logger.error(f"Input file '{input_file}' not found")
        sys.exit(1)
    output_file = 'enriched.csv'

    # Read input CSV
    try:
        df = pd.read_csv(input_file)
    except Exception as e:
        logger.error(f"Failed to read input file: {e}")
        sys.exit(1)
    if args.n is not None:
        df = df.head(args.n)
    elif args.sample:
        df = df.head(1)
    elif args.row is not None:
        df = df.iloc[[args.row]]

    # Initialize API client
    try:
        from coresignal_client import CoreSignalClient
        api = CoreSignalClient(API_KEY)
    except Exception as e:
        logger.error(f"Failed to initialize CoreSignalClient: {e}")
        sys.exit(1)

    # At the start of main(), add a check
    try:
        _ = OUTPUT_SCHEMA
    except NameError:
        logger.error("OUTPUT_SCHEMA is not defined. Please check config.py and the import at the top of the file.")
        sys.exit(1)

    # Add to OUTPUT_SCHEMA if not present
    if 'is_pe_vc_funded' not in OUTPUT_SCHEMA:
        OUTPUT_SCHEMA.append('is_pe_vc_funded')

    # --- At the start of main(), after loading leads.csv ---
    # Ensure 'is_opened' is the first column in OUTPUT_SCHEMA
    if 'is_opened' not in OUTPUT_SCHEMA:
        OUTPUT_SCHEMA.insert(0, 'is_opened')

    # Enrich each lead
    enriched_rows = []
    postprocess_changes = []
    debug_rows = []
    for idx, row in df.iterrows():
        try:
            if args.local_json and idx == 0:
                import json
                def load_json_robust(filepath):
                    try:
                        with open(filepath, 'r', encoding='utf-8') as f:
                            text = f.read()
                        # Find the first '{' and last '}'
                        start = text.find('{')
                        end = text.rfind('}')
                        if start != -1 and end != -1 and end > start:
                            json_str = text[start:end+1]
                            return json.loads(json_str)
                        else:
                            raise ValueError('No JSON object found in file')
                    except Exception as e:
                        logger.error(f"[LOCAL ENRICHMENT] Could not parse JSON from {filepath}: {e}")
                        return None
                try:
                    company_data = load_json_robust('companycurl.txt')
                    person_data = load_json_robust('curlc2.txt')
                    if not company_data or not person_data:
                        raise ValueError('One or both local JSONs could not be loaded')
                    company_flat = flatten_all_fields(company_data, parent_key='company') if company_data else {}
                    company_flat = postprocess_flattened_for_schema(company_flat, OUTPUT_SCHEMA)
                    company_map = extract_company_collections(company_data) if company_data else {}
                    person_flat = flatten_all_fields(person_data, parent_key='employee') if person_data else {}
                    person_flat = postprocess_flattened_for_schema(person_flat, OUTPUT_SCHEMA)
                    person_map = extract_member_collections(person_data) if person_data else {}
                    enriched = row.to_dict()
                    # --- Robust is_opened extraction ---
                    is_opened = None
                    if 'email_opened' in row:
                        is_opened = row['email_opened']
                    else:
                        # Fallback: case/whitespace-insensitive match
                        for col in row.index:
                            if col.strip().lower() == 'email_opened':
                                is_opened = row[col]
                                break
                        if is_opened is None:
                            for col in row.index:
                                if 'open' in col.strip().lower():
                                    is_opened = row[col]
                                    break
                    # Normalize is_opened: if NaN/None/empty, set to 0; else cast to int
                    if is_opened is None or (isinstance(is_opened, float) and math.isnan(is_opened)) or str(is_opened).strip() == '':
                        is_opened = 0
                    else:
                        is_opened = int(float(is_opened))
                    logger.info(f"[DEBUG] Row {idx} final is_opened value: {is_opened} (type: {type(is_opened)})")
                    enriched_row = {'is_opened': is_opened}
                    # --- Enhanced mapping logic ---
                    for field in OUTPUT_SCHEMA:
                        if field == 'is_opened':
                            continue  # Already set, do not remap
                        val = None
                        sources = [company_map, person_map, company_flat, person_flat, enriched]
                        key_variants = [field]
                        # Add common aliases and flattening patterns
                        # ... (existing alias logic) ...
                        # Try all key variants in all sources
                        found = False
                        for src in sources:
                            for k in key_variants:
                                if k in src and src[k] not in [None, '', [], {}]:
                                    val = src[k]
                                    found = True
                                    break
                            if found:
                                break
                        # Fuzzy match if still not found
                        if not found:
                            max_ratio = 0
                            closest_key = None
                            for src in sources:
                                for k in src.keys():
                                    ratio = SequenceMatcher(None, field, k).ratio()
                                    if ratio > max_ratio:
                                        max_ratio = ratio
                                        closest_key = k
                                        val = src[k]
                            if max_ratio > 0.8 and val not in [None, '', [], {}]:
                                logger.info(f"[ENRICH][FUZZY] Field '{field}' populated from fuzzy key '{closest_key}' (similarity {max_ratio:.2f})")
                                found = True
                        if not found or val in [None, '', [], {}]:
                            logger.warning(f"[ENRICH][MISSING][DEBUG] Field '{field}' not found. Tried: {key_variants}. Available keys: {list(company_flat.keys())[:10]} ...")
                        enriched_row[field] = val if val not in [None, '', [], {}] else ''
                    # --- Actionable field fill summary (inside enrichment loop) ---
                    actionable_fields = [f for f in OUTPUT_SCHEMA if not any(x in f for x in ['id', 'email', 'url', 'contact', 'canonical', 'hash', 'api_calls', 'score', 'error', 'status', 'is_opened'])]
                    filled = sum(1 for f in actionable_fields if enriched_row.get(f, '') not in ['', None, [], {}])
                    logger.info(f"[SUMMARY] Row actionable fields filled: {filled} / {len(actionable_fields)}")
                    # Add all flattened fields for debug
                    enriched['company_raw_json'] = company_data if company_data else ''
                    enriched['employee_raw_json'] = person_data if person_data else ''
                    for k, v in {**company_flat, **person_flat}.items():
                        if k not in OUTPUT_SCHEMA:
                            enriched[k] = v
                    debug_row = {**row.to_dict(), **enriched}
                    debug_rows.append(debug_row)
                    enriched_post = smart_postprocess(enriched.copy())
                    changes = {k: (enriched[k], enriched_post[k]) for k in enriched if enriched[k] != enriched_post[k]}
                    if changes:
                        postprocess_changes.append({'row': idx, 'changes': changes})
                    enriched_rows.append(enriched_post)
                    logger.info(f"[LOCAL ENRICHMENT] Populated lead {idx+1} from local JSONs.")
                    continue
                except Exception as e:
                    logger.error(f"[LOCAL ENRICHMENT] Failed to load local JSONs: {e}")
                    # Fallback to API mode for this row
            enriched = enrich_lead(api, row.to_dict())
            enriched_post = smart_postprocess(enriched.copy())
            changes = {k: (enriched[k], enriched_post[k]) for k in enriched if enriched[k] != enriched_post[k]}
            if changes:
                postprocess_changes.append({'row': idx, 'changes': changes})
            enriched_rows.append(enriched_post)
            debug_row = {**row.to_dict(), **enriched, **enriched_post}
            debug_rows.append(debug_row)
            logger.info(f"Processed lead {idx+1}/{len(df)}")
        except Exception as e:
            logger.error(f"Error enriching lead {idx}: {e}")
            enriched_rows.append(row.to_dict())
        # --- At the end of the enrichment loop for each row ---
        # actionable_fields = [f for f in OUTPUT_SCHEMA if not any(x in f for x in ['id', 'email', 'url', 'contact', 'canonical', 'hash', 'api_calls', 'score', 'error', 'status', 'is_opened'])]
        # filled = sum(1 for f in actionable_fields if enriched_row.get(f, '') not in ['', None, [], {}])
        # logger.info(f"[SUMMARY] Row actionable fields filled: {filled} / {len(actionable_fields)}")

    # Write output CSV
    try:
        out_df = pd.DataFrame(enriched_rows)
        # Use reindex to avoid fragmentation and ensure all columns are present
        out_df = out_df.reindex(columns=OUTPUT_SCHEMA, fill_value="")
        # Warn if any row is all empty strings
        for idx, row in out_df.iterrows():
            if all((str(x).strip() == "" for x in row)):
                logger.warning(f"Enriched row {idx} is completely empty after processing!")
        out_df.to_csv(output_file, index=False)
        logger.info(f"Successfully wrote {len(out_df)} enriched leads to {output_file}")
    except Exception as e:
        logger.error(f"Failed to write output file: {e}")
        sys.exit(1)

    # Postprocess report
    if args.postprocess_report:
        print("\n=== Postprocess Enhancement Report ===")
        for change in postprocess_changes:
            print(f"Row {change['row']}: {change['changes']}")
        print("=== End of Report ===\n")

    if args.debug_csv:
        pd.DataFrame(debug_rows).to_csv('enriched_debug.csv', index=False)
        logger.info(f"Debug CSV with all flattened fields written to 'enriched_debug.csv'")

    # --- After writing the output CSV ---
    df_out = pd.DataFrame(enriched_rows)
    df_out = df_out.reindex(columns=OUTPUT_SCHEMA)
    df_out.to_csv('enriched.csv', index=False)
    logger.info(f"[DEBUG] First 5 rows of enriched.csv:\n{df_out.head()}\n")


if __name__ == "__main__":
    main()