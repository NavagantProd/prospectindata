#!/usr/bin/env python3
"""
Test script for CoreSignal API integration
This script tests the API connection and demonstrates the robust enrichment system.
"""

import requests
import json
import logging
from test_api import CoreSignalEnricher, safe_str

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# CoreSignal API configuration
CORESIGNAL_API_KEY = "rBLZ9TafWmj4OAiUn4Pb18unIv8XEUkt"
BASE_URL = "https://api.coresignal.com/cdapi/v2"
HEADERS = {
    "apikey": CORESIGNAL_API_KEY,
    "Content-Type": "application/json",
    "accept": "application/json"
}

def test_api_connection():
    """Test basic API connection"""
    logger.info("Testing CoreSignal API connection...")
    
    # Test with a simple endpoint that should always work
    url = f"{BASE_URL}/member/search/filter"
    payload = {"email": "test@example.com"}  # This will return empty but should not error
    
    try:
        response = requests.post(url, headers=HEADERS, json=payload, timeout=30)
        logger.info(f"API Test Response Status: {response.status_code}")
        logger.info(f"API Test Response Headers: {dict(response.headers)}")
        
        if response.status_code == 401:
            logger.error("❌ API Key authentication failed!")
            logger.error(f"Response: {response.text}")
            return False
        elif response.status_code == 422:
            logger.error("❌ Invalid payload format!")
            logger.error(f"Response: {response.text}")
            return False
        elif response.status_code == 200:
            logger.info("✅ API connection successful!")
            return True
        else:
            logger.warning(f"⚠️ Unexpected status code: {response.status_code}")
            logger.warning(f"Response: {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"❌ API connection failed: {e}")
        return False

def test_person_search(email=None, name=None, company=None):
    """Test person search with specific data"""
    logger.info(f"Testing person search with: email={email}, name={name}, company={company}")
    
    url = f"{BASE_URL}/member/search/filter"
    
    # Build search payload - try different formats
    payloads_to_try = []
    
    # Format 1: Direct fields
    if email:
        payloads_to_try.append({"email": email})
    if name and company:
        payloads_to_try.append({"full_name": name, "company_name": company})
    if name:
        payloads_to_try.append({"full_name": name})
    
    # Format 2: With filter wrapper (in case that's required)
    if email:
        payloads_to_try.append({"filter": {"email": email}})
    if name and company:
        payloads_to_try.append({"filter": {"full_name": name, "company_name": company}})
    if name:
        payloads_to_try.append({"filter": {"full_name": name}})
    
    if not payloads_to_try:
        logger.warning("No search criteria provided")
        return None
    
    for i, payload in enumerate(payloads_to_try, 1):
        logger.info(f"Trying payload format {i}: {payload}")
        
        try:
            response = requests.post(url, headers=HEADERS, json=payload, timeout=30)
            logger.info(f"Search Response Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"Search Response: {json.dumps(data, indent=2)}")
                
                if isinstance(data, list):
                    if len(data) > 0:
                        logger.info(f"✅ Found {len(data)} person(s)")
                        return data[0]  # Return first match
                    else:
                        logger.info("❌ No persons found")
                        continue  # Try next payload format
                elif isinstance(data, dict) and data:
                    logger.info("✅ Found person (dict response)")
                    return data
                else:
                    logger.info("❌ No persons found (empty response)")
                    continue  # Try next payload format
            elif response.status_code == 422:
                logger.warning(f"⚠️ Payload format {i} invalid: {response.text}")
                continue  # Try next payload format
            else:
                logger.error(f"❌ Search failed: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Search error with payload {i}: {e}")
            continue
    
    logger.info("❌ All payload formats failed")
    return None

def test_person_collect(person_id):
    """Test collecting detailed person data"""
    logger.info(f"Testing person collection for ID: {person_id}")
    person_id = int(float(person_id))
    url = f"{BASE_URL}/employee_clean/collect?ids={person_id}"
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        logger.info(f"Collect Response Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list) and data:
                data = data[0]
            logger.info(f"✅ Person data collected successfully")
            logger.info(f"Person name: {data.get('full_name', 'N/A')}")
            logger.info(f"Person email: {data.get('email', 'N/A')}")
            logger.info(f"Current company: {data.get('current_position', {}).get('company', 'N/A')}")
            return data
        else:
            logger.error(f"❌ Collection failed: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logger.error(f"❌ Collection error: {e}")
        return None

def test_detailed_enrichment():
    """Test detailed enrichment with a sample record."""
    logger.info("Testing detailed enrichment...")
    
    enricher = CoreSignalEnricher(CORESIGNAL_API_KEY)
    
    # Create a sample row (similar to your CSV structure)
    sample_row = {
        'contact_email': 'karan@assessprep.com',
        'contact_full_name': 'Karan Gupta',
        'firm_name': 'AssessPrep',
        'firm_website': 'http://assessprep.com'
    }
    
    # Test person enrichment
    logger.info("Testing person enrichment...")
    person_enrichment = enricher.enrich_person_data(sample_row)
    
    if person_enrichment.get('cs_person_id'):
        logger.info("✅ Person enrichment successful!")
        logger.info(f"   Person ID: {person_enrichment.get('cs_person_id')}")
        logger.info(f"   Skills: {person_enrichment.get('cs_person_skills', 'N/A')}")
        logger.info(f"   Experience count: {len([k for k in person_enrichment.keys() if 'cs_person_exp_' in k])}")
    else:
        logger.warning("❌ Person enrichment failed")
    
    # Test company enrichment
    logger.info("Testing company enrichment...")
    company_enrichment = enricher.enrich_company_data(sample_row)
    
    if company_enrichment.get('cs_company_id'):
        logger.info("✅ Company enrichment successful!")
        logger.info(f"   Company ID: {company_enrichment.get('cs_company_id')}")
        logger.info(f"   Size: {company_enrichment.get('cs_company_size', 'N/A')}")
        logger.info(f"   Industry: {company_enrichment.get('cs_company_industry', 'N/A')}")
    else:
        logger.warning("❌ Company enrichment failed")

def test_csv_processing():
    """Test processing a small sample CSV."""
    logger.info("Testing CSV processing...")
    
    # Create a small test CSV
    import pandas as pd
    
    test_data = [
        {
            'contact_email': 'karan@assessprep.com',
            'contact_full_name': 'Karan Gupta',
            'firm_name': 'AssessPrep',
            'firm_website': 'http://assessprep.com'
        },
        {
            'contact_email': 'brad@cyberhoot.com',
            'contact_full_name': 'Bradley Margist',
            'firm_name': 'CyberHoot',
            'firm_website': 'http://cyberhoot.com'
        }
    ]
    
    # Save test CSV
    test_df = pd.DataFrame(test_data)
    test_df.to_csv('test_sample.csv', index=False)
    logger.info("Created test_sample.csv with 2 records")
    
    # Process with CoreSignal
    enricher = CoreSignalEnricher(CORESIGNAL_API_KEY)
    enricher.process_csv('test_sample.csv', 'test_enriched.csv')
    
    logger.info("✅ CSV processing test completed!")

def test_employee_search(full_name=None, company_name=None):
    """Test employee search using Multi-source Employee API with Elasticsearch DSL."""
    logger.info(f"Testing employee search with: full_name={full_name}, company_name={company_name}")
    url = f"{BASE_URL}/employee_multi_source/search/es_dsl"
    
    # Build Elasticsearch DSL query
    must_clauses = []
    if full_name:
        must_clauses.append({"match": {"full_name": full_name}})
    if company_name:
        must_clauses.append({"match": {"experience.company_name": company_name}})
    if not must_clauses:
        logger.warning("No search criteria provided")
        return None
    dsl_query = {
        "query": {
            "bool": {
                "must": must_clauses
            }
        }
    }
    logger.info(f"Elasticsearch DSL query: {json.dumps(dsl_query, indent=2)}")
    try:
        response = requests.post(url, headers=HEADERS, json=dsl_query, timeout=30)
        logger.info(f"Search Response Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            logger.info(f"Search Response: {json.dumps(data, indent=2)}")
            if isinstance(data, list) and len(data) > 0:
                logger.info(f"✅ Found {len(data)} employee(s)")
                return data[0]  # Return first match
            elif isinstance(data, dict) and data.get('hits'):
                hits = data['hits']['hits']
                if hits:
                    logger.info(f"✅ Found {len(hits)} employee(s)")
                    return hits[0]['_source']
                else:
                    logger.info("❌ No employees found (empty hits)")
                    return None
            else:
                logger.info("❌ No employees found (empty response)")
                return None
        else:
            logger.error(f"❌ Search failed: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logger.error(f"❌ Search error: {e}")
        return None

def main():
    logger.info("=== CoreSignal Multi-source Employee API Test Suite ===")
    test_cases = [
        {
            "full_name": "Jacob Vorhees",
            "company_name": "Navagant"
        },
    ]
    for i, test_case in enumerate(test_cases, 1):
        logger.info(f"\n--- Test Case {i} ---")
        employee = test_employee_search(**test_case)
        if employee:
            logger.info(f"✅ Employee found: {employee.get('full_name', 'N/A')} (ID: {employee.get('id', 'N/A')})")
        else:
            logger.info("❌ Employee not found in CoreSignal database")
    logger.info("\n=== Test Complete ===")
    logger.info("If no employees were found, it means:")
    logger.info("1. The people in your distribution are not in CoreSignal's database")
    logger.info("2. The search criteria don't match CoreSignal's data format")
    logger.info("3. There might be data quality issues in the source data")

if __name__ == "__main__":
    main() 