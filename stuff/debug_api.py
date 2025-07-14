import requests
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# CoreSignal API configuration
CORESIGNAL_API_KEY = "PTbCfCGO366FUpf10kPDGzeROW9zMIyR"
BASE_URL = "https://api.coresignal.com/cdapi/v2"
HEADERS = {
    "apikey": CORESIGNAL_API_KEY,
    "Content-Type": "application/json",
    "accept": "application/json"
}

def get_employee_details(employee_id: str) -> dict:
    """Get detailed employee information and examine the response structure."""
    url = f"{BASE_URL}/employee_multi_source/collect/{employee_id}"
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        logger.info(f"Response status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            logger.error(f"Error: {response.status_code} - {response.text}")
            return {}
    except Exception as e:
        logger.error(f"Request failed: {e}")
        return {}

def analyze_response_structure(data: dict, employee_id: str):
    """Analyze the API response structure to understand available fields."""
    print(f"\n=== API Response Analysis for Employee {employee_id} ===")
    print(f"Name: {data.get('full_name', 'N/A')}")
    print(f"Connections: {data.get('connections_count', 'N/A')}")
    
    # Check for key fields we're looking for
    target_fields = [
        'projected_total_salary_updated_at',
        'projected_base_salary_median',
        'active_experience_department',
        'active_experience_management_level',
        'is_decision_maker',
        'inferred_skills',
        'historical_skills',
        'total_experience_duration_months',
        'active_experience_company_id',
        'education',
        'languages',
        'activity'
    ]
    
    print("\nChecking for target fields:")
    for field in target_fields:
        if field in data:
            value = data[field]
            if isinstance(value, list):
                print(f"✅ {field}: LIST with {len(value)} items")
                if value and len(value) > 0:
                    print(f"   Sample: {value[0]}")
            elif isinstance(value, dict):
                print(f"✅ {field}: DICT with keys: {list(value.keys())[:5]}")
            elif value is not None:
                print(f"✅ {field}: {type(value).__name__} = {value}")
            else:
                print(f"❌ {field}: NULL")
        else:
            print(f"❌ {field}: NOT FOUND")
    
    # Check education structure
    if 'education' in data and data['education']:
        print(f"\nEducation structure (first item):")
        edu = data['education'][0]
        for key, value in edu.items():
            print(f"  {key}: {value}")
    
    # Check languages structure
    if 'languages' in data and data['languages']:
        print(f"\nLanguages structure (first item):")
        lang = data['languages'][0]
        for key, value in lang.items():
            print(f"  {key}: {value}")
    
    # Check activity structure
    if 'activity' in data and data['activity']:
        print(f"\nActivity structure (first item):")
        act = data['activity'][0]
        for key, value in act.items():
            print(f"  {key}: {value}")

def test_fuzzy_search(name: str, company: str = None):
    """Test fuzzy search with different strategies."""
    url = f"{BASE_URL}/employee_multi_source/search/es_dsl"
    
    print(f"\n=== Testing Fuzzy Search for: {name} at {company} ===")
    
    # Strategy 1: Exact name + company
    if company:
        dsl_query = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"full_name": name}},
                        {"match": {"experience.company_name": company}}
                    ]
                }
            }
        }
        print(f"Strategy 1: Exact match - {name} + {company}")
        print(f"  Query: {json.dumps(dsl_query, indent=2)}")
        response = requests.post(url, json=dsl_query, headers=HEADERS, timeout=30)
        print(f"  Status: {response.status_code}")
        if response.status_code == 200:
            results = response.json()
            print(f"  Results: {len(results)} found")
            if results:
                print(f"  First result ID: {results[0]}")
        else:
            print(f"  Error: {response.text}")
    
    # Strategy 2: Name only (this worked in the original script)
    dsl_query = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"full_name": name}}
                ]
            }
        }
    }
    print(f"\nStrategy 2: Name only - {name}")
    print(f"  Query: {json.dumps(dsl_query, indent=2)}")
    response = requests.post(url, json=dsl_query, headers=HEADERS, timeout=30)
    print(f"  Status: {response.status_code}")
    if response.status_code == 200:
        results = response.json()
        print(f"  Results: {len(results)} found")
        if results:
            print(f"  First 3 result IDs: {results[:3]}")
            # Get details of first result to see what we found
            if len(results) > 0:
                first_id = results[0]
                print(f"  Getting details for first result (ID: {first_id})...")
                details = get_employee_details(str(first_id))
                if details:
                    print(f"  Found: {details.get('full_name', 'N/A')} - Connections: {details.get('connections_count', 'N/A')}")
    else:
        print(f"  Error: {response.text}")
    
    # Strategy 3: Fuzzy name
    dsl_query = {
        "query": {
            "bool": {
                "must": [
                    {"fuzzy": {"full_name": {"value": name, "fuzziness": "AUTO"}}}
                ]
            }
        }
    }
    print(f"\nStrategy 3: Fuzzy name - {name}")
    print(f"  Query: {json.dumps(dsl_query, indent=2)}")
    response = requests.post(url, json=dsl_query, headers=HEADERS, timeout=30)
    print(f"  Status: {response.status_code}")
    if response.status_code == 200:
        results = response.json()
        print(f"  Results: {len(results)} found")
        if results:
            print(f"  First 3 result IDs: {results[:3]}")
    else:
        print(f"  Error: {response.text}")
    
    # Strategy 4: Wildcard first name
    first_name = name.split()[0]
    dsl_query = {
        "query": {
            "bool": {
                "must": [
                    {"wildcard": {"full_name": f"*{first_name}*"}}
                ]
            }
        }
    }
    print(f"\nStrategy 4: Wildcard first name - *{first_name}*")
    print(f"  Query: {json.dumps(dsl_query, indent=2)}")
    response = requests.post(url, json=dsl_query, headers=HEADERS, timeout=30)
    print(f"  Status: {response.status_code}")
    if response.status_code == 200:
        results = response.json()
        print(f"  Results: {len(results)} found")
        if results:
            print(f"  First 3 result IDs: {results[:3]}")
    else:
        print(f"  Error: {response.text}")
    
    # Strategy 5: Test with just first name
    dsl_query = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"full_name": first_name}}
                ]
            }
        }
    }
    print(f"\nStrategy 5: First name only - {first_name}")
    print(f"  Query: {json.dumps(dsl_query, indent=2)}")
    response = requests.post(url, json=dsl_query, headers=HEADERS, timeout=30)
    print(f"  Status: {response.status_code}")
    if response.status_code == 200:
        results = response.json()
        print(f"  Results: {len(results)} found")
        if results:
            print(f"  First 3 result IDs: {results[:3]}")
    else:
        print(f"  Error: {response.text}")

def test_known_working_search():
    """Test the search that we know worked in the original script."""
    url = f"{BASE_URL}/employee_multi_source/search/es_dsl"
    
    print(f"\n=== Testing Known Working Search ===")
    
    # This is the query that worked in the original script
    dsl_query = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"full_name": "Michele Dufresne"}}
                ]
            }
        }
    }
    
    print(f"Query: {json.dumps(dsl_query, indent=2)}")
    response = requests.post(url, json=dsl_query, headers=HEADERS, timeout=30)
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        results = response.json()
        print(f"Results: {len(results)} found")
        if results:
            print(f"First 5 result IDs: {results[:5]}")
            
            # Check what we actually found
            for i, result_id in enumerate(results[:3]):
                print(f"\nChecking result {i+1} (ID: {result_id})...")
                details = get_employee_details(str(result_id))
                if details:
                    print(f"  Name: {details.get('full_name', 'N/A')}")
                    print(f"  Connections: {details.get('connections_count', 'N/A')}")
                    print(f"  Location: {details.get('location_full', 'N/A')}")
    else:
        print(f"Error: {response.text}")

def main():
    """Main function to debug API response."""
    # Test the known working search first
    test_known_working_search()
    
    # Test with a few different employee IDs from the enriched data
    test_ids = [
        "222116895",  # Michele Zingariello
        "249969789",  # Haynes Bradley  
        "169085253",  # Papa Gupta
    ]
    
    print("\n" + "="*60)
    print("Testing API responses for different employees...")
    for employee_id in test_ids:
        print(f"\n{'='*60}")
        data = get_employee_details(employee_id)
        if data:
            analyze_response_structure(data, employee_id)
        else:
            print(f"No data received for employee {employee_id}")
    
    # Test fuzzy search with a few examples
    print(f"\n{'='*60}")
    print("Testing fuzzy search strategies...")
    test_fuzzy_search("Michele Dufresne", "Pioneer Valley Books")
    test_fuzzy_search("Bradley Margist", "CyberHoot")

if __name__ == "__main__":
    main() 