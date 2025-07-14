"""
CoreSignal v2 API Client - Fixed Implementation
"""
import requests
import json
import time
import logging
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass
import tldextract
from config import (
    CORESIGNAL_BASE_URL, CORESIGNAL_API_KEY, ENDPOINTS,
    RATE_LIMIT_DELAY, MAX_RETRIES, REQUEST_TIMEOUT
)

logger = logging.getLogger(__name__)

@dataclass
class APIStats:
    """Track API usage statistics"""
    total_calls: int = 0
    company_searches: int = 0
    company_collects: int = 0
    member_searches: int = 0
    member_collects: int = 0
    cache_hits: int = 0
    errors: int = 0

class CoreSignalClient:
    """CoreSignal v2 API Client with proper error handling and caching"""
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or CORESIGNAL_API_KEY
        if not self.api_key:
            raise ValueError("CoreSignal API key is required")
            
        self.base_url = CORESIGNAL_BASE_URL
        self.session = requests.Session()
        self.session.headers.update({
            "apikey": self.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json"
        })
        
        self.stats = APIStats()
        self.company_cache: Dict[str, Any] = {}
        self.member_cache: Dict[str, Any] = {}
        self.last_request_time = 0
        
        logger.info("CoreSignal client initialized")

    def _rate_limit(self):
        """Implement rate limiting between requests"""
        elapsed = time.time() - self.last_request_time
        if elapsed < RATE_LIMIT_DELAY:
            time.sleep(RATE_LIMIT_DELAY - elapsed)
        self.last_request_time = time.time()

    def _make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        url = f"{self.base_url}{endpoint}"
        payload = kwargs.get('json', None)
        
        for attempt in range(MAX_RETRIES):
            try:
                self._rate_limit()
                
                if 'timeout' not in kwargs:
                    kwargs['timeout'] = REQUEST_TIMEOUT
                
                logger.debug(f"API Request: {method} {url}")
                if 'json' in kwargs:
                    logger.debug(f"Payload: {json.dumps(kwargs['json'], indent=2)}")
                
                response = self.session.request(method, url, **kwargs)
                self.stats.total_calls += 1
                
                logger.debug(f"Response: {response.status_code}")
                
                # Store curl and response for every request
                # Remove the _log_and_store_curl function and all calls to it. Instead, just log the curl command and response to the logger for debugging, but do not write any files.
                # In each API call (search and collect), replace file-writing with logger.debug or logger.info as appropriate.
                # Example: logger.info(f"[CURL DEBUG] {curl_cmd}") and logger.info(f"[CURL DEBUG] response: {response.status_code} {response.text}")

                if response.status_code == 200:
                    return response
                elif response.status_code == 422:
                    logger.error(f"API validation error: {response.text}")
                    break
                elif response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limited, waiting {retry_after}s")
                    time.sleep(retry_after)
                    continue
                elif response.status_code == 401:
                    logger.error("Authentication failed - check API key")
                    break
                else:
                    logger.warning(f"HTTP {response.status_code}: {response.text}")
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(2 ** attempt)
                        continue
                    break
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)
                    continue
                break
        
        self.stats.errors += 1
        return requests.Response()

    def search_companies(self, name: str = "", website: str = "") -> List[Dict]:
        """Search for companies by name and/or website. Only send allowed fields and log payload/response."""
        if not name and not website:
            return []
        cache_key = f"{name}|{website}"
        if cache_key in self.company_cache:
            self.stats.cache_hits += 1
            return self.company_cache[cache_key]
        payload = {}
        if name:
            payload["name"] = name.strip()
        if website:
            payload["website"] = website.strip()
        logger.info(f"[API DEBUG] company_search payload: {json.dumps(payload)}")
        response = self._make_request("POST", ENDPOINTS["company_search"], json=payload)
        self.stats.company_searches += 1
        logger.info(f"[API DEBUG] company_search response: {response.status_code} {response.text[:500]}")
        results = []
        if response.status_code == 200:
            try:
                data = response.json()
                raw_results = []
                if isinstance(data, list):
                    raw_results = data[:10]
                elif isinstance(data, dict) and 'hits' in data:
                    raw_results = data['hits'][:10]
                for entry in raw_results:
                    if isinstance(entry, int):
                        detailed = self.get_company_details(entry)
                        if isinstance(detailed, dict):
                            results.append(detailed)
                        else:
                            logger.warning(f"Company ID {entry} could not be resolved to a dict.")
                    elif isinstance(entry, dict):
                        results.append(entry)
                    else:
                        logger.warning(f"Company search result is not a dict or int: {entry}")
                self.company_cache[cache_key] = results
                logger.debug(f"Found {len(results)} companies for search")
                return results
            except Exception as e:
                logger.error(f"Failed to parse company search response: {e}")
        return []

    def search_company(self, name: str = "", website: str = "") -> Optional[Dict]:
        """Return the best company match (first result) for compatibility with enrichment logic."""
        results = self.search_companies(name, website)
        if results:
            logger.info(f"[DEBUG] search_company found {len(results)} results, returning first.")
            return results[0]
        logger.info("[DEBUG] search_company found no results.")
        return None

    def get_company_details(self, company_id: Union[str, int]) -> Optional[Dict]:
        """Get detailed company information by ID, with debug logging and save raw JSON to file."""
        if not company_id:
            return None
        company_id = str(company_id)
        if company_id in self.company_cache:
            self.stats.cache_hits += 1
            return self.company_cache[company_id]
        endpoint = f"{ENDPOINTS['company_collect']}/{company_id}"
        logger.info(f"[API DEBUG] company_collect endpoint: {endpoint}")
        response = self._make_request("GET", endpoint)
        self.stats.company_collects += 1
        logger.info(f"[API DEBUG] company_collect response: {response.status_code} {response.text[:500]}")
        if response.status_code == 200:
            try:
                data = response.json()
                # Save raw JSON for debugging
                with open(f"company_{company_id}.json", "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)
                self.company_cache[company_id] = data
                logger.debug(f"Retrieved company details for ID {company_id}")
                return data
            except Exception as e:
                logger.error(f"Failed to parse company details: {e}")
        return None

    def collect_company(self, company_id: str) -> Optional[Dict]:
        """Alias for get_company_details for compatibility with enrichment logic."""
        return self.get_company_details(company_id)

    def search_members(self, name: str = "", experience_company_name: str = "", title: str = "", location: str = "") -> List[Dict]:
        """Search for members/employees with company context. Only send allowed fields and log payload/response."""
        payload = {}
        if name:
            payload["name"] = name.strip()
        if experience_company_name:
            payload["experience_company_name"] = experience_company_name.strip()
        # Only send allowed fields: name, experience_company_name
        cache_key = json.dumps(payload, sort_keys=True)
        if not payload:
            return []
        if cache_key in self.member_cache:
            self.stats.cache_hits += 1
            return self.member_cache[cache_key]
        logger.info(f"[API DEBUG] member_search payload: {json.dumps(payload)}")
        response = self._make_request("POST", ENDPOINTS["member_search"], json=payload)
        self.stats.member_searches += 1
        logger.info(f"[API DEBUG] member_search response: {response.status_code} {response.text[:500]}")
        results = []
        if response.status_code == 200:
            try:
                data = response.json()
                raw_results = []
                if isinstance(data, list):
                    raw_results = data[:10]
                elif isinstance(data, dict) and 'hits' in data:
                    raw_results = data['hits'][:10]
                for entry in raw_results:
                    if isinstance(entry, int):
                        detailed = self.get_member_details(entry)
                        if isinstance(detailed, dict):
                            results.append(detailed)
                        else:
                            logger.warning(f"Member ID {entry} could not be resolved to a dict.")
                    elif isinstance(entry, dict):
                        results.append(entry)
                    else:
                        logger.warning(f"Member search result is not a dict or int: {entry}")
                self.member_cache[cache_key] = results
                logger.debug(f"API member search response: {results}")
                return results
            except Exception as e:
                logger.error(f"Failed to parse member search response: {e}")
        return []

    def search_person(self, name: str = "", experience_company_name: str = "") -> Optional[Dict]:
        """Return the best person match (first result) for compatibility with enrichment logic."""
        results = self.search_members(name, experience_company_name)
        if results:
            logger.info(f"[DEBUG] search_person found {len(results)} results, returning first.")
            return results[0]
        logger.info("[DEBUG] search_person found no results.")
        return None

    def get_member_details(self, member_id: Union[str, int]) -> Optional[Dict]:
        """Get detailed member information by ID, with debug logging and save raw JSON to file."""
        if not member_id:
            return None
        member_id = str(member_id)
        if member_id in self.member_cache:
            self.stats.cache_hits += 1
            return self.member_cache[member_id]
        endpoint = f"{ENDPOINTS['member_collect']}/{member_id}"
        logger.info(f"[API DEBUG] member_collect endpoint: {endpoint}")
        response = self._make_request("GET", endpoint)
        self.stats.member_collects += 1
        logger.info(f"[API DEBUG] member_collect response: {response.status_code} {response.text[:500]}")
        if response.status_code == 200:
            try:
                data = response.json()
                # Save raw JSON for debugging
                with open(f"member_{member_id}.json", "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)
                self.member_cache[member_id] = data
                logger.debug(f"Retrieved member details for ID {member_id}")
                return data
            except Exception as e:
                logger.error(f"Failed to parse member details: {e}")
        return None

    def collect_person(self, person_id: str) -> Optional[Dict]:
        """Alias for get_member_details for compatibility with enrichment logic."""
        return self.get_member_details(person_id)

    def get_stats(self) -> Dict[str, Any]:
        """Get API usage statistics"""
        return {
            "total_api_calls": self.stats.total_calls,
            "company_searches": self.stats.company_searches,
            "company_collects": self.stats.company_collects,
            "member_searches": self.stats.member_searches,
            "member_collects": self.stats.member_collects,
            "cache_hits": self.stats.cache_hits,
            "errors": self.stats.errors,
            "cache_sizes": {
                "companies": len(self.company_cache),
                "members": len(self.member_cache)
            }
        }