# enhanced_client.py
import asyncio
import json
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiofiles
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import hashlib

from config import API_BASE_URL, API_KEY

logger = logging.getLogger("coresignal_client")


class RateLimiter:
    """Async rate limiter to control API request frequency."""
    
    def __init__(self, max_requests: int = 5, time_window: float = 1.0):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests: List[float] = []
        self._lock = asyncio.Lock()
    
    async def acquire(self):
        async with self._lock:
            now = time.time()
            # Remove old requests outside the time window
            self.requests = [t for t in self.requests if now - t < self.time_window]
            if len(self.requests) >= self.max_requests:
                sleep_time = self.time_window - (now - self.requests[0])
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                    return await self.acquire()
            self.requests.append(now)


class EnhancedCoreSignalClient:
    """Enhanced CoreSignal API client with improved error handling and caching."""
    
    ENDPOINTS = {
        "person_search":       "/v1/people/search",
        "person_profile":      "/v1/people/{person_id}",
        "person_by_email":     "/v1/people/search?email={email}",
        "person_by_url":       "/v1/people/search?url={url}",
        "person_by_shorthand": "/v1/people/search?shorthand_name={shorthand}",
        "company_search":      "/v1/organizations/search",
        "company_profile":     "/v1/organizations/{company_id}",
        "company_by_domain":   "/v1/organizations/search?domain={domain}",
        "v2_person_search":    "/v2/person/search",
        "v2_company_search":   "/v2/company/search",
    }
    
    def __init__(self,
                 api_key: str = API_KEY,
                 cache_dir: Path = Path("cache"),
                 cache_ttl_days: int = 7,
                 max_concurrent: int = 5):
        self.api_key = api_key
        self.cache_dir = cache_dir
        self.cache_ttl = timedelta(days=cache_ttl_days)
        self.rate_limiter = RateLimiter(max_requests=max_concurrent, time_window=1.0)
        self.cache_dir.mkdir(exist_ok=True, parents=True)
        self.client_config = {
            "timeout": httpx.Timeout(30.0),
            "limits": httpx.Limits(max_connections=max_concurrent),
            "headers": {"Authorization": f"Bearer {self.api_key}"}
        }
    
    def _get_cache_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        key_data = f"{endpoint}:{json.dumps(params, sort_keys=True)}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def _get_cache_path(self, cache_key: str) -> Path:
        return self.cache_dir / f"{cache_key}.json"
    
    async def _is_cache_valid(self, cache_path: Path) -> bool:
        if not cache_path.exists():
            return False
        try:
            stat = cache_path.stat()
            cache_time = datetime.fromtimestamp(stat.st_mtime)
            return (datetime.now() - cache_time) < self.cache_ttl
        except Exception as e:
            logger.warning(f"Error checking cache validity: {e}")
            return False
    
    async def _load_from_cache(self, cache_path: Path) -> Optional[Dict[str, Any]]:
        try:
            async with aiofiles.open(cache_path, 'r', encoding='utf-8') as f:
                return json.loads(await f.read())
        except Exception as e:
            logger.warning(f"Error loading cache {cache_path}: {e}")
            return None
    
    async def _save_to_cache(self, cache_path: Path, data: Dict[str, Any]):
        try:
            cache_path.parent.mkdir(exist_ok=True, parents=True)
            async with aiofiles.open(cache_path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(data, indent=2))
        except Exception as e:
            logger.warning(f"Error saving cache {cache_path}: {e}")
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError))
    )
    async def _make_request(self, method: str, endpoint: str, **kwargs) -> Optional[Dict[str, Any]]:
        await self.rate_limiter.acquire()
        url = f"{API_BASE_URL}{endpoint}"
        async with httpx.AsyncClient(**self.client_config) as client:
            try:
                logger.info(f"{method.upper()} {url}")
                response = await getattr(client, method.lower())(url, **kwargs)
                if response.status_code == 404:
                    logger.warning(f"Endpoint not found: {url}")
                    return None
                if response.status_code == 429:
                    logger.warning("Rate limit exceeded, waiting...")
                    await asyncio.sleep(60)
                    raise httpx.HTTPStatusError("Rate limited", request=response.request, response=response)
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as e:
                code = e.response.status_code
                if code in (401, 403):
                    logger.error(f"Authentication error: {e}")
                    return None
                if code == 404:
                    logger.warning(f"Resource not found: {url}")
                    return None
                logger.error(f"HTTP error {code}: {e}")
                raise
            except Exception as e:
                logger.error(f"Request failed: {e}")
                raise
    
    async def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        params = params or {}
        cache_key = self._get_cache_key(endpoint, params)
        cache_path = self._get_cache_path(cache_key)
        if await self._is_cache_valid(cache_path):
            data = await self._load_from_cache(cache_path)
            if data:
                logger.debug(f"Cache hit: {cache_key}")
                return data
        data = await self._make_request("get", endpoint, params=params)
        if data:
            await self._save_to_cache(cache_path, data)
        return data
    
    async def post(self, endpoint: str, json_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        cache_key = self._get_cache_key(endpoint, json_data)
        cache_path = self._get_cache_path(cache_key)
        if await self._is_cache_valid(cache_path):
            data = await self._load_from_cache(cache_path)
            if data:
                logger.debug(f"Cache hit: {cache_key}")
                return data
        data = await self._make_request("post", endpoint, json=json_data)
        if data:
            await self._save_to_cache(cache_path, data)
        return data
    
    # High-level methods...
    async def search_person_by_email(self, email: str) -> List[Dict[str, Any]]:
        endpoint = self.ENDPOINTS["person_by_email"].format(email=email)
        result = await self.get(endpoint)
        if not result:
            result = await self.post(self.ENDPOINTS["v2_person_search"], {"email": email})
        return result.get("results", []) if result else []
    
    async def search_person_by_linkedin_url(self, linkedin_url: str) -> List[Dict[str, Any]]:
        endpoint = self.ENDPOINTS["person_by_url"].format(url=linkedin_url)
        result = await self.get(endpoint)
        return result.get("results", []) if result else []
    
    async def search_person_by_shorthand(self, shorthand: str) -> List[Dict[str, Any]]:
        endpoint = self.ENDPOINTS["person_by_shorthand"].format(shorthand=shorthand)
        result = await self.get(endpoint)
        return result.get("results", []) if result else []
    
    async def search_company_by_name(self, company_name: str) -> List[Dict[str, Any]]:
        result = await self.get(self.ENDPOINTS["company_search"], {"name": company_name})
        if not result:
            result = await self.post(self.ENDPOINTS["v2_company_search"], {"name": company_name})
        return result.get("results", []) if result else []
    
    async def search_company_by_domain(self, domain: str) -> List[Dict[str, Any]]:
        endpoint = self.ENDPOINTS["company_by_domain"].format(domain=domain)
        result = await self.get(endpoint)
        return result.get("results", []) if result else []
