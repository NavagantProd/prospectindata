import os
import json
import httpx
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Dict, Optional
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from stuff.config import API_KEY, CACHE_TTL_DAYS, CACHE_DIR
import re

logger = logging.getLogger("core_sig.client")

ENDPOINTS = {
    "person_search": "/v1/people/search?email={email}",
    "person_profile": "/v1/people/{profile_id}",
    "person_skills": "/v1/people/{profile_id}/skills",
    "org_core": "/v1/organizations/{org_id}",
    "org_headcount": "/v1/organizations/{org_id}/headcount_monthly",
    "org_funding": "/v1/funding_rounds?org_id={org_id}&order=date_desc&limit=1",
    "org_tech": "/v1/organizations/{org_id}/technologies",
    "org_traffic": "/v1/organizations/{org_id}/web_traffic_monthly",
    "org_jobs": "/v1/organizations/{org_id}/job_postings?date_from={date_from}",
}

def _sanitize(s: str) -> str:
    """Replace all non-alphanumeric characters with underscores for safe filenames."""
    return re.sub(r'[^A-Za-z0-9_.-]', '_', s)

class CoreSignalClient:
    """
    Async client for CoreSignal API with caching and robust error handling.
    """
    def __init__(self, api_key: Optional[str] = None, cache_dir: str = CACHE_DIR, cache_ttl_days: int = CACHE_TTL_DAYS):
        self.api_key = api_key or API_KEY
        self.cache_dir = Path(cache_dir)
        self.cache_ttl = timedelta(days=cache_ttl_days)
        self.headers = {"Authorization": f"Token {self.api_key}"}
        self.cache_dir.mkdir(exist_ok=True)

    def _cache_path(self, endpoint: str, cache_key: str) -> Path:
        safe_ep = _sanitize(endpoint.strip("/").replace("/", "_"))
        safe_key = _sanitize(str(cache_key))
        p = self.cache_dir / safe_ep / f"{safe_key}.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        return p

    def _is_fresh(self, path: Path) -> bool:
        if not path.exists():
            return False
        mtime = datetime.fromtimestamp(path.stat().st_mtime)
        return (datetime.now() - mtime) < self.cache_ttl

    @retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=1, min=1, max=10), retry=retry_if_exception_type(httpx.RequestError))
    async def get(self, endpoint: str, cache_key: str) -> Any:
        cache_path = self._cache_path(endpoint, cache_key)
        if self._is_fresh(cache_path):
            try:
                with open(cache_path, "r", encoding="utf-8") as f:
                    logger.debug(f"Cache hit: {cache_path}")
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to read cache {cache_path}: {e}")
        url = f"https://api.coresignal.com{endpoint}"
        async with httpx.AsyncClient(timeout=30) as client:
            logger.info(f"Requesting {url}")
            resp = await client.get(url, headers=self.headers)
            resp.raise_for_status()
            data = resp.json()
            try:
                with open(cache_path, "w", encoding="utf-8") as f:
                    json.dump(data, f)
            except Exception as e:
                logger.warning(f"Failed to write cache {cache_path}: {e}")
            return data

    @retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=1, min=1, max=10), retry=retry_if_exception_type(httpx.RequestError))
    async def post(self, endpoint: str, json_body: dict, cache_key: str) -> Any:
        cache_path = self._cache_path(endpoint, cache_key)
        if self._is_fresh(cache_path):
            try:
                with open(cache_path, "r", encoding="utf-8") as f:
                    logger.debug(f"Cache hit: {cache_path}")
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to read cache {cache_path}: {e}")
        url = f"https://api.coresignal.com{endpoint}"
        async with httpx.AsyncClient(timeout=30) as client:
            logger.info(f"POSTing {url} with body {json_body}")
            resp = await client.post(url, headers=self.headers, json=json_body)
            logger.info(f"HTTP Response: {resp.status_code} {resp.text}")
            resp.raise_for_status()
            data = resp.json()
            try:
                with open(cache_path, "w", encoding="utf-8") as f:
                    json.dump(data, f)
            except Exception as e:
                logger.warning(f"Failed to write cache {cache_path}: {e}")
            return data

    # Endpoint wrappers
    async def person_by_email(self, email: str) -> Any:
        return await self.get(ENDPOINTS["person_search"].format(email=email), email)

    async def person_profile(self, profile_id: str) -> Any:
        return await self.get(ENDPOINTS["person_profile"].format(profile_id=profile_id), profile_id)

    async def person_skills(self, profile_id: str) -> Any:
        return await self.get(ENDPOINTS["person_skills"].format(profile_id=profile_id), f"skills_{profile_id}")

    async def org_core(self, org_id: str) -> Any:
        return await self.get(ENDPOINTS["org_core"].format(org_id=org_id), f"org_{org_id}")

    async def org_headcount(self, org_id: str) -> Any:
        return await self.get(ENDPOINTS["org_headcount"].format(org_id=org_id), f"head_{org_id}")

    async def org_funding(self, org_id: str) -> Any:
        return await self.get(ENDPOINTS["org_funding"].format(org_id=org_id), f"fund_{org_id}")

    async def org_tech(self, org_id: str) -> Any:
        return await self.get(ENDPOINTS["org_tech"].format(org_id=org_id), f"tech_{org_id}")

    async def org_traffic(self, org_id: str) -> Any:
        return await self.get(ENDPOINTS["org_traffic"].format(org_id=org_id), f"traffic_{org_id}")

    async def org_jobs(self, org_id: str, date_from: str) -> Any:
        return await self.get(ENDPOINTS["org_jobs"].format(org_id=org_id, date_from=date_from), f"jobs_{org_id}_{date_from}")

    async def person_search_v2(self, email: str) -> Any:
        # POST to /cdapi/v2/member/search/filter with {"email": email}
        endpoint = "/cdapi/v2/member/search/filter"
        body = {"email": email}
        return await self.post(endpoint, body, f"v2_{email}")

    def get_member_by_id(self, member_id: int) -> dict:
        """Fetch full member profile by ID from CoreSignal API."""
        url = f"https://api.coresignal.com/cdapi/v2/member/collect/{member_id}"
        headers = {
            "apikey": self.api_key,
            "Content-Type": "application/json"
        }
        try:
            response = self.session.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.error(f"Error fetching member by id {member_id}: {e}")
            return {}

    def collect_company(self, company_id: str) -> dict:
        """Synchronous wrapper for org_core for compatibility with enrichment logic."""
        import asyncio
        try:
            return asyncio.run(self.org_core(company_id))
        except Exception as e:
            logger.error(f"Error in collect_company: {e}")
            return {}

    def collect_person(self, person_id: str) -> dict:
        """Synchronous wrapper for get_member_by_id for compatibility with enrichment logic."""
        try:
            return self.get_member_by_id(int(person_id))
        except Exception as e:
            logger.error(f"Error in collect_person: {e}")
            return {} 