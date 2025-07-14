# enhanced_enrichment.py
import pandas as pd
import numpy as np
import re
from urllib.parse import urlparse
from typing import Any, Dict, List, Optional
import logging

logger = logging.getLogger("enrichment")

class DataCleaner:
    """Utility class for cleaning and validating input data."""
    
    @staticmethod
    def clean_email(email: Any) -> Optional[str]:
        if pd.isna(email) or not isinstance(email, str):
            return None
        email = email.strip().lower()
        if email in ("", "nan", "null"):
            return None
        pattern = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
        return email if re.match(pattern, email) else None
    
    @staticmethod
    def clean_name(name: Any) -> Optional[str]:
        if pd.isna(name) or not isinstance(name, str):
            return None
        name = name.strip().title()
        if name.lower() in ("", "nan", "null"):
            return None
        name = re.sub(r"[^A-Za-z\s'-]", "", name)
        return name if len(name) >= 2 else None
    
    @staticmethod
    def clean_company_name(company: Any) -> Optional[str]:
        if pd.isna(company) or not isinstance(company, str):
            return None
        company = company.strip()
        if company.lower() in ("", "nan", "null"):
            return None
        for suffix in ("Inc.", "Inc", "LLC", "Ltd.", "Ltd", "Corp.", "Corp", "Co.", "Co"):
            if company.endswith(f" {suffix}"):
                company = company[: -len(suffix) - 1].strip()
        return company
    
    @staticmethod
    def extract_domain_from_email(email: str) -> Optional[str]:
        try:
            return email.split("@", 1)[1].lower()
        except:
            return None
    
    @staticmethod
    def extract_domain_from_url(url: Any) -> Optional[str]:
        if pd.isna(url) or not isinstance(url, str):
            return None
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        try:
            netloc = urlparse(url).netloc.lower()
            return netloc[4:] if netloc.startswith("www.") else netloc
        except:
            return None

class EnrichmentEngine:
    """Main enrichment engine that coordinates data enrichment."""
    
    def __init__(self, client: EnhancedCoreSignalClient):
        self.client = client
        self.cleaner = DataCleaner()
        self.stats = {
            "processed": 0,
            "person_matches": 0,
            "company_matches": 0,
            "api_errors": 0
        }
    
    async def enrich_person(self, row: pd.Series) -> Dict[str, Any]:
        result = {"person_enriched": False, "person_data": {}, "person_match_method": None}
        email = self.cleaner.clean_email(row.get("email"))
        if not email:
            return result
        try:
            people = await self.client.search_person_by_email(email)
            if people:
                data = people[0]
                result.update({
                    "person_enriched": True,
                    "person_data": self._flatten_person_data(data),
                    "person_match_method": "email"
                })
                self.stats["person_matches"] += 1
                logger.info(f"Person found for email: {email}")
        except Exception as e:
            logger.error(f"Error enriching person {email}: {e}")
            self.stats["api_errors"] += 1
        return result
    
    async def enrich_company(self, row: pd.Series) -> Dict[str, Any]:
        result = {"company_enriched": False, "company_data": {}, "company_match_method": None}
        company_name = self.cleaner.clean_company_name(row.get("company_name"))
        email = self.cleaner.clean_email(row.get("email"))
        try:
            if email:
                domain = self.cleaner.extract_domain_from_email(email)
                companies = await self.client.search_company_by_domain(domain)
                if companies:
                    data = companies[0]
                    result.update({
                        "company_enriched": True,
                        "company_data": self._flatten_company_data(data),
                        "company_match_method": "domain"
                    })
                    self.stats["company_matches"] += 1
                    logger.info(f"Company found by domain: {domain}")
                    return result
            if company_name:
                companies = await self.client.search_company_by_name(company_name)
                if companies:
                    data = companies[0]
                    result.update({
                        "company_enriched": True,
                        "company_data": self._flatten_company_data(data),
                        "company_match_method": "name"
                    })
                    self.stats["company_matches"] += 1
                    logger.info(f"Company found by name: {company_name}")
        except Exception as e:
            logger.error(f"Error enriching company {company_name}: {e}")
            self.stats["api_errors"] += 1
        return result
    
    def _flatten_person_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        mapping = {
            "id": "person_id",
            "name": "person_name",
            "first_name": "person_first_name",
            "last_name": "person_last_name",
            "title": "person_title",
            "email": "person_email",
            "linkedin_url": "person_linkedin_url",
            "location": "person_location",
            "company": "person_company"
        }
        return {out: data[inp] for inp, out in mapping.items() if inp in data}
    
    def _flatten_company_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        mapping = {
            "id": "company_id",
            "name": "company_name_enriched",
            "website": "company_website",
            "industry": "company_industry",
            "size": "company_size",
            "founded": "company_founded",
            "location": "company_location",
            "description": "company_description",
            "linkedin_url": "company_linkedin_url"
        }
        return {out: data[inp] for inp, out in mapping.items() if inp in data}
    
    async def enrich_dataframe(self, df: pd.DataFrame, max_concurrent: int = 5) -> pd.DataFrame:
        if df.empty:
            return df
        logger.info(f"Starting enrichment of {len(df)} records")
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def worker(row):
            async with semaphore:
                self.stats["processed"] += 1
                p = await self.enrich_person(row)
                c = await self.enrich_company(row)
                enriched = row.to_dict()
                enriched.update(p["person_data"])
                enriched.update(c["company_data"])
                enriched["person_enriched"] = p["person_enriched"]
                enriched["company_enriched"] = c["company_enriched"]
                enriched["person_match_method"] = p["person_match_method"]
                enriched["company_match_method"] = c["company_match_method"]
                if self.stats["processed"] % 10 == 0:
                    logger.info(f"Processed {self.stats['processed']}/{len(df)}")
                return enriched
        
        tasks = [worker(row) for _, row in df.iterrows()]
        results = [await t for t in asyncio.as_completed(tasks)]
        enriched_df = pd.DataFrame(results)
        logger.info(f"Enrichment complete: {self.stats}")
        return enriched_df
