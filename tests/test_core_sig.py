import pytest
import pandas as pd
import httpx
import asyncio
from core_sig import enrich_leads
from unittest.mock import patch

@pytest.mark.asyncio
async def test_enrich_leads_happy(monkeypatch):
    # Prepare fake API responses
    async def fake_cached_get(client, endpoint, cache_key):
        if endpoint.startswith('/v1/people/search'):
            return {"results": [{"profile_id": "p1", "org_id": "c1"}]}
        if endpoint.startswith('/v1/people/p1'):
            return {
                "first_name": "Jane", "last_name": "Doe", "location": {"city": "SF", "region": "CA", "country": "US"},
                "headline": "CTO", "connections_count": 500, "stats": {"posts_30d": 2, "reactions_30d": 3},
                "experience": [{"title": "CTO", "org_id": "c1"}], "last_updated": "2024-06-01T00:00:00"
            }
        if endpoint.startswith('/v1/people/p1/skills'):
            return {"skills": [{"name": "Python", "endorsements": 10}, {"name": "ML", "endorsements": 5}]}
        if endpoint.startswith('/v1/organizations/c1'):
            return {"name": "Acme", "website": "acme.com", "industry": "Tech", "last_updated": "2024-06-01T00:00:00"}
        if endpoint.startswith('/v1/organizations/c1/headcount_monthly'):
            return {"data": [{"date": "2024-06-01", "value": 100}, {"date": "2024-05-01", "value": 90}, {"date": "2024-04-01", "value": 80}]}
        if endpoint.startswith('/v1/funding_rounds'):
            return {"results": [{"type": "seed", "amount_usd": 1000000, "announced_date": "2024-05-01"}]}
        if endpoint.startswith('/v1/organizations/c1/job_postings'):
            return {"results": [{}, {}]}
        if endpoint.startswith('/v1/organizations/c1/technologies'):
            return {"technologies": [{"name": "Python"}, {"name": "AWS"}]}
        if endpoint.startswith('/v1/organizations/c1/web_traffic_monthly'):
            return {"data": [{"date": "2024-06-01", "visits": 1000}, {"date": "2024-05-01", "visits": 900}, {"date": "2024-04-01", "visits": 800}]}
        return {}
    # Patch cached_get in core_sig
    with patch('core_sig.cached_get', new=fake_cached_get):
        df = enrich_leads(pd.DataFrame([{
            'email': 'jane@acme.com',
            'first_name': 'Jane',
            'last_name': 'Doe',
            'company_name': 'Acme',
            'coresignal_company_id': ''
        }]))
        row = df.iloc[0]
        assert row['person_id'] == 'p1'
        assert row['company_id'] == 'c1'
        assert row['first_name'] == 'Jane'
        assert row['company_name'] == 'Acme'
        assert row['skills_top5'] == 'Python, ML'
        assert row['open_jobs_30d'] == 2
        assert row['tech_tags'] == 'AWS, Python'
        assert row['traffic_monthly_visits'] == 1000 