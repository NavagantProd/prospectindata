import os
import re
import json
import pandas as pd
import asyncio
import httpx
from typing import Any, Dict, List, Optional
from datetime import datetime
from tqdm.asyncio import tqdm
from stuff.config import API_KEY, CONCURRENCY
import logging

logger = logging.getLogger("enrichment")
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

API_ROOT = "https://api.coresignal.com"
HEADERS = {"Authorization": f"Token {API_KEY}"}

COLUMN_ALIASES = {
    'email': ['recipient_email', 'contact_email'],
    'first_name': ['contact_first_name', 'cs_employee_first_name'],
    'last_name': ['contact_last_name', 'cs_employee_last_name'],
    'company_name': ['contact_firm_name', 'firm_name', 'cs_company_name'],
    'coresignal_company_id': ['cs_company_id'],
    'company_url': ['cs_company_professional_network_url', 'firm_url', 'cs_company_website'],
    'employee_url': ['cs_employee_professional_network_url', 'contact_linked_in_url'],
}

def clean(val):
    if isinstance(val, str):
        v = val.strip()
        return v if v and v.lower() != 'nan' else None
    try:
        if pd.isna(val): return None
        return str(int(float(val)))
    except: return None

def clean_email(email):
    if not isinstance(email, str): return None
    email = email.strip().lower()
    if not email or email == 'nan' or '@' not in email: return None
    if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email): return None
    return email

def flatten(d, parent_key='', sep='__'):
    if isinstance(d, list):
        return {f'{parent_key}{sep}{i}': v for i, v in enumerate(d)}
    if isinstance(d, dict):
        items = {}
        for k, v in d.items():
            new_key = f'{parent_key}{sep}{k}' if parent_key else k
            items.update(flatten(v, new_key, sep=sep))
        return items
    return {parent_key: d}

def parse_shorthand(url):
    if not isinstance(url, str) or not url: return None
    for pat in [r'/company/([\w-]+)', r'/in/([\w-]+)', r'www\.professional-network\.com/([\w-]+)']:
        m = re.search(pat, url)
        if m: return m.group(1)
    return None

def es_dsl_payload(kind, **kwargs):
    if kind == 'company':
        name = kwargs['company_name']
        return {"query": {"bool": {"should": [
            {"match": {"company_name": {"query": name, "fuzziness": "AUTO"}}},
            {"match": {"company_name_alias": {"query": name, "fuzziness": "AUTO"}}},
            {"match": {"company_legal_name": {"query": name, "fuzziness": "AUTO"}}}
        ], "minimum_should_match": 1}}, "size": 3}
    if kind == 'employee':
        fn, ln, cn = kwargs['first_name'], kwargs['last_name'], kwargs['company_name']
        return {"query": {"bool": {"must": [
            {"bool": {"should": [
                {"match": {"full_name": {"query": f"{fn} {ln}", "fuzziness": "AUTO"}}},
                {"match": {"first_name": {"query": fn, "fuzziness": "AUTO"}}},
                {"match": {"last_name": {"query": ln, "fuzziness": "AUTO"}}}
            ], "minimum_should_match": 1}},
            {"match": {"company_name": {"query": cn, "fuzziness": "AUTO"}}}
        ]}}, "size": 3}
    if kind == 'employee_email':
        email = kwargs['email']
        return {"query": {"bool": {"should": [
            {"match": {"primary_professional_email": {"query": email, "fuzziness": "AUTO"}}},
            {"match": {"email": {"query": email, "fuzziness": "AUTO"}}}
        ], "minimum_should_match": 1}}, "size": 3}

def remap_columns(df):
    for std, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            if alias in df.columns:
                df[std] = df[alias]
                break
    return df

async def get_json(client, method, endpoint, **kwargs):
    url = f"{API_ROOT}{endpoint}"
    try:
        resp = await getattr(client, method)(url, headers=HEADERS, **kwargs)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning(f"API {method} {url} failed: {e}")
        return None

async def enrich_entity(client, kind, **kwargs):
    results = []
    if kind == 'company':
        cid = clean(kwargs.get('company_id'))
        cname = clean(kwargs.get('company_name'))
        curl = clean(kwargs.get('company_url'))
        if cid:
            data = await get_json(client, 'get', f"/v2/company_multi_source/collect/{cid}")
            if data: results.append(flatten(data)); return results[:3]
        shorthand = parse_shorthand(curl) if curl else None
        if shorthand:
            data = await get_json(client, 'get', f"/v2/company_multi_source/collect/{shorthand}")
            if data: results.append(flatten(data)); return results[:3]
        if cname:
            payload = es_dsl_payload('company', company_name=cname)
            data = await get_json(client, 'post', "/v2/company_multi_source/search/es_dsl", json=payload)
            if data and data.get('hits', {}).get('hits'):
                for hit in data['hits']['hits'][:3]:
                    results.append(flatten(hit.get('_source', {})))
                return results[:3]
    if kind == 'employee':
        email = clean_email(kwargs.get('email'))
        fname = clean(kwargs.get('first_name'))
        lname = clean(kwargs.get('last_name'))
        cname = clean(kwargs.get('company_name'))
        eurl = clean(kwargs.get('employee_url'))
        shorthand = parse_shorthand(eurl) if eurl else None
        if shorthand:
            data = await get_json(client, 'get', f"/v2/employee_multi_source/collect/{shorthand}")
            if data: results.append(flatten(data)); return results[:3]
        if fname and lname and cname:
            payload = es_dsl_payload('employee', first_name=fname, last_name=lname, company_name=cname)
            data = await get_json(client, 'post', "/v2/employee_multi_source/search/es_dsl", json=payload)
            if data and data.get('hits', {}).get('hits'):
                for hit in data['hits']['hits'][:3]:
                    results.append(flatten(hit.get('_source', {})))
                return results[:3]
        if email:
            payload = es_dsl_payload('employee_email', email=email)
            data = await get_json(client, 'post', "/v2/employee_multi_source/search/es_dsl", json=payload)
            if data and data.get('hits', {}).get('hits'):
                for hit in data['hits']['hits'][:3]:
                    results.append(flatten(hit.get('_source', {})))
                return results[:3]
    return results

async def enrich_leads_async(leads: pd.DataFrame) -> pd.DataFrame:
    async with httpx.AsyncClient(timeout=30) as client:
        tasks = []
        for _, row in leads.iterrows():
            tasks.append((row, enrich_entity(client, 'company', company_id=row.get('coresignal_company_id'), company_name=row.get('company_name'), company_url=row.get('company_url')), enrich_entity(client, 'employee', email=row.get('email'), first_name=row.get('first_name'), last_name=row.get('last_name'), company_name=row.get('company_name'), employee_url=row.get('employee_url'))))
        results = []
        for row, company_coro, employee_coro in tqdm(tasks, desc="Enriching", total=len(tasks)):
            company_data, employee_data = await asyncio.gather(company_coro, employee_coro)
            base = row.to_dict()
            for i, c in enumerate(company_data):
                for k, v in c.items():
                    base[f'company_{i+1}__{k}'] = v
            for i, e in enumerate(employee_data):
                for k, v in e.items():
                    base[f'employee_{i+1}__{k}'] = v
            results.append(base)
        return pd.DataFrame(results)

def enrich_leads(leads: pd.DataFrame) -> pd.DataFrame:
    leads = remap_columns(leads)
    return asyncio.run(enrich_leads_async(leads))

def diagnose(email=None, company_name=None, first_name=None, last_name=None, company_url=None, employee_url=None):
    async def diag():
        async with httpx.AsyncClient(timeout=30) as client:
            print(f"Diagnosing company: id=None, name={company_name}, url={company_url}")
            company = await enrich_entity(client, 'company', company_id=None, company_name=company_name, company_url=company_url)
            print(json.dumps(company, indent=2))
            print(f"Diagnosing employee: email={email}, name={first_name} {last_name}, company={company_name}, url={employee_url}")
            employee = await enrich_entity(client, 'employee', email=email, first_name=first_name, last_name=last_name, company_name=company_name, employee_url=employee_url)
            print(json.dumps(employee, indent=2))
    asyncio.run(diag()) 