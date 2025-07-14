import os
import json
import httpx
import pandas as pd
from typing import Any, Dict, Optional, List
from datetime import datetime, timedelta
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import asyncio
from tqdm.asyncio import tqdm

CORESIGNAL_API_KEY = os.getenv("CORESIGNAL_API_KEY")
CACHE_ROOT = Path(".cache")
CACHE_ROOT.mkdir(exist_ok=True)

HEADERS = {"Authorization": f"Token {CORESIGNAL_API_KEY}"}

CACHE_TTL = timedelta(days=7)

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

@retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=1, min=1, max=10), retry=retry_if_exception_type(httpx.RequestError))
async def cached_get(client: httpx.AsyncClient, endpoint: str, cache_key: str) -> Any:
    cache_path = CACHE_ROOT / endpoint.strip("/").replace("/", "_") / f"{cache_key}.json"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    if cache_path.exists():
        mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
        if datetime.now() - mtime < CACHE_TTL:
            with open(cache_path, "r", encoding="utf-8") as f:
                return json.load(f)
    url = f"https://api.coresignal.com{endpoint}"
    resp = await client.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(data, f)
    return data

def derive_seniority(title: str) -> str:
    t = title.lower() if title else ""
    if any(x in t for x in ["chief", "ceo", "cfo", "cto", "coo", "president", "founder", "owner"]):
        return "c_level"
    if any(x in t for x in ["vp", "vice president"]):
        return "vp"
    if any(x in t for x in ["director"]):
        return "director"
    if any(x in t for x in ["manager"]):
        return "manager"
    if any(x in t for x in ["lead", "head"]):
        return "lead"
    if any(x in t for x in ["intern"]):
        return "intern"
    return "employee"

def derive_department(title: str) -> str:
    t = title.lower() if title else ""
    if any(x in t for x in ["engineering", "developer", "software", "cto"]):
        return "engineering"
    if any(x in t for x in ["product", "pm", "product manager"]):
        return "product"
    if any(x in t for x in ["it", "information technology"]):
        return "it"
    if any(x in t for x in ["marketing", "growth"]):
        return "marketing"
    if any(x in t for x in ["sales", "account executive"]):
        return "sales"
    if any(x in t for x in ["finance", "cfo"]):
        return "finance"
    if any(x in t for x in ["hr", "human resources"]):
        return "hr"
    return "other"

def derive_persona_type(seniority: str, department: str) -> str:
    if seniority in {"owner", "c_level", "vp", "founder"}:
        return "economic_buyer"
    if department in {"engineering", "product", "it"}:
        return "technical_buyer"
    return "champion"

def staleness_flag(*dates: Optional[str]) -> bool:
    now = datetime.now()
    for d in dates:
        if not d:
            continue
        try:
            dt = datetime.fromisoformat(d[:19])
            if (now - dt).days > 30:
                return True
        except Exception:
            continue
    return False

def top5_skills(skills: List[Dict]) -> str:
    if not skills:
        return ""
    sorted_skills = sorted(skills, key=lambda x: x.get("endorsements", 0), reverse=True)
    return ", ".join(
        x["name"] if isinstance(x, dict) and x.get("name") else json.dumps(x, ensure_ascii=False) if isinstance(x, dict) else str(x)
        for x in sorted_skills[:5] if (isinstance(x, dict) and x.get("name")) or x)

def pct_delta(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None or b == 0:
        return None
    return round(100 * (a - b) / b, 2)

async def enrich_one(lead: pd.Series, client: httpx.AsyncClient) -> Dict[str, Any]:
    out = {k: lead.get(k, "") for k in lead.index}
    email = lead.get("email", "")
    profile_id = None
    org_id = None
    # 1. Person by email
    if email:
        try:
            person = await cached_get(client, ENDPOINTS["person_search"].format(email=email), email)
            if person and person.get("results"):
                profile_id = person["results"][0]["profile_id"]
                org_id = person["results"][0].get("org_id")
        except Exception:
            pass
    # 2. Fallback: use provided or fuzzy
    if not profile_id:
        out["error"] = "not_found"
        return out
    out["person_id"] = profile_id
    # 3. Person profile
    person_profile = await cached_get(client, ENDPOINTS["person_profile"].format(profile_id=profile_id), profile_id)
    out["first_name"] = person_profile.get("first_name", "")
    out["last_name"] = person_profile.get("last_name", "")
    out["location_city"] = person_profile.get("location", {}).get("city", "")
    out["location_region"] = person_profile.get("location", {}).get("region", "")
    out["location_country"] = person_profile.get("location", {}).get("country", "")
    out["headline"] = person_profile.get("headline", "")
    out["linkedin_connections"] = person_profile.get("connections_count", "")
    stats = person_profile.get("stats", {})
    out["activity_score_30d"] = stats.get("posts_30d", 0) * 0.4 + stats.get("reactions_30d", 0) * 0.6
    # 4. Skills
    skills = await cached_get(client, ENDPOINTS["person_skills"].format(profile_id=profile_id), f"skills_{profile_id}")
    out["skills_top5"] = top5_skills(skills.get("skills", []))
    # 5. Current title/role
    exp = person_profile.get("experience", [])
    current_exp = next((e for e in exp if not e.get("date_to")), exp[0] if exp else {})
    title = current_exp.get("title", "")
    out["seniority"] = derive_seniority(title)
    out["department"] = derive_department(title)
    out["persona_type"] = derive_persona_type(out["seniority"], out["department"])
    # 6. Company ID
    company_id = lead.get("coresignal_company_id") or org_id or current_exp.get("org_id")
    if not company_id:
        out["error"] = "no_company_id"
        return out
    out["company_id"] = company_id
    # 7. Company core
    org = await cached_get(client, ENDPOINTS["org_core"].format(org_id=company_id), f"org_{company_id}")
    out["company_name"] = org.get("name", "")
    out["website"] = org.get("website", "")
    out["industry"] = org.get("industry", "")
    # 8. Headcount
    head = await cached_get(client, ENDPOINTS["org_headcount"].format(org_id=company_id), f"head_{company_id}")
    months = sorted(head.get("data", []), key=lambda x: x["date"], reverse=True)
    out["headcount_current"] = months[0]["value"] if months else None
    out["headcount_3mo_delta_pct"] = pct_delta(months[0]["value"], months[2]["value"]) if len(months) > 2 else None
    # 9. Funding
    fund = await cached_get(client, ENDPOINTS["org_funding"].format(org_id=company_id), f"fund_{company_id}")
    if fund.get("results"):
        f = fund["results"][0]
        out["funding_round_type"] = f.get("type", "")
        out["funding_amount_usd"] = f.get("amount_usd", "")
        out["funding_announced_date"] = f.get("announced_date", "")
    # 10. Jobs
    date_from = (datetime.now() - timedelta(days=30)).date().isoformat()
    jobs = await cached_get(client, ENDPOINTS["org_jobs"].format(org_id=company_id, date_from=date_from), f"jobs_{company_id}_{date_from}")
    out["open_jobs_30d"] = len(jobs.get("results", []))
    # 11. Tech
    tech = await cached_get(client, ENDPOINTS["org_tech"].format(org_id=company_id), f"tech_{company_id}")
    out["tech_tags"] = ", ".join(
        t.get("name", "") if isinstance(t, dict) and t.get("name") else json.dumps(t, ensure_ascii=False) if isinstance(t, dict) else str(t)
        for t in tech.get("technologies", []) if (isinstance(t, dict) and t.get("name")) or t)
    # 12. Traffic
    traffic = await cached_get(client, ENDPOINTS["org_traffic"].format(org_id=company_id), f"traffic_{company_id}")
    visits = sorted(traffic.get("data", []), key=lambda x: x["date"], reverse=True)
    out["traffic_monthly_visits"] = visits[0]["visits"] if visits else None
    out["traffic_3mo_delta_pct"] = pct_delta(visits[0]["visits"], visits[2]["visits"]) if len(visits) > 2 else None
    # 13. Staleness
    out["staleness_flag"] = staleness_flag(
        person_profile.get("last_updated"),
        org.get("last_updated"),
        months[0]["date"] if months else None,
        visits[0]["date"] if visits else None
    )
    # 14. Add all extra fields from person_profile for modeling
    for k, v in person_profile.items():
        if k not in out:
            out[f"person_{k}"] = v
    return out

async def enrich_all(leads: pd.DataFrame) -> List[Dict[str, Any]]:
    async with httpx.AsyncClient(timeout=30) as client:
        tasks = [enrich_one(row, client) for _, row in leads.iterrows()]
        results = []
        for f in tqdm.as_completed(tasks, total=len(tasks), desc="Leads"):
            res = await f
            results.append(res)
        return results

def enrich_leads(leads: pd.DataFrame) -> pd.DataFrame:
    """Enrich a DataFrame of leads using CoreSignal API."""
    results = asyncio.run(enrich_all(leads))
    df = pd.DataFrame(results)
    return df 