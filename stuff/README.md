# CoreSignal Lead Enrichment

This tool enriches a list of leads using the CoreSignal API, producing a detailed CSV for modeling and analytics.

## Features
- Person and company enrichment using CoreSignal v1 endpoints
- Async, concurrent, and cached API calls (10 max concurrent)
- Robust retry, error handling, and logging
- Caching of all API responses for 7 days
- Outputs a single enriched CSV with all required and extra fields

## Setup
1. Clone/download this repo.
2. Install dependencies:
   ```sh
   pip install -r requirements.txt
   ```
3. Set your CoreSignal API key in a `.env` file or as an environment variable:
   ```sh
   echo CORESIGNAL_API_KEY=your_key_here > .env
   ```

## Usage
```sh
python main.py leads.csv
```
- Input: `leads.csv` with columns: `email,first_name,last_name,company_name,coresignal_company_id`
- Output: `enriched_leads_YYYYMMDD.csv`

## Output Schema
- email,person_id,company_id,first_name,last_name,location_city,location_region,location_country,
  seniority,department,persona_type,headline,linkedin_connections,activity_score_30d,
  skills_top5,company_name,website,industry,headcount_current,headcount_3mo_delta_pct,
  funding_round_type,funding_amount_usd,funding_announced_date,
  open_jobs_30d,tech_tags,traffic_monthly_visits,traffic_3mo_delta_pct,staleness_flag
- Plus any extra fields found per person.

## Caching
- All API responses are cached in `.cache/{endpoint}/{id}.json` for 7 days.
- If a cached file exists and is fresh, the API call is skipped.

## Testing
Run unit tests with:
```sh
pytest tests/test_core_sig.py
```

## Example
See `leads.csv` for input format. Output sample:
```
email,person_id,company_id,first_name,last_name,location_city,...
jane@acme.com,123,456,Jane,Doe,San Francisco,...
```

---
MIT License 