# CoreSignal Enrichment Workflow

## Overview
This directory contains all scripts and outputs for enriching Mademarket data with company and person information from the CoreSignal API.

---

## Directory Structure

```
coresignal_enrichment/
  Company_Enrich.py         # Enriches company data using email domain
  Person_Enrich.py          # Enriches person data using email address
  company_enriched/         # Output: company-enriched CSVs
  person_enriched/          # Output: person-enriched CSVs
```

---

## Prerequisites
- **Python 3.8+**
- **Required packages:**
  - `requests`
  - `python-dotenv`
- **CoreSignal API Key:**
  - Set your API key in a `.env` file or as an environment variable:
    ```
    CORESIGNAL_API_KEY=your_coresignal_api_key_here
    ```

---

## Enriching Company Data
**Script:** `Company_Enrich.py`

- **What it does:**
  - Reads any Mademarket CSV (opened, unopened, or all) as input.
  - For each recipient, extracts the domain from their email address.
  - Queries the CoreSignal Multi-Source Company API for each unique domain.
  - Appends all available company data to each row.
  - Writes the enriched output to `coresignal_enrichment/company_enriched/` by default.
  - **Note:** By default, the script is set to process only the first 5 unique domains for testing. Remove this limit for full enrichment.

- **How to run for opened recipients:**
  ```bash
  python coresignal_enrichment/Company_Enrich.py --input Made_Market_Data/opened/mademarket_2025_ISTE_opened.csv --output coresignal_enrichment/company_enriched/mademarket_2025_ISTE_opened_enriched.csv
  ```
- **How to run for unopened recipients:**
  ```bash
  python coresignal_enrichment/Company_Enrich.py --input Made_Market_Data/unopened/mademarket_2025_ISTE_unopened.csv --output coresignal_enrichment/company_enriched/mademarket_2025_ISTE_unopened_enriched.csv
  ```
- **How to run for all recipients:**
  ```bash
  python coresignal_enrichment/Company_Enrich.py --input Made_Market_Data/mademarket_2025_ISTE.csv --output coresignal_enrichment/company_enriched/mademarket_2025_ISTE_company_enriched.csv
  ```

---

## Enriching Person Data
**Script:** `Person_Enrich.py`

- **What it does:**
  - Reads any Mademarket CSV (opened, unopened, or all) as input.
  - For each recipient, uses the email address to query the CoreSignal Member API.
  - Appends all available person data to each row.
  - Writes the enriched output to `coresignal_enrichment/person_enriched/` by default.
  - **Note:** By default, the script is set to process only the first 5 unique emails for testing. Remove this limit for full enrichment.

- **How to run for opened recipients:**
  ```bash
  python coresignal_enrichment/Person_Enrich.py --input Made_Market_Data/opened/mademarket_2025_ISTE_opened.csv --output coresignal_enrichment/person_enriched/mademarket_2025_ISTE_opened_person_enriched.csv
  ```
- **How to run for unopened recipients:**
  ```bash
  python coresignal_enrichment/Person_Enrich.py --input Made_Market_Data/unopened/mademarket_2025_ISTE_unopened.csv --output coresignal_enrichment/person_enriched/mademarket_2025_ISTE_unopened_person_enriched.csv
  ```
- **How to run for all recipients:**
  ```bash
  python coresignal_enrichment/Person_Enrich.py --input Made_Market_Data/mademarket_2025_ISTE.csv --output coresignal_enrichment/person_enriched/mademarket_2025_ISTE_person_enriched.csv
  ```

---

## Output Files
- `coresignal_enrichment/company_enriched/` — Company-enriched CSVs
- `coresignal_enrichment/person_enriched/` — Person-enriched CSVs

---

## Customization
- To process more than 5 domains or emails, remove or adjust the test limit in the scripts.
- You can use the CLI arguments to enrich any CSV you want.

---

## Troubleshooting
- Ensure your `CORESIGNAL_API_KEY` is set and valid.
- Check the logs for API errors or empty results.
- If you encounter rate limiting, the scripts will automatically pause and retry.

---

## Contact
For questions or issues, contact the data engineering team or the workflow maintainer. 