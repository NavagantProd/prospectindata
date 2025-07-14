# Mademarket + CoreSignal Enrichment Pipeline

## Overview
This pipeline automates the process of pulling, segmenting, and enriching recipient data from the Mademarket API with company and person-level data from CoreSignal. Outputs are organized for easy access and further analysis.

---

## 1. Mademarket API Data Pull
- **Script:** `Made_Market/MadeMarket_Pull.py`
- **Function:** Pulls recipient data for a specified distribution and segments into opened/unopened.
- **How to run:**
  ```sh
  python Made_Market/MadeMarket_Pull.py
  ```
- **Outputs:**
  - `Made_Market_Data/mademarket_2025_ISTE.csv`
  - `Made_Market_Data/opened/mademarket_2025_ISTE_opened.csv`
  - `Made_Market_Data/unopened/mademarket_2025_ISTE_unopened.csv`

---

## 2. Company Enrichment (CoreSignal)
- **Script:** `coresignal_enrichment/Company_Enrich.py`
- **Function:** Enriches each recipient with company data from CoreSignal using the email domain.
- **How to run:**
  ```sh
  python coresignal_enrichment/Company_Enrich.py --input Made_Market_Data/opened/mademarket_2025_ISTE_opened.csv --output coresignal_enrichment/company_enriched/opened_enriched.csv
  ```
- **Outputs:**
  - `coresignal_enrichment/company_enriched/opened_enriched.csv`
  - `coresignal_enrichment/company_enriched/unopened_enriched.csv`

---

## 3. Person Enrichment (CoreSignal)
- **Script:** `coresignal_enrichment/Person_Enrich.py`
- **Function:** Attempts to enrich each recipient with person-level data from CoreSignal using available information (email, first/last name, company name).
- **How to run:**
  ```sh
  python coresignal_enrichment/Person_Enrich.py --input Made_Market_Data/opened/mademarket_2025_ISTE_opened.csv --output coresignal_enrichment/person_enriched/opened_enriched.csv
  ```
- **Outputs:**
  - `coresignal_enrichment/person_enriched/opened_enriched.csv`
  - `coresignal_enrichment/person_enriched/unopened_enriched.csv`

---

## 4. Output Directory Structure
```
prospectindata/
  Made_Market_Data/
    mademarket_2025_ISTE.csv
    opened/
      mademarket_2025_ISTE_opened.csv
    unopened/
      mademarket_2025_ISTE_unopened.csv
  coresignal_enrichment/
    company_enriched/
      opened_enriched.csv
      unopened_enriched.csv
    person_enriched/
      opened_enriched.csv
      unopened_enriched.csv
```

---

## 5. Notes on Person Enrichment
- The pipeline preprocesses recipient emails to extract first/last names and company names for enrichment.
- **Limitation:** CoreSignal rarely has emails for individuals; name and company must match their public professional profile for a successful enrichment.
- **Current status:** Despite preprocessing, individual-level data is not consistently populated. Raahul has previously had this working and will take ownership of further refinement.

---

## 6. How to Run the Full Pipeline
1. **Pull and segment Mademarket data:**
   ```sh
   python Made_Market/MadeMarket_Pull.py
   ```
2. **Enrich with company data:**
   ```sh
   python coresignal_enrichment/Company_Enrich.py --input Made_Market_Data/opened/mademarket_2025_ISTE_opened.csv --output coresignal_enrichment/company_enriched/opened_enriched.csv
   ```
3. **Enrich with person data:**
   ```sh
   python coresignal_enrichment/Person_Enrich.py --input Made_Market_Data/opened/mademarket_2025_ISTE_opened.csv --output coresignal_enrichment/person_enriched/opened_enriched.csv
   ```

---

## 7. Handoff & Next Steps
- The pipeline is modular, robust, and well-logged.
- Company enrichment is working and outputs are as expected.
- Person enrichment is set up and ready for further refinement by Raahul or others with more experience on the CoreSignal person API.
- All scripts are documented and can be run independently for testing or troubleshooting. 