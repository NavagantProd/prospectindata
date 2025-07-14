# Mademarket Data Pull & Segmentation

## Overview

This directory contains logic for pulling recipient data from the Mademarket API for a specific distribution (e.g., "2025 ISTE") and segmenting that data based on email activity.

---

## What the Script Does

- **Script:** `MadeMarket_Pull.py`
- **Functionality:**
  1. **Pulls recipient data** for a specified distribution from the Mademarket API.
  2. **Saves the full recipient list** as a CSV in this directory:  
     - `Made_Market/mademarket_<DISTRIBUTION>.csv`
  3. **Segments recipients** into:
     - `Made_Market/opened/mademarket_<DISTRIBUTION>_opened.csv` (recipients who opened the email)
     - `Made_Market/unopened/mademarket_<DISTRIBUTION>_unopened.csv` (recipients who did not open)

---

## How to Run

From the project root, run:
```sh
python Made_Market/MadeMarket_Pull.py
```

---

## Output Files

- `Made_Market/mademarket_<DISTRIBUTION>.csv`  
  The full recipient list for the specified distribution.
- `Made_Market/opened/mademarket_<DISTRIBUTION>_opened.csv`  
  Only recipients who opened the email.
- `Made_Market/unopened/mademarket_<DISTRIBUTION>_unopened.csv`  
  Only recipients who did not open the email.

---

## Notes

- All output files are saved inside the `Made_Market/` directory and its subfolders.
- This directory is **only for raw Mademarket data pulls and segmentation**.  
  All enrichment (CoreSignal, etc.) should be handled in a separate directory (e.g., `coresignal_enrichment/`).

---

## Troubleshooting

- Ensure your API key is valid and the distribution name matches exactly.
- Check the console output for the exact file paths written. 