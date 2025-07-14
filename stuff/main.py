import sys
import os
import argparse
import pandas as pd
from datetime import datetime
from core_sig.enrichment import enrich_leads
from dotenv import load_dotenv
import logging
from stuff.config import LOG_LEVEL

load_dotenv()

def setup_logging():
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
        format='%(asctime)s %(levelname)s %(name)s %(message)s',
        handlers=[logging.StreamHandler()]
    )

def main():
    parser = argparse.ArgumentParser(description="Enrich leads using CoreSignal API.")
    parser.add_argument('input_csv', nargs='?', help='Input leads CSV')
    parser.add_argument('-o', '--output', help='Output CSV', default=None)
    parser.add_argument('--log', help='Log file', default=None)
    parser.add_argument('--diagnose', help='Test a single email for v1/v2 search and print full request/response', default=None)
    args = parser.parse_args()
    setup_logging()
    if args.log:
        fh = logging.FileHandler(args.log, encoding='utf-8')
        fh.setLevel(logging.DEBUG)
        logging.getLogger().addHandler(fh)
    if args.diagnose:
        import asyncio
        from core_sig.client import CoreSignalClient
        client = CoreSignalClient()
        email = args.diagnose.strip().lower()
        print(f"Diagnosing CoreSignal search for: {email}")
        async def diag():
            try:
                print("Trying v1 GET /v1/people/search?email=...")
                try:
                    resp1 = await client.person_by_email(email)
                    print("v1 response:", resp1)
                except Exception as e:
                    print(f"v1 error: {e}")
                print("Trying v2 POST /cdapi/v2/member/search/filter ...")
                try:
                    resp2 = await client.person_search_v2(email)
                    print("v2 response:", resp2)
                except Exception as e:
                    print(f"v2 error: {e}")
            except Exception as e:
                print(f"Diagnostic error: {e}")
        asyncio.run(diag())
        return
    if not args.input_csv:
        print("Usage: python main.py leads.csv [--diagnose EMAIL]")
        return
    leads_path = args.input_csv
    if not os.path.exists(leads_path):
        print(f"File not found: {leads_path}")
        return
    leads = pd.read_csv(leads_path)
    print(f"Loaded {len(leads)} leads from {leads_path}")
    enriched = enrich_leads(leads)
    today = datetime.now().strftime('%Y%m%d')
    out_path = args.output or f"enriched_leads_{today}.csv"
    enriched.to_csv(out_path, index=False)
    print(f"Wrote {len(enriched)} enriched leads to {out_path}")
    # Write error report if any
    if 'error' in enriched.columns:
        err_path = out_path.replace('.csv', '_errors.csv')
        enriched[enriched['error'].notnull()].to_csv(err_path, index=False)
        logging.info(f"Wrote error report to {err_path}")
    # Print summary
    n_errors = enriched['error'].notnull().sum() if 'error' in enriched.columns else 0
    print(f"Enrichment complete. {len(enriched)} rows. {n_errors} errors.")

if __name__ == "__main__":
    main() 