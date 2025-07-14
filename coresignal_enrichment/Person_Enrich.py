import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import argparse
import pandas as pd
import logging
from core_sig.enrichment import enrich_leads

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger("Person_Enrich")

def preprocess_leads(df):
    # Add first_name and last_name if not present
    if 'first_name' not in df.columns:
        df['first_name'] = df['recipient_email'].str.split('@').str[0].str.split('.').str[0].str.title()
    if 'last_name' not in df.columns:
        df['last_name'] = df['recipient_email'].str.split('@').str[0].str.split('.').str[1].fillna('').str.title()
    # Add company_name if not present
    if 'company_name' not in df.columns:
        df['company_name'] = df['recipient_email'].str.split('@').str[1].str.split('.').str[0].str.title()
    return df

def main():
    parser = argparse.ArgumentParser(description="Enrich Mademarket recipients with CoreSignal employee data (async, unified logic)")
    parser.add_argument('--input', required=True, help='Input CSV file (from new data collection)')
    parser.add_argument('--output', required=True, help='Output enriched CSV file')
    args = parser.parse_args()

    input_file = args.input
    output_file = args.output

    logger.info(f"Reading input file: {input_file}")
    df = pd.read_csv(input_file)
    logger.info(f"Read {len(df)} rows from input file")

    # Preprocess to add first_name, last_name, company_name
    df = preprocess_leads(df)
    logger.info("First 5 rows after preprocessing:")
    logger.info(df.head())

    logger.info("Starting enrichment using core_sig.enrichment.enrich_leads...")
    enriched_df = enrich_leads(df)
    logger.info(f"Enrichment complete. Writing {len(enriched_df)} rows to {output_file}")
    logger.info(f"Enriched columns: {list(enriched_df.columns)}")
    logger.info(f"First 5 enriched rows:\n{enriched_df.head()}\n")
    enriched_df.to_csv(output_file, index=False)
    logger.info("Done.")

if __name__ == "__main__":
    main()