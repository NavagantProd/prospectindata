"""
Main enrichment pipeline
"""
import pandas as pd
import logging
import sys
import argparse
from pathlib import Path
from typing import List, Dict
import time
from datetime import datetime
from dataclasses import asdict

from config import INPUT_COLUMN_MAPPING, OUTPUT_SCHEMA, DEFAULT_INPUT_FILE, DEFAULT_OUTPUT_FILE
from coresignal_client import CoreSignalClient
from enrichment_engine import EnrichmentEngine, ContactRecord

def setup_logging(log_file: str = "pipeline.log"):
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def load_input_data(file_path: str) -> pd.DataFrame:
    """Load and validate input CSV data"""
    logger = logging.getLogger(__name__)
    
    if not Path(file_path).exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")
    
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Loaded {len(df)} rows from {file_path}")
        logger.info(f"Columns: {list(df.columns)}")
        # Clean and map columns using INPUT_COLUMN_MAPPING
        for key, col in INPUT_COLUMN_MAPPING.items():
            if col in df.columns:
                # Fill NaN with empty string, then convert to str, then replace string 'nan', 'NaN', 'None' with ''
                df[key] = df[col].fillna('').astype(str).replace(['nan', 'NaN', 'None'], '').str.strip()
            else:
                df[key] = ''
        # Debug: print mapping for first row
        if len(df) > 0:
            logger.info(f"[DEBUG] First row raw cs_company_name: '{df[INPUT_COLUMN_MAPPING['company_name']].iloc[0]}'")
            logger.info(f"[DEBUG] First row mapped company_name: '{df['company_name'].iloc[0]}'")
        logger.info(f"[DEBUG] DataFrame head after cleaning:\n{df.head()}\n")
        # Validate required columns exist
        required_cols = list(INPUT_COLUMN_MAPPING.keys())
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        # Remove rows with missing name or email
        initial_count = len(df)
        df = df[(df['name'] != '') & (df['email'] != '')]
        if len(df) < initial_count:
            logger.warning(f"Removed {initial_count - len(df)} rows with missing name/email")
        return df
    except Exception as e:
        logger.error(f"Error loading input file: {e}")
        raise

def create_contact_record(row: pd.Series) -> ContactRecord:
    """Create ContactRecord from DataFrame row"""
    return ContactRecord(
        name=str(row.get(INPUT_COLUMN_MAPPING['name'], '')).strip(),
        email=str(row.get(INPUT_COLUMN_MAPPING['email'], '')).strip().lower(),
        company_name=str(row.get(INPUT_COLUMN_MAPPING['company_name'], '')).strip(),
        company_website=str(row.get(INPUT_COLUMN_MAPPING['company_website'], '')).strip()
    )

def contact_to_dict(contact: ContactRecord, original_row: dict = None) -> dict:
    """Convert ContactRecord to dictionary including all dynamic fields and original input fields."""
    # Start with all attributes from the contact object
    result = dict(contact.__dict__)
    # Add original input fields if provided and not already present
    if original_row:
        for k, v in original_row.items():
            if k not in result:
                result[k] = v
    # Ensure all values are serializable (convert None to empty string)
    for key, value in result.items():
        if value is None:
            result[key] = ''
        elif not isinstance(value, (str, int, float, bool)):
            result[key] = str(value)
    return result

def save_results(contacts: List[dict], output_file: str):
    """Save enriched contacts to CSV"""
    logger = logging.getLogger(__name__)
    try:
        import os
        df = pd.DataFrame(contacts)
        from config import OUTPUT_SCHEMA
        # Ensure all schema columns are present
        for col in OUTPUT_SCHEMA:
            if col not in df.columns:
                df[col] = ""
        # Only keep schema columns in main output
        df_out = df[list(OUTPUT_SCHEMA)]
        df_out.to_csv(output_file, index=False, header=True)
        logger.info(f"Results saved to {output_file} (only schema columns)")
        logger.info(f"CSV header columns: {list(df_out.columns)}")
        # Optionally, save a full version with all columns for debugging
        full_output = os.path.splitext(output_file)[0] + '_full.csv'
        df.to_csv(full_output, index=False, header=True)
        logger.info(f"Full results (all columns) saved to {full_output}")
        # Log summary statistics
        total = len(contacts)
        successful = len([c for c in contacts if c.get('enrichment_status') == "success"])
        with_company = len([c for c in contacts if c.get('company_id')])
        with_employee = len([c for c in contacts if c.get('employee_id')])
        logger.info(f"Enrichment Summary:")
        logger.info(f"  Total contacts: {total}")
        logger.info(f"  Successful enrichments: {successful} ({successful/total*100:.1f}%)")
        logger.info(f"  With company data: {with_company} ({with_company/total*100:.1f}%)")
        logger.info(f"  With employee data: {with_employee} ({with_employee/total*100:.1f}%)")
    except Exception as e:
        logger.error(f"Error saving results: {e}")
        raise

def main():
    """Main pipeline execution"""
    parser = argparse.ArgumentParser(description="CoreSignal Lead Enrichment Pipeline")
    parser.add_argument("-i", "--input", default=DEFAULT_INPUT_FILE, 
                       help="Input CSV file path")
    parser.add_argument("-o", "--output", default=DEFAULT_OUTPUT_FILE,
                       help="Output CSV file path")
    parser.add_argument("-k", "--api-key", required=True,
                       help="CoreSignal API key")
    parser.add_argument("--sample", type=int,
                       help="Process only first N rows (for testing)")
    parser.add_argument("--log-file", default="pipeline.log",
                       help="Log file path")
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging(args.log_file)
    logger.info("Starting CoreSignal enrichment pipeline")
    
    start_time = time.time()
    
    try:
        # Load input data
        df = load_input_data(args.input)
        
        if args.sample:
            df = df.head(args.sample)
            logger.info(f"Processing sample of {len(df)} rows")
        
        # Initialize CoreSignal client and enrichment engine
        client = CoreSignalClient(args.api_key)
        engine = EnrichmentEngine(client)
        
        # Process each contact
        enriched_rows = []
        total_contacts = len(df)
        
        for idx, row in df.iterrows():
            # Use cleaned columns directly
            name = row['name']
            email = row['email']
            company_name = row['company_name']
            company_website = row['company_website']
            if idx < 5:
                logger.info(f"[DEBUG] Contact {idx+1}: name='{name}', company_name='{company_name}'")
            contact = ContactRecord(
                name=name,
                email=email,
                company_name=company_name,
                company_website=company_website
            )
            if not contact.is_valid():
                logger.warning(f"Invalid contact data: name='{contact.name}', email='{contact.email}'")
                contact.enrichment_status = "invalid"
                contact.enrichment_error = "Missing required fields (name or email)"
                enriched_rows.append(contact_to_dict(contact, original_row=row.to_dict()))
                continue
            try:
                enriched_contact = engine.enrich_contact(contact)
                # Use our custom function instead of asdict(), and pass original row
                enriched_rows.append(contact_to_dict(enriched_contact, original_row=row.to_dict()))
            except Exception as e:
                logger.error(f"Error enriching contact {contact.name}: {e}")
                error_contact = contact_to_dict(contact, original_row=row.to_dict())
                error_contact['enrichment_status'] = 'error'
                error_contact['enrichment_error'] = str(e)
                enriched_rows.append(error_contact)
            if (idx + 1) % 10 == 0:
                logger.info(f"Processed {idx + 1} contacts")
        
        # Save results
        save_results(enriched_rows, args.output)
        
        
        # Final statistics
        end_time = time.time()
        duration = end_time - start_time
        
        stats = client.get_stats()
        logger.info(f"Pipeline completed in {duration:.2f} seconds")
        logger.info(f"API Statistics: {stats}")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()