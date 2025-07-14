import subprocess
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger("FullPipeline")

# Determine project root (directory containing both Made_Market and coresignal_enrichment)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Paths to scripts
mademarket_pull = os.path.join(PROJECT_ROOT, "Made_Market", "MadeMarket_Pull.py")
company_enrich = os.path.join(PROJECT_ROOT, "coresignal_enrichment", "Company_Enrich.py")
person_enrich = os.path.join(PROJECT_ROOT, "coresignal_enrichment", "Person_Enrich.py")

# Input/Output files
opened_csv = os.path.join(PROJECT_ROOT, "Made_Market_Data", "opened", "mademarket_2025_ISTE_opened.csv")
unopened_csv = os.path.join(PROJECT_ROOT, "Made_Market_Data", "unopened", "mademarket_2025_ISTE_unopened.csv")
company_opened_out = os.path.join(PROJECT_ROOT, "coresignal_enrichment", "company_enriched", "opened_enriched.csv")
company_unopened_out = os.path.join(PROJECT_ROOT, "coresignal_enrichment", "company_enriched", "unopened_enriched.csv")
person_opened_out = os.path.join(PROJECT_ROOT, "coresignal_enrichment", "person_enriched", "opened_enriched.csv")
person_unopened_out = os.path.join(PROJECT_ROOT, "coresignal_enrichment", "person_enriched", "unopened_enriched.csv")


def run_step(cmd_args, desc):
    logger.info(f"Starting: {desc}")
    result = subprocess.run(cmd_args)
    if result.returncode != 0:
        logger.error(f"Step failed: {desc}")
        exit(1)
    logger.info(f"Completed: {desc}")


def main():
    # 1. Pull and segment Mademarket data
    run_step(["python", mademarket_pull], "Pull and segment Mademarket data")

    # 2. Company enrichment
    run_step(["python", company_enrich, "--input", opened_csv, "--output", company_opened_out], "Company enrichment (opened)")
    run_step(["python", company_enrich, "--input", unopened_csv, "--output", company_unopened_out], "Company enrichment (unopened)")

    # 3. Person enrichment
    run_step(["python", person_enrich, "--input", opened_csv, "--output", person_opened_out], "Person enrichment (opened)")
    run_step(["python", person_enrich, "--input", unopened_csv, "--output", person_unopened_out], "Person enrichment (unopened)")

    logger.info("Full pipeline completed successfully.")

if __name__ == "__main__":
    main() 