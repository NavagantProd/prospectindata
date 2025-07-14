# main.py
import argparse
import sys
from pathlib import Path
import logging

import pandas as pd

from enhanced_client import EnhancedCoreSignalClient
from enhanced_enrichment import EnrichmentEngine
from safe_file_handler import SafeFileHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("enrichment.log"),
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger("main")


async def main():
    parser = argparse.ArgumentParser(description="Enrich leads with CoreSignal data")
    parser.add_argument("input_file", help="Path to input CSV file")
    parser.add_argument("--output", "-o", help="Output file path")
    parser.add_argument("--api-key", help="CoreSignal API key")
    parser.add_argument("--max-concurrent", type=int, default=5, help="Max concurrent requests")
    parser.add_argument("--diagnose", action="store_true", help="Run in diagnostic mode")
    args = parser.parse_args()

    api_key = args.api_key or EnhancedCoreSignalClient().api_key
    input_path = Path(args.input_file)
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        return 1

    client = EnhancedCoreSignalClient(api_key, max_concurrent=args.max_concurrent)
    engine = EnrichmentEngine(client)
    handler = SafeFileHandler()

    try:
        logger.info(f"Loading data from {input_path}")
        df = pd.read_csv(input_path)
        logger.info(f"Loaded {len(df)} records")
        if args.diagnose:
            logger.info("Running in diagnostic mode (first 3 rows only)")
            df = df.head(3)

        enriched = await engine.enrich_dataframe(df, max_concurrent=args.max_concurrent)

        output_path = Path(args.output) if args.output else handler.generate_output_filename()
        if handler.save_dataframe_safely(enriched, output_path):
            logger.info(f"Enrichment completed. Output: {output_path}")
            logger.info(f"Stats: {engine.stats}")
            return 0
        else:
            logger.error("Failed to save output")
            return 1

    except Exception as e:
        logger.error(f"Enrichment failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    import asyncio
    sys.exit(asyncio.run(main()))
