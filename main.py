"""
Reconciliation engine main script.
"""
import argparse
import logging
from src.recon_engine import ReconciliationEngine
from src.source_loader import SourceLoader
from src.config_validator import ConfigValidator

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Run the reconciliation process."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Run reconciliation between internal and external sources (files or databases)."
    )
    parser.add_argument(
        "--config",
        type=str,
        default="examples/config.yaml",
        help="Path to config YAML file"
    )
    args = parser.parse_args()

    # Load and validate configuration
    config = ConfigValidator.load_and_validate(args.config)
    
    # Load data from sources
    logger.info("Loading data sources...")
    internal_df = SourceLoader.load_source(config["sources"]["internal"])
    external_df = SourceLoader.load_source(config["sources"]["external"])

    # Initialize and run reconciliation
    logger.info("Running reconciliation...")
    engine = ReconciliationEngine(
        match_on=config["matching"]["match_on"],
        internal_key=config["matching"]["internal_key"],
        external_key=config["matching"]["external_key"],
        amount_tolerance=config.get("tolerances", {}).get("amount", 0.0),
        date_tolerance_days=config.get("tolerances", {}).get("days", 0)
    )

    matched, unmatched_internal, unmatched_external = engine.reconcile(internal_df, external_df)

    # Save results
    logger.info("Saving results...")
    output_path = config["output"]["path"]
    matched.compute().to_csv(f"{output_path}/{config['output']['reconciled_file']}", index=False)
    unmatched_internal.compute().to_csv(f"{output_path}/{config['output']['unmatched_internal_file']}", index=False)
    unmatched_external.compute().to_csv(f"{output_path}/{config['output']['unmatched_external_file']}", index=False)

    logger.info(f"✅ Outputs written to `{output_path}`")

if __name__ == "__main__":
    main()
