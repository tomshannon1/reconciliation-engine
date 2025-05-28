import dask.dataframe as dd
import yaml
from src.recon_engine import ReconciliationEngine

def load_config(path="config.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def save_outputs(matched, unmatched_internal, unmatched_external, config):
    output_path = config["output"]["path"]
    matched.compute().to_csv(f"{output_path}/{config['output']['reconciled_file']}", index=False)
    unmatched_internal.compute().to_csv(f"{output_path}/{config['output']['unmatched_internal_file']}", index=False)
    unmatched_external.compute().to_csv(f"{output_path}/{config['output']['unmatched_external_file']}", index=False)
    print("[INFO] Output files written to", output_path)

def main():
    config = load_config()

    # Load data using Dask
    internal_df = dd.read_csv("data/internal_transactions.csv", parse_dates=["date"])
    external_df = dd.read_csv("data/external_transactions.csv", parse_dates=["date"])

    # Configure engine
    engine = ReconciliationEngine(
        match_on=config["matching"]["match_on"],
        internal_key=config["matching"]["internal_key"],
        external_key=config["matching"]["external_key"]
    )

    # Reconcile
    matched, unmatched_internal, unmatched_external = engine.reconcile(internal_df, external_df)

    # Save results
    save_outputs(matched, unmatched_internal, unmatched_external, config)

if __name__ == "__main__":
    main()
