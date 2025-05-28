import argparse
import yaml
import dask.dataframe as dd
from src.recon_engine import ReconciliationEngine

def load_config(path):
    with open(path, 'r') as file:
        return yaml.safe_load(file)

def load_csv(path):
    if path.endswith(".psv"):
        return dd.read_csv(path, sep="|", parse_dates=["date"])
    return dd.read_csv(path, parse_dates=["date"])

def main():
    parser = argparse.ArgumentParser(description="Run reconciliation between internal and external CSV files.")
    parser.add_argument("--config", type=str, default="examples/config.yaml", help="Path to config YAML file")
    parser.add_argument("--internal", type=str, default="data/internal_transactions.csv", help="Path to internal transaction CSV/PSV file")
    parser.add_argument("--external", type=str, default="data/external_transactions.csv", help="Path to external transaction CSV/PSV file")

    args = parser.parse_args()
    config = load_config(args.config)

    internal_df = load_csv(args.internal)
    external_df = load_csv(args.external)

    engine = ReconciliationEngine(
        match_on=config["matching"]["match_on"],
        internal_key=config["matching"]["internal_key"],
        external_key=config["matching"]["external_key"],
        amount_tolerance=config.get("tolerances", {}).get("amount", 0.0),
        date_tolerance_days=config.get("tolerances", {}).get("days", 0)
    )

    matched, unmatched_internal, unmatched_external = engine.reconcile(internal_df, external_df)

    output_path = config["output"]["path"]
    matched.compute().to_csv(f"{output_path}/{config['output']['reconciled_file']}", index=False)
    unmatched_internal.compute().to_csv(f"{output_path}/{config['output']['unmatched_internal_file']}", index=False)
    unmatched_external.compute().to_csv(f"{output_path}/{config['output']['unmatched_external_file']}", index=False)

    print(f"[✅] Outputs written to `{output_path}`")

if __name__ == "__main__":
    main()
