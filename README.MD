# Automated Reconciliation Engine

This project is a modular reconciliation engine for financial transaction logs. It compares internal transaction data with external payment processor reports (like Stripe, PayPal, or bank exports) and outputs matched, unmatched, and mismatched records.

## 🚀 Features

- Load internal and external transaction data (CSV-based)
- Apply reconciliation logic with configurable tolerances
- Scalable data processing using **Dask**
- Generate summary reports to CSV files
- Easily extendable to support databases and streaming sources

## ⚙️ Why Dask?

Unlike Pandas, which loads everything into memory, **Dask** is designed for scalable parallel processing. It works like Pandas but under the hood uses lazy evaluation and chunked processing, making it better suited for large datasets.

- 🧠 Handles **millions of rows** (10M+) without crashing
- ⚡ Efficient parallel computation across CPU cores
- 🧩 Drop-in Pandas compatibility for most operations

> For massive, distributed workloads (100M+ rows or terabyte-scale), this repo can be extended to support **Apache Spark** in a future iteration.

## 📦 Setup

1. Clone the repository:

```bash
git clone https://github.com/yourusername/automated-reconciliation-engine.git
cd automated-reconciliation-engine
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Run the engine:

```bash
python main.py
```

## 🎯 CLI Usage

Run the reconciliation engine with specific configuration and data files:

```bash
python cli.py --config config.yaml --internal data/internal_transactions.csv --external data/external_transactions.csv
```

## Testing
To run the tests, run the following command from the root directory:
```bash
python3 -m pytest
```

### Parameters:
- `--config`: Path to the YAML configuration file
- `--internal`: Path to the internal transactions CSV file
- `--external`: Path to the external transactions CSV file

## 🐳 Run with Docker

To run the project using Docker:

```bash
docker build -t recon-engine .
docker run --rm -v $(pwd)/data:/app/data -v $(pwd)/outputs:/app/outputs recon-engine
```

> 💡 Ensure the `data/` directory contains `internal_transactions.csv` and `external_transactions.csv`.
> The `outputs/` directory will be created if it doesn't exist and will contain the resulting CSV reports.

## 📂 Folder Structure

```
.
├── data/                   # Input data files (CSV)
├── outputs/                # Reconciliation result outputs
├── src/                    # Source code (loader, recon logic, etc.)
├── tests/                  # Unit tests
├── main.py                 # CLI entrypoint
├── requirements.txt
├── Dockerfile
├── pyproject.toml
└── README.md
```
