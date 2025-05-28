# Reconciliation Engine

A powerful and flexible data reconciliation tool that supports multiple data sources including files (CSV, PSV) and databases (BigQuery, Redshift). The engine helps you match and reconcile transactions between different sources, with support for amount and date tolerances.

## Features

- **Multiple Data Source Support**:
  - Files:
    - CSV files (comma-separated)
    - PSV files (pipe-separated)
    - Local filesystem
    - Amazon S3 (`s3://` URLs)
    - Google Cloud Storage (`gs://` URLs)
  - Databases:
    - Google BigQuery
    - Amazon Redshift

- **Flexible Matching**:
  - Match on multiple fields
  - Amount tolerance support
  - Date tolerance support
  - Customizable matching keys

- **Output**:
  - Matched records
  - Unmatched internal records
  - Unmatched external records

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/reconciliation-engine.git
cd reconciliation-engine

# Install dependencies
pip install -r requirements.txt

# Set up cloud credentials (if needed)
# For AWS S3/Redshift:
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key

# For GCP (BigQuery/GCS):
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
```

## Usage

The reconciliation engine is configured using YAML files. Run it using:

```bash
python main.py --config path/to/your/config.yaml
```

## Configuration

### File-based Configuration Example

```yaml
sources:
  internal:
    type: "file"
    path: "data/internal_transactions.csv"  # Local file
    # or "s3://bucket/internal.csv"        # S3 file
    # or "gs://bucket/internal.psv"        # GCS file (pipe-separated)
  
  external:
    type: "file"
    path: "data/external_transactions.csv"

matching:
  match_on: ["date", "customer_id", "location_id"]
  internal_key: "amount"
  external_key: "net_amount"

tolerances:
  amount: 0.01  # Allow $0.01 difference in amounts
  days: 1       # Allow 1 day difference in dates

output:
  path: "reconciliation_results"
  reconciled_file: "matched.csv"
  unmatched_internal_file: "unmatched_internal.csv"
  unmatched_external_file: "unmatched_external.csv"
```

### Database Configuration Example

```yaml
sources:
  internal:
    type: "bigquery"
    query: |
      SELECT
        date,
        customer_id,
        location_id,
        amount
      FROM `project.dataset.internal_transactions`
      WHERE date >= '2024-01-01'
    connection:
      project_id: "your-gcp-project-id"

  external:
    type: "redshift"
    query: |
      SELECT
        date,
        customer_id,
        location_id,
        net_amount
      FROM external_transactions
      WHERE date >= '2024-01-01'
    connection:
      host: "your-redshift-cluster.region.redshift.amazonaws.com"
      database: "your_database"
      user: "your_user"
      password: "your_password"
      port: 5439        # optional, defaults to 5439
      schema: "public"  # optional, defaults to public

matching:
  match_on: ["date", "customer_id", "location_id"]
  internal_key: "amount"
  external_key: "net_amount"

tolerances:
  amount: 0.01
  days: 1

output:
  path: "reconciliation_results"
  reconciled_file: "matched.csv"
  unmatched_internal_file: "unmatched_internal.csv"
  unmatched_external_file: "unmatched_external.csv"
```

### Mixed Source Configuration Example

You can mix different source types:

```yaml
sources:
  internal:
    type: "file"
    path: "s3://my-bucket/internal/transactions.csv"
  
  external:
    type: "bigquery"
    query: "SELECT * FROM external_transactions"
    connection:
      project_id: "your-gcp-project-id"
```

## Configuration Details

### Source Types

1. **File Sources** (`type: "file"`):
   - Required fields:
     - `path`: File location (local, S3, or GCS URL)
   - Supported formats:
     - `.csv`: Comma-separated files
     - `.psv`: Pipe-separated files

2. **BigQuery Sources** (`type: "bigquery"`):
   - Required fields:
     - `query`: SQL query to fetch data
     - `connection.project_id`: GCP project ID

3. **Redshift Sources** (`type: "redshift"`):
   - Required fields:
     - `query`: SQL query to fetch data
     - `connection.host`: Redshift host
     - `connection.database`: Database name
     - `connection.user`: Username
     - `connection.password`: Password
   - Optional fields:
     - `connection.port`: Port number (default: 5439)
     - `connection.schema`: Schema name (default: public)

### Matching Configuration

- `match_on`: List of fields to match records on
- `internal_key`: Amount field name in internal source
- `external_key`: Amount field name in external source

### Tolerances

- `amount`: Maximum allowed difference between amounts
- `days`: Maximum allowed difference between dates (in days)

### Output Configuration

- `path`: Directory to write output files
- `reconciled_file`: Filename for matched records
- `unmatched_internal_file`: Filename for unmatched internal records
- `unmatched_external_file`: Filename for unmatched external records

## Required Data Fields

Your data sources must include these fields (column names can be configured):
- `date`: Transaction date
- Amount field (configurable name)
- Any additional fields specified in `match_on`

## Development

```bash
# Run tests
pytest tests/

# Run specific test file
pytest tests/test_recon_engine.py
```

## Error Handling

The engine provides detailed logging for:
- File access issues
- Database connection problems
- Authentication errors
- Data format issues
- Matching process details

Logs include timestamps and error details for troubleshooting.
