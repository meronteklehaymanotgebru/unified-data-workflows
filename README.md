# unified-data-workflows

A minimal ETL scaffold to simulate a unified data layer:
- Read sample CRM and Sales CSVs
- Apply a mapping-driven transform (mappings.yml)
- Normalize dates, money, emails
- Link sales orders to leads and produce a unified `customers` CSV

## Quick start

```bash
# clone the repo or create folder
python -m venv .venv
# mac/linux
source .venv/bin/activate
# windows (powershell)
# .venv\Scripts\Activate.ps1

pip install -r requirements.txt

# Run ETL
python etl/etl_merge.py

# Open output/unified_customers.csv
