# Academic Research ETL Pipeline — Demo

**Zero dependencies. Just Python 3. Runs in under 5 seconds.**

## Run it

```bash
python3 pipeline.py
```

## What it does

| Stage | What happens |
|-------|-------------|
| **Extract** | Simulates OpenAlex REST API with cursor pagination & retry logic |
| **Transform** | Flattens JSON, deduplicates, normalises DOIs, validates nulls |
| **Load** | Upserts into SQLite (↔ AWS RDS) + writes CSVs (↔ AWS S3) |
| **Report** | Live analytics: citations, open access %, language breakdown |

## Output files

```
academic_research.db        ← SQLite database (4 tables)
output/
  papers_*.csv              ← cleaned paper records
  authors_*.csv             ← unique author records
  institutions_*.csv        ← institution records
  paper_authors_*.csv       ← many-to-many bridge table
  raw_*.ndjson              ← raw API responses (newline-delimited JSON)
```

## Production equivalent

| Demo | Production (AWS) |
|------|-----------------|
| Simulated API data | Real OpenAlex REST API |
| SQLite | AWS RDS (PostgreSQL) |
| CSV files | AWS S3 (Parquet, snappy-compressed) |
| `python pipeline.py` | AWS Lambda + EventBridge (runs every 6h) |
| Print output | AWS CloudWatch Logs |

## Tech Stack
`Python 3.10` · `sqlite3` · `json` · `csv` · `datetime`

In production: `boto3` · `psycopg2` · `pandas` · `pyarrow` · `tenacity` · `AWS Lambda` · `S3` · `RDS` · `EventBridge`

---
*Built as part of academic research at university — 2023*
