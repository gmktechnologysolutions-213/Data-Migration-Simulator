
# Data Migration Simulator (MySQL ➜ Snowflake via PySpark)

A production-style demo showing how to migrate **large datasets** from **MySQL** to **Snowflake** using **PySpark**, with **data validation checks, error handling**, CI, and **Grafana dashboards** for migration KPIs.

## Features
- PySpark pipeline with partitioned JDBC read from MySQL and bulk write to Snowflake
- Robust **data quality validations** (row count match, null checks, schema checks)
- **Error handling & structured logging**; exports validation reports
- **Synthetic data generator** for reproducible testing
- **Grafana dashboard** JSON + sample screenshots
- **GitHub Actions CI** (lint + tests)
- Dockerfile for reproducible local runs

## Architecture (High Level)

```
MySQL (source)  --JDBC-->  PySpark (transform/validate)  --Snowflake Connector-->  Snowflake (target)
         |                         |                                |
         |                         |                                +--> Validation reports (CSV/JSON)
         |                         +--> Logs (structured)                 Grafana (KPIs via JSON/API)
         +--> Synthetic data generator
```

## Repo Structure
```
data-migration-simulator/
├── data/
│   ├── synthetic_generator.py
│   └── sample_schema.sql
├── grafana/
│   ├── dashboard.json
│   └── screenshots/
│       ├── overview.png
│       └── dq_report.png
├── src/
│   ├── config.py
│   ├── migration.py
│   ├── validation.py
│   └── utils/
│       └── logging_setup.py
├── tests/
│   └── test_validation.py
├── logs/  (runtime logs & reports)
├── .github/workflows/ci.yml
├── Dockerfile
├── requirements.txt
├── .env.example
└── README.md
```

## Setup

1) **Python & Java & Spark**
- Install Python 3.10+ and Java 8/11. Install Apache Spark 3.4+.
- Ensure `SPARK_HOME` is set and `pyspark` is available.

2) **Dependencies**
```bash
pip install -r requirements.txt
```

3) **Environment Variables** (copy and edit)
```bash
cp .env.example .env
```

4) **MySQL Source**
- Import `data/sample_schema.sql` to create `source_db.customer_orders` (or bring your own DB).
- Or use `data/synthetic_generator.py` to generate CSVs and load them into MySQL.

5) **Snowflake Target**
- Create a Snowflake database & schema.
- Provide credentials and parameters in `.env`.

## Run the Migration
```bash
python -m src.migration   --mysql-dbtable source_db.customer_orders   --snowflake-table ANALYTICS.PUBLIC.CUSTOMER_ORDERS   --batch-size 5000   --partitions 8   --dry-run false
```

**Tip:** Use `--dry-run true` to execute validations without writing to Snowflake.

## Data Quality & Reports
- Row counts, null distributions, and schema checks are saved to `logs/validation_report_<timestamp>.json` and `.csv`.
- Errors are logged to `logs/app.log`.

## Grafana
- The `grafana/dashboard.json` can be imported into Grafana (replace data sources as needed).
- Sample screenshots are in `grafana/screenshots/` for your GitHub README.

## CI
GitHub Actions (`.github/workflows/ci.yml`) runs unit tests and linting on push.

## Credits
Built for showcasing a freelance-ready, end-to-end data engineering capability with PySpark, MySQL, and Snowflake.
