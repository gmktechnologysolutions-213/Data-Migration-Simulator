import argparse
import json
from datetime import datetime
from typing import Dict

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException

from .config import MySQLConfig, SnowflakeConfig, AppConfig
from .validation import null_check, schema_check, rowcount_check, with_audit_columns
from .utils.logging_setup import configure_logging

def build_spark(app_name: str = "DataMigration"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )
    return spark

def read_mysql(spark: SparkSession, cfg: MySQLConfig, table: str, partitions: int = 8):
    jdbc_url = f"jdbc:mysql://{cfg.host}:{cfg.port}/{cfg.database}"
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", table)
        .option("user", cfg.user)
        .option("password", cfg.password)
        .option("fetchsize", 10000)
        .load()
    )
    # Optional: Repartition for parallelism
    if partitions and partitions > 1:
        # Choose a numeric column if available; fallback to hash partitioning
        cols = [f.name for f in df.schema.fields if f.dataType.simpleString().startswith("int")]
        if cols:
            df = df.repartition(partitions, col(cols[0]))
        else:
            df = df.repartition(partitions)
    return df

def write_snowflake(df, sfcfg: SnowflakeConfig, table: str, mode: str = "overwrite", batch_size: int = 5000, dry_run: bool = False):
    if dry_run:
        return 0
    # Requires spark-snowflake connector on the classpath when running spark-submit
    sfoptions = {
        "sfURL": f"{sfcfg.account}.snowflakecomputing.com",
        "sfUser": sfcfg.user,
        "sfPassword": sfcfg.password,
        "sfWarehouse": sfcfg.warehouse,
        "sfDatabase": sfcfg.database,
        "sfSchema": sfcfg.schema,
        "sfRole": sfcfg.role,
        "dbtable": table,
    }
    (df.write
        .format("snowflake")
        .options(**sfoptions)
        .option("truncate_table", "on" if mode == "overwrite" else "off")
        .option("autopushdown", "on")
        .option("parallelism", "8")
        .option("batchSize", str(batch_size))
        .mode(mode)
        .save())
    return df.count()

def run_validations(df_src, df_tgt_sample, expected_schema: Dict[str, str], run_id: str) -> Dict:
    # df_tgt_sample can be empty in dry-run; we still run schema/null checks on source
    cols = list(expected_schema.keys())
    nulls = null_check(df_src, cols)
    schema_ok = schema_check(df_src, expected_schema)
    src_count = df_src.count()
    tgt_count = df_tgt_sample.count() if df_tgt_sample is not None else 0
    counts = rowcount_check(src_count, tgt_count)
    return {
        "run_id": run_id,
        "nulls": nulls,
        "schema_match": schema_ok,
        "counts": counts,
    }

def save_reports(report: Dict, run_id: str):
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    json_path = f"logs/validation_report_{run_id}_{ts}.json"
    with open(json_path, "w") as f:
        json.dump(report, f, indent=2)
    # Simple CSV export
    csv_path = f"logs/validation_counts_{run_id}_{ts}.csv"
    with open(csv_path, "w") as f:
        f.write("metric,value\n")
        f.write(f"source_count,{report['counts']['source_count']}\n")
        f.write(f"target_count,{report['counts']['target_count']}\n")
        f.write(f"diff,{report['counts']['diff']}\n")
    return json_path, csv_path

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mysql-dbtable", required=True, help="e.g. source_db.customer_orders")
    parser.add_argument("--snowflake-table", required=True, help="e.g. ANALYTICS.PUBLIC.CUSTOMER_ORDERS")
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--partitions", type=int, default=8)
    parser.add_argument("--dry-run", type=str, default="true")
    args = parser.parse_args()

    mysql_table = args.mysql_dbtable
    snowflake_table = args.snowflake_table
    batch_size = args.batch_size
    partitions = args.partitions
    dry_run = args.dry_run.lower() == "true"

    appcfg = AppConfig()
    log = configure_logging(appcfg.log_level)
    run_id = datetime.utcnow().strftime("%Y%m%d%H%M%S")

    try:
        spark = build_spark("DataMigration")
        log.info("Starting read from MySQL...")
        mysql_cfg = MySQLConfig()
        df_src = read_mysql(spark, mysql_cfg, mysql_table, partitions=partitions)
        log.info(f"Source count (sample/approx may differ until action): {df_src.count()}")

        # expected schema map
        expected = {f.name: f.dataType.simpleString() for f in df_src.schema.fields}

        log.info("Adding audit columns...")
        df_aug = with_audit_columns(df_src, run_id)

        log.info("Writing to Snowflake (dry_run=%s)...", dry_run)
        sfcfg = SnowflakeConfig()
        tgt_written = write_snowflake(df_aug, sfcfg, snowflake_table, mode="overwrite", batch_size=batch_size, dry_run=dry_run)
        log.info("Write completed. Rows written (0 if dry-run): %s", tgt_written)

        # Optionally sample from Snowflake to validate target (skipped in dry_run)
        df_tgt_sample = None
        if not dry_run:
            # For demonstration: read back first 1000 rows (requires connector)
            df_tgt_sample = (
                spark.read.format("snowflake")
                .options(**{
                    "sfURL": f"{sfcfg.account}.snowflakecomputing.com",
                    "sfUser": sfcfg.user,
                    "sfPassword": sfcfg.password,
                    "sfWarehouse": sfcfg.warehouse,
                    "sfDatabase": sfcfg.database,
                    "sfSchema": sfcfg.schema,
                    "sfRole": sfcfg.role,
                    "dbtable": snowflake_table,
                })
                .load()
                .limit(1000)
            )

        log.info("Running validations...")
        report = run_validations(df_src=df_src, df_tgt_sample=df_tgt_sample, expected_schema=expected, run_id=run_id)
        json_path, csv_path = save_reports(report, run_id)
        log.info("Validation reports saved: %s, %s", json_path, csv_path)

        log.info("Migration completed successfully.")
    except AnalysisException as e:
        log.exception("Spark analysis error: %s", e)
        raise
    except Exception as e:
        log.exception("Unhandled error: %s", e)
        raise
    finally:
        try:
            spark.stop()
        except Exception:
            pass

if __name__ == "__main__":
    main()
