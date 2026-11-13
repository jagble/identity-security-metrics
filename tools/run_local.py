"""
Run the Databricks notebooks on a laptop with local Spark, to test changes before uploading them.

    pip install pyspark==3.5.3 scikit-learn pandas
    python data/generate_pds_identity_data.py --out data/output
    python tools/run_local.py --raw data/output

Notebook cells marked "# MAGIC" (markdown, %run, %pip) are skipped; 00_config runs first instead of %run.
Tables are written as Parquet to a local warehouse folder instead of Delta.
"""
import argparse
import os
import sys

from pyspark.sql import SparkSession

ORDER = ["00_config", "01_bronze_ingest", "02_silver_clean_and_quality", "03_gold_metrics", "04_anomaly_detection"]


def code_of(path):
    keep = []
    for line in open(path, encoding="utf-8"):
        if line.startswith("# MAGIC") or line.startswith("# COMMAND") or line.startswith("# Databricks notebook"):
            continue
        keep.append(line)
    return "".join(keep)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", required=True, help="folder with the generated CSVs")
    ap.add_argument("--warehouse", default="local-warehouse")
    ap.add_argument("--only", nargs="*", help="run only these notebooks (00_config always runs)")
    a = ap.parse_args()

    spark = (SparkSession.builder.master("local[*]").appName("pds-identity-metrics")
             .config("spark.sql.warehouse.dir", os.path.abspath(a.warehouse))
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.ui.enabled", "false")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    def display(df, n=30):
        df.show(n, truncate=60)

    here = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "notebooks")
    for nb in ORDER:
        if a.only and nb != "00_config" and nb not in a.only:
            continue
        print(f"\n{'=' * 20} {nb} {'=' * 20}")
        ns = {"spark": spark, "display": display, "LOCAL_SCHEMA": "pds_identity",
              "LOCAL_RAW_PATH": os.path.abspath(a.raw), "__name__": "__main__"}
        exec(compile(code_of(os.path.join(here, "00_config.py")), "00_config", "exec"), ns)
        if nb != "00_config":
            exec(compile(code_of(os.path.join(here, f"{nb}.py")), nb, "exec"), ns)
    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
