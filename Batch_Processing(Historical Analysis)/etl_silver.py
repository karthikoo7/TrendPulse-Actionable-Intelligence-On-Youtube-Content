"""
etl_silver.py
─────────────────────────────────────────────────────────────────
SILVER layer — standalone script. Reads the bronze table, de-dupes,
fixes types, normalizes the keyword field, engineers derived columns
(engagement_rate, performance_tier, etc.), and writes to
youtube_videos_clean.

Must sit in the SAME folder as etl_common.py (it imports from it).
Must be run AFTER etl_bronze.py has landed at least one batch.

    spark-submit --master "local[*]" \\
        --packages io.delta:delta-spark_2.13:4.0.0 \\
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \\
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \\
        etl_silver.py --drop-legacy-constraints
"""

import argparse
import sys

from etl_common import get_spark, read_bronze, run_silver_stage


def parse_args():
    parser = argparse.ArgumentParser(description="SILVER layer: youtube_videos_bronze -> youtube_videos_clean")
    parser.add_argument("--drop-legacy-constraints", action="store_true",
                         help="Auto-drop any hardcoded keyword CHECK constraint found on "
                              "youtube_videos_clean before writing (see etl_common.py docstring).")
    parser.add_argument("--force-local-warehouse", action="store_true",
                         help="Use local warehouse even if HDFS appears available (for testing).")
    return parser.parse_args()


def main():
    args = parse_args()
    spark = get_spark(force_local=args.force_local_warehouse)
    clean_df = None
    try:
        bronze_df = read_bronze(spark)
        clean_df = run_silver_stage(spark, bronze_df, args.drop_legacy_constraints)
        print("\n[SILVER] Clean/load complete. Run etl_gold.py next.")
    except Exception as e:
        print(f"\n[SILVER] Failed: {e}", file=sys.stderr)
        raise
    finally:
        if clean_df is not None and clean_df.is_cached:
            clean_df.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()