"""
etl_bronze.py
─────────────────────────────────────────────────────────────────
BRONZE layer — standalone script. Lands the raw CSV into
youtube_videos_bronze almost unmodified (schema applied, audit
columns added, no cleaning, no dedup, append-only).

Must sit in the SAME folder as etl_common.py (it imports from it).

    spark-submit --master "local[*]" \\
        --packages io.delta:delta-spark_2.13:4.0.0 \\
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \\
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \\
        etl_bronze.py --input /home/jatin/bigdata/test-jupyter/youtube_social_data_enriched.csv
"""

import argparse
import sys

from etl_common import get_spark, ingest_bronze


def parse_args():
    parser = argparse.ArgumentParser(description="BRONZE layer: raw CSV -> youtube_videos_bronze")
    parser.add_argument("--input", required=True,
                         help="Path to the raw source CSV (local, HDFS, or S3 path).")
    parser.add_argument("--force-local-warehouse", action="store_true",
                         help="Use local warehouse even if HDFS appears available (for testing).")
    return parser.parse_args()


def main():
    args = parse_args()
    spark = get_spark(force_local=args.force_local_warehouse)
    bronze_df = None
    try:
        bronze_df = ingest_bronze(spark, args.input)
        print("\n[BRONZE] Ingest complete. Run etl_silver.py next.")
    except Exception as e:
        print(f"\n[BRONZE] Failed: {e}", file=sys.stderr)
        raise
    finally:
        if bronze_df is not None and bronze_df.is_cached:
            bronze_df.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()