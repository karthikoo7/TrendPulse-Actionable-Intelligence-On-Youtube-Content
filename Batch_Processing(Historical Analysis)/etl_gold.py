"""
etl_gold.py
─────────────────────────────────────────────────────────────────
GOLD layer — standalone script. Reads the silver table (optionally
keyword/min-views filtered), runs outlier/correlation statistics,
builds the keyword/category summary (youtube_keyword_summary),
appends a snapshot to youtube_videos_history, and exports the final
CSV (the file Tableau reads from).

Must sit in the SAME folder as etl_common.py (it imports from it).
Must be run AFTER etl_silver.py has written youtube_videos_clean.

    spark-submit --master "local[*]" \\
        --packages io.delta:delta-spark_2.13:4.0.0 \\
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \\
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \\
        etl_gold.py --drop-legacy-constraints \\
        --output /home/jatin/bigdata/test-jupyter/youtube_analysis_final.csv \\
        --min-views 100

# No --keyword passed above -> every keyword is processed (see etl_common's
# read_from_hive(): the keyword predicate is only applied if --keyword is set).
# Export straight to S3 or HDFS instead of local disk:
#   --output s3a://my-bucket/youtube/analysis_final.csv
#   --output hdfs:///user/jatin/output/youtube_analysis_final.csv
"""

import argparse
import sys

from etl_common import get_spark, run_gold_stage


def parse_args():
    parser = argparse.ArgumentParser(description="GOLD layer: youtube_videos_clean -> summary + CSV export")
    parser.add_argument("--output", required=True,
                         help="Path for the final CSV export — local, hdfs://, or s3a:// path.")
    parser.add_argument("--keyword", type=str, default=None,
                         help="Optional keyword(s) to filter on, comma-separated. Omit to process "
                              "every keyword.")
    parser.add_argument("--keyword-mode", choices=["exact", "contains"], default="exact",
                         help="'exact' (default) or 'contains' substring matching for --keyword.")
    parser.add_argument("--min-views", type=int, default=100,
                         help="Minimum view_count to keep during the Hive re-read (default: 100).")
    parser.add_argument("--drop-legacy-constraints", action="store_true",
                         help="Auto-drop any hardcoded keyword CHECK constraint found on "
                              "youtube_keyword_summary before writing.")
    parser.add_argument("--force-local-warehouse", action="store_true",
                         help="Use local warehouse even if HDFS appears available (for testing).")
    return parser.parse_args()


def main():
    args = parse_args()
    spark = get_spark(force_local=args.force_local_warehouse)
    summary_df = None
    try:
        summary_df = run_gold_stage(spark, args)
        print("\n[GOLD] Analyze/export complete.")
        print(f"[GOLD] Warehouse used: {spark.conf.get('spark.sql.warehouse.dir')}")
    except Exception as e:
        print(f"\n[GOLD] Failed: {e}", file=sys.stderr)
        raise
    finally:
        if summary_df is not None and summary_df.is_cached:
            summary_df.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()