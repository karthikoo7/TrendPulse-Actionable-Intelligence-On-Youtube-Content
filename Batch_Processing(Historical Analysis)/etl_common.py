"""
etl_common.py
─────────────────────────────────────────────────────────────────
SHARED LIBRARY MODULE for the split medallion pipeline. Not runnable
on its own -- it has no argparse/main/__main__ block. Import from it
in etl_bronze.py / etl_silver.py / etl_gold.py instead.

This is what used to be etl_spark_new.py's --stage bronze/silver/gold
single-script pipeline. It's now split into three standalone scripts
that each do ONE layer, so they can be scheduled/retried/run
independently -- but they still share one Spark-session builder,
warehouse-permission handling, legacy-CHECK-constraint detection, the
CSV schema, transform logic, and Hive read/write helpers, all defined
here ONCE so bronze/silver/gold never drift out of sync with each other.

Deploy this file into the SAME folder as etl_bronze.py / etl_silver.py
/ etl_gold.py (e.g. ~/bigdata/test-jupyter/) -- spark-submit runs each
script as a plain Python file, and Python resolves `import etl_common`
via the script's own directory, so they need to be siblings on disk.

Requires the Delta Lake Spark package. The package version AND Scala suffix
must match your Spark build exactly, or you'll hit Scala binary-incompatibility
errors (NoClassDefFoundError: scala/Serializable, scala/collection/...):

    Spark version   ->  Scala   ->  correct package
    Spark 3.5.x     ->  2.12    ->  io.delta:delta-spark_2.12:3.2.0
    Spark 4.0.x     ->  2.13    ->  io.delta:delta-spark_2.13:4.0.0

Check your Spark version with `spark-submit --version` before choosing.
This module is currently pinned to Spark 4.0 / Delta 4.0.0.

# IMPORTANT — legacy hardcoded keyword CHECK constraint:
# An earlier point in this project added a hand-maintained CHECK constraint
# on youtube_videos_clean (a hardcoded whitelist of ~300 keyword strings,
# baked into the Delta table's own metadata via ALTER TABLE ... ADD
# CONSTRAINT). That whitelist has a typo ("directors cut" instead of
# "director's cut") and can never be kept in sync with organically-generated
# keywords from 8 platforms. check_and_handle_legacy_constraints() below
# detects that constraint on startup and, after attempting to drop it
# (--drop-legacy-constraints), RE-VERIFIES it is actually gone before
# proceeding to write -- if it reappears, the pipeline fails LOUDLY with a
# clear message instead of silently writing and hitting the cryptic Delta
# error three steps later.
"""

import sys
import os
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, BooleanType, TimestampType
)

# ── CONFIG ──────────────────────────────────────────────────────
# ── MEDALLION ARCHITECTURE TABLE NAMES ──────────────────────────
# BRONZE : raw, append-only landing zone. Near-zero transformation --
#          schema applied so types are correct, plus audit columns, but
#          NO dedup / NO null-handling / NO business logic. Every run's
#          data lands here so silver/gold can always be rebuilt from
#          bronze without re-reading the original source file.
# SILVER : cleaned, de-duplicated, conformed data. This is what the
#          codebase has historically called RAW_TABLE (the name predates
#          the medallion split) -- it is the SILVER table.
# GOLD   : business-level aggregates, ready for reporting/BI/export.
HIVE_DB             = "youtube_analytics"
BRONZE_TABLE        = "youtube_videos_bronze"          # BRONZE layer — raw landing zone
RAW_TABLE           = "youtube_videos_clean"           # SILVER layer — cleaned & conformed (legacy name)
HISTORY_TABLE       = "youtube_videos_history"         # GOLD layer — historical run snapshots
KEYWORD_AGG_TABLE   = "youtube_keyword_summary"        # GOLD layer — keyword/category aggregates

#  CRITICAL: User-writable fallback paths (adjust these for your environment)
LOCAL_WAREHOUSE_FALLBACK = os.path.expanduser("~/spark-warehouse")  # Local dev fallback
HDFS_WAREHOUSE_PRIMARY   = "/user/hive/warehouse"              # Standard Hive warehouse

# Outlier detection tuning (IQR method)
IQR_MULTIPLIER = 1.5


# --- SPARK SESSION (Hive enabled with SMART WAREHOUSE HANDLING) ----------------------------
def get_spark(force_local: bool = False):
    """Creates Spark session with automatic warehouse permission handling"""

    # Determine appropriate warehouse directory
    warehouse_dir = determine_warehouse_dir(force_local)

    print(f"\n[SPARK CONFIG] Using warehouse directory: {warehouse_dir}")

    spark_builder = (
        SparkSession.builder
        .appName("YouTube_ETL_Delta_Pipeline_WITH_HPP")
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.shuffle.partitions", "8")
        #  DELTA LAKE — table format + catalog integration
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0")
        #  CRITICAL HPP OPTIMIZATIONS (Delta inherits Parquet's columnar file format)
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.sql.parquet.columnarReaderBatchSize", "4096")
        # AQE disabled: Spark 4.0 + Delta 4.0.0 has a known issue where AQE's
        # background re-optimization thread doesn't inherit the active
        # SparkSession context, causing PrepareDeltaScan's subquery-transform
        # path to throw "SparkSessionCompanion.active ... internalError"
        # during Delta table analysis. Disabling AQE avoids that background
        # thread entirely. Revisit if you upgrade to a Delta/Spark release
        # that's confirmed to have fixed this.
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
        .config("spark.sql.adaptive.skewJoin.enabled", "false")
        #  SET WAREHOUSE DIRECTORY (KEY FIX FOR PERMISSION ERRORS)
        .config("spark.sql.warehouse.dir", warehouse_dir)
        #  S3A SUPPORT — reads credentials from env vars, no-op if unset
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", ""))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", ""))
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("AWS_S3_ENDPOINT", "s3.amazonaws.com"))
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .enableHiveSupport()
    )

    spark = spark_builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Verify warehouse permissions immediately
    verify_warehouse_permissions(spark, warehouse_dir)

    return spark


def determine_warehouse_dir(force_local: bool = False) -> str:
    """
    Intelligently select warehouse directory based on environment permissions
    Returns: First writable path from priority list
    """
    # Priority 1: Explicit local override (for testing)
    if force_local:
        return _ensure_dir_exists(LOCAL_WAREHOUSE_FALLBACK, is_local=True)
    # Priority 2: Try HDFS primary warehouse (if we can write)
    if _is_hdfs_path_available(HDFS_WAREHOUSE_PRIMARY):
        hdfs_path = _ensure_dir_exists(HDFS_WAREHOUSE_PRIMARY, is_local=False)
        if _test_hdfs_write_permissions(hdfs_path):
            return hdfs_path
        else:
            print(f"[WARN] HDFS warehouse {HDFS_WAREHOUSE_PRIMARY} exists but no write permissions")

    # Priority 3: Fallback to local warehouse (guaranteed writable)
    print(f"[INFO] Falling back to local warehouse: {LOCAL_WAREHOUSE_FALLBACK}")
    return _ensure_dir_exists(LOCAL_WAREHOUSE_FALLBACK, is_local=True)


def _is_hdfs_path_available(path: str) -> bool:
    """Check if path appears to be HDFS (not local filesystem)"""
    return path.startswith("/user/") or path.startswith("hdfs://") or path.startswith("maprfs://")


def _ensure_dir_exists(path: str, is_local: bool = False) -> str:
    """Create directory if missing, with appropriate permissions"""
    try:
        if is_local:
            Path(path).mkdir(parents=True, exist_ok=True)
            # Ensure local directory is writable
            os.chmod(path, 0o755)
        else:
            # For HDFS, we'd need Hadoop CLI - but we'll test via Spark later
            pass
        return path
    except Exception as e:
        print(f"[WARN] Could not create directory {path}: {str(e)}")
        raise


def _test_hdfs_write_permissions(hdfs_path: str) -> bool:
    """Test if we can write to HDFS path (using Spark's Hadoop API)"""
    # This will be verified in verify_warehouse_permissions() after Spark session starts
    return True  # Optimistic assumption - verified later


def verify_warehouse_permissions(spark: SparkSession, warehouse_dir: str):
    """Fail fast with clear message if warehouse isn't writable"""
    try:
        # Try to create a test table in the warehouse
        test_table = f"__spark_warehouse_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        (
            spark.createDataFrame([(1, "test")], ["id", "value"])
            .write
            .mode("overwrite")
            .saveAsTable(test_table)
        )

        #  FIXED: Corrected SQL syntax (removed duplicate "TABLE IF EXISTS")
        spark.sql(f"DROP TABLE IF EXISTS {test_table}")
        print(f"[SUCCESS] Warehouse directory is writable: {warehouse_dir}")

    except Exception as e:
        error_msg = str(e)
        if "Permission denied" in error_msg or "Mkdirs failed" in error_msg:
            print(f"\n CRITICAL: Warehouse permission error!")
            print(f"   Attempted path: {warehouse_dir}")
            print(f"   Error: {error_msg}")
            print("\n SOLUTIONS:")
            print("   1. For local development: Use --force-local-warehouse flag")
            print("   2. For cluster deployment: Ask admin to grant write permissions to:")
            print(f"      hdfs dfs -chmod -R 775 {warehouse_dir}")
            print(f"      hdfs dfs -chown -R $(whoami):{warehouse_dir}")
            print("   3. Or configure proper Hive metastore with shared permissions")
            raise RuntimeError(f"Warehouse not writable: {warehouse_dir}") from e
        else:
            # Re-raise non-permission errors
            raise


# --- LEGACY CONSTRAINT DETECTION (fixes DELTA_VIOLATE_CONSTRAINT_WITH_VALUES) ---
def get_legacy_constraints(spark: SparkSession, full_table: str):
    """
    Returns a list of (constraint_name, constraint_expression) tuples for any
    delta.constraints.* table properties found on `full_table`. Empty list if
    the table doesn't exist or has none.
    """
    if not spark.catalog.tableExists(full_table):
        return []
    props = spark.sql(f"SHOW TBLPROPERTIES {full_table}").collect()
    return [
        (r["key"].split("delta.constraints.", 1)[1], r["value"])
        for r in props if r["key"].startswith("delta.constraints.")
    ]


def check_and_handle_legacy_constraints(spark: SparkSession, full_table: str, auto_drop: bool = False):
    """
    Detects hand-added Delta CHECK constraints stored as table properties
    (delta.constraints.<name>) on `full_table`.

    Why this exists: a CHECK constraint like
        keyword IN ('10-minute meal prep', ..., 'directors cut', ...)
    is NOT generated anywhere in this script's Python code -- the only
    keyword-based predicate this pipeline builds is the per-batch
    `replaceWhere` scoping in load_to_hive(), via _sql_quote_list(). A
    hardcoded whitelist CHECK constraint like the one above must have been
    added directly against the table (e.g. manually, or by a separate
    provisioning script/notebook elsewhere in the project), and it lives
    permanently in the Delta table's own transaction log -- independent of
    whatever this script does on each run.

    Because that whitelist can never track every legitimate keyword produced
    across 8 platforms (and already has at least one typo -- "directors cut"
    vs. the correct "director's cut"), it will keep throwing
    DELTA_VIOLATE_CONSTRAINT_WITH_VALUES / DELTA_REPLACE_WHERE_MISMATCH
    forever on any keyword not in the list. It has to be removed at the
    table level, and — critically — VERIFIED as actually removed, since if
    something else (a separate setup script, a scheduled provisioning job,
    a Databricks notebook writing to the same underlying table path) is
    re-adding it, a silent drop-and-proceed would just hit the same error
    three steps later with a much more confusing stack trace.
    """
    constraints = get_legacy_constraints(spark, full_table)

    if not constraints:
        print(f"[PRECHECK] {full_table}: no CHECK constraints found. Nothing to do.")
        return

    for name, expr in constraints:
        preview = expr[:120] + ("..." if len(expr) > 120 else "")
        print(f"[PRECHECK] {full_table}: found constraint '{name}': {preview}")

        if not auto_drop:
            raise RuntimeError(
                f"\nFound a hardcoded CHECK constraint '{name}' on {full_table} that is NOT "
                f"generated by this script (this script only ever builds a per-batch `replaceWhere` "
                f"predicate, never a permanent table CHECK constraint). This looks like a hand-"
                f"maintained keyword whitelist, which will keep breaking on any new or differently-"
                f"spelled/punctuated keyword this pipeline legitimately produces (e.g. \"director's "
                f"cut\" vs a constraint listing \"directors cut\").\n\n"
                f"Re-run with --drop-legacy-constraints to remove it automatically, or drop it "
                f"manually first:\n"
                f"    spark.sql(\"ALTER TABLE {full_table} DROP CONSTRAINT {name}\")\n"
            )

        print(f"[PRECHECK] {full_table}: --drop-legacy-constraints set -> dropping '{name}'")
        spark.sql(f"ALTER TABLE {full_table} DROP CONSTRAINT {name}")

    #  INVALIDATE CACHED METADATA: ALTER TABLE updates the Delta transaction
    # log on disk, but Spark can hold a cached DeltaLog/catalog snapshot for
    # this table path within the session. Without an explicit refresh, a
    # later .saveAsTable() on the SAME session can still plan against the
    # stale pre-drop snapshot and enforce a constraint that's technically
    # already gone -- producing the exact "dropped it, re-verified it's
    # gone, then the write fails on it anyway" symptom.
    spark.catalog.refreshTable(full_table)

    #  RE-VERIFY after attempting to drop everything found. If this recurs
    # (e.g. because a separate provisioning script re-adds it before every
    # run), fail loudly and clearly RIGHT HERE instead of proceeding to the
    # write and surfacing the cryptic Delta stack trace three steps later.
    remaining = get_legacy_constraints(spark, full_table)
    if remaining:
        remaining_names = [n for n, _ in remaining]
        raise RuntimeError(
            f"\nAttempted to drop legacy CHECK constraint(s) on {full_table}, but "
            f"{remaining_names} are STILL present immediately after the drop. This strongly "
            f"suggests something else is re-adding this constraint -- e.g. a separate table "
            f"provisioning script/notebook that runs 'ALTER TABLE ... ADD CONSTRAINT' before "
            f"this pipeline, or a concurrent job (Databricks, another spark-submit) writing to "
            f"the same underlying Delta table path.\n\n"
            f"Search your project for the source of this constraint before re-running, e.g.:\n"
            f"    grep -rn \"ADD CONSTRAINT\" /home/jatin/bigdata/ --include=\"*.py\"\n"
            f"    grep -rln \"ADD CONSTRAINT\" /home/jatin/bigdata/ --include=\"*.ipynb\"\n"
        )

    print(f"[PRECHECK] {full_table}: confirmed no CHECK constraints remain. Safe to write.")


def _align_for_insert_into(spark: SparkSession, full_table: str, df: DataFrame) -> DataFrame:
    """
    Reconciles `df`'s schema against `full_table`'s current schema before an
    .insertInto() call.

    WHY THIS IS NEEDED: unlike .saveAsTable(), .insertInto() resolves columns
    BY POSITION, not by name, and it does NOT auto-evolve the target table's
    schema even with mergeSchema=true set on the writer. If the table was
    created by an earlier run whose DataFrame had fewer/different columns
    (e.g. `outlier_video_count` only appears when outlier detection actually
    ran, which depends on the data), a later run whose DataFrame has an
    extra column fails with INSERT_COLUMN_ARITY_MISMATCH -- and even if the
    column counts happened to match by coincidence, mismatched ORDER would
    silently insert values into the wrong columns, which is worse than an
    error.

    This function:
      1. Adds any column present in `df` but missing from the table via
         ALTER TABLE ... ADD COLUMNS (explicit schema evolution -- the thing
         mergeSchema was supposed to do automatically but doesn't for
         insertInto).
      2. Reorders/selects `df`'s columns to exactly match the table's
         column order, filling any table column absent from `df` with NULL
         so the positional insert lines up correctly either way.
    """
    target_cols = spark.table(full_table).columns
    df_cols = df.columns

    missing_in_table = [c for c in df_cols if c not in target_cols]
    if missing_in_table:
        df_types = dict(df.dtypes)
        add_cols_sql = ", ".join(f"`{c}` {df_types[c]}" for c in missing_in_table)
        print(f"[SCHEMA] {full_table} is missing column(s) present in this batch: "
              f"{missing_in_table}. Adding via ALTER TABLE ... ADD COLUMNS.")
        spark.sql(f"ALTER TABLE {full_table} ADD COLUMNS ({add_cols_sql})")
        spark.catalog.refreshTable(full_table)
        target_cols = spark.table(full_table).columns

    missing_in_df = [c for c in target_cols if c not in df_cols]
    if missing_in_df:
        print(f"[SCHEMA] This batch is missing column(s) present in {full_table}: "
              f"{missing_in_df}. Filling with NULL for this write.")

    # Reorder to match the table's column order EXACTLY -- insertInto is
    # positional, so this is what actually makes the write land correctly,
    # not just a cosmetic reordering.
    select_exprs = [
        F.col(c) if c in df_cols else F.lit(None).alias(c)
        for c in target_cols
    ]
    return df.select(*select_exprs)


def _is_legacy_constraint_failure(exc: Exception) -> bool:
    """
    True if `exc` looks like a Delta write failure caused by a hardcoded
    CHECK constraint / replaceWhere mismatch (DELTA_VIOLATE_CONSTRAINT_WITH_VALUES
    / DELTA_REPLACE_WHERE_MISMATCH), as opposed to some unrelated write failure.
    """
    msg = str(exc)
    return (
        "DELTA_VIOLATE_CONSTRAINT_WITH_VALUES" in msg
        or "DELTA_REPLACE_WHERE_MISMATCH" in msg
        or "CHECK constraint" in msg
    )


def _write_with_constraint_retry(spark: SparkSession, full_table: str,
                                  write_fn, drop_legacy_constraints: bool):
    """
    Runs `write_fn()` (a zero-arg closure that performs the actual
    `.saveAsTable(full_table)` call). If it fails with what looks like a
    legacy CHECK-constraint violation, this means the constraint was
    re-added by something else AFTER our precheck already ran (a race,
    not a logic bug -- see check_and_handle_legacy_constraints() docstring).

    In that case: re-run the precheck/drop/re-verify once more and retry
    the write exactly once. If the retry also fails, or auto_drop is off,
    raise a single clear error instead of looping or surfacing the raw
    Delta stack trace.
    """
    try:
        write_fn()
        return
    except Exception as e:
        if not _is_legacy_constraint_failure(e):
            raise  # unrelated failure -- don't mask it, just propagate

        if not drop_legacy_constraints:
            raise RuntimeError(
                f"\nWrite to {full_table} failed because a legacy CHECK constraint is "
                f"present -- it was NOT there when this script last checked, so something "
                f"else re-added it between the precheck and this write. Re-run with "
                f"--drop-legacy-constraints so this can be cleared automatically, e.g.:\n"
                f"    --drop-legacy-constraints\n"
            ) from e

        print(f"\n[RETRY] Write to {full_table} hit a legacy CHECK constraint that appeared "
              f"AFTER the initial precheck (classic re-provisioning race condition -- see "
              f"check_and_handle_legacy_constraints() docstring). Re-checking and retrying "
              f"the write once...")
        check_and_handle_legacy_constraints(spark, full_table, auto_drop=True)
        #  Belt-and-suspenders: refreshTable() also runs inside the drop
        # path above, but refresh again here immediately before the retried
        # write closes any remaining window between re-verification and the
        # write actually executing.
        spark.catalog.refreshTable(full_table)

        try:
            write_fn()
            print(f"[RETRY] Write to {full_table} succeeded after dropping the re-added constraint.")
        except Exception as e2:
            if not _is_legacy_constraint_failure(e2):
                raise
            raise RuntimeError(
                f"\nWrite to {full_table} failed again with a legacy CHECK constraint even "
                f"after dropping it and retrying once. Something is actively re-adding this "
                f"constraint on every run (e.g. a scheduled provisioning job, a notebook cell "
                f"that runs on a timer, or a concurrent spark-submit writing to the same "
                f"underlying Delta table path). Find and stop/fix that source before "
                f"re-running this pipeline, e.g.:\n"
                f"    grep -rn \"ADD CONSTRAINT\" /home/jatin/bigdata/ --include=\"*.py\"\n"
                f"    grep -rln \"ADD CONSTRAINT\" /home/jatin/bigdata/ --include=\"*.ipynb\"\n"
            ) from e2


# --- EXPLICIT SCHEMA FOR CSV READING (matches scraper's FIELDNAMES) -------------
def get_csv_schema():
    """Schema matching the YouTube scraper's CSV (19 columns - NO etl_date)"""
    return StructType([
        StructField("video_id",          StringType(),  True),
        StructField("title",             StringType(),  True),
        StructField("channel",           StringType(),  True),
        StructField("published_at",      StringType(),  True),   # parsed to timestamp later
        StructField("keyword",           StringType(),  True),
        StructField("platform",          StringType(),  True),
        StructField("view_count",        LongType(),    True),
        StructField("like_count",        LongType(),    True),
        StructField("comment_count",     LongType(),    True),
        StructField("description",       StringType(),  True),
        StructField("duration_seconds",  IntegerType(), True),
        StructField("duration_category", StringType(),  True),
        StructField("category_id",       StringType(),  True),
        StructField("category_name",     StringType(),  True),
        StructField("tags",              StringType(),  True),
        StructField("tag_count",         IntegerType(), True),
        StructField("has_captions",      StringType(),  True),   # cast to boolean later
        StructField("definition",        StringType(),  True),
        StructField("licensed_content",  StringType(),  True),   # cast to boolean later
    ])


# ── 0. BRONZE LAYER — RAW LANDING ZONE ──────────────────────────
def ingest_bronze(spark: SparkSession, input_path: str) -> DataFrame:
    """
    BRONZE LAYER: lands the raw CSV into a Delta table with almost no
    transformation. This is the "as received" copy of the source data.

    Deliberately does NOT do:
      - de-duplication
      - null-handling
      - keyword normalization
      - any business logic at all

    It DOES:
      - apply the explicit schema (get_csv_schema()), so column TYPES are
        correct from the earliest possible point -- letting Spark guess
        types from a raw CSV is a common source of silent data corruption.
      - stamp every row with _ingested_at and _source_file audit columns,
        so you can always trace a row back to which run/file it came from.

    Bronze is written in APPEND mode (not overwrite): every run's data
    accumulates here, forming a permanent, replayable record of everything
    ever ingested. This is what makes it possible to rebuild silver/gold
    from scratch later (e.g. after fixing a bug in transform()) without
    needing the original CSV file again.
    """
    print(f"\n[BRONZE] Ingesting raw CSV from: {input_path}")
    df = (
        spark.read
        .option("header", "true")
        .option("multiLine", "true")
        .option("escape", '"')
        .schema(get_csv_schema())
        .csv(input_path)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(input_path))
    )
    df = df.cache()
    row_count = df.count()
    print(f"[BRONZE] Raw row count: {row_count}")

    full_table = f"{HIVE_DB}.{BRONZE_TABLE}"
    (
        df.write
        .format("delta")
        .mode("append")  # bronze is append-only across runs — see docstring
        .option("mergeSchema", "true")
        .saveAsTable(full_table)
    )
    print(f"[BRONZE] Landed {row_count} rows into {full_table} (append mode).")
    return df


def read_bronze(spark: SparkSession) -> DataFrame:
    """
    Reads the full bronze table back, for use when SILVER is run as its
    own standalone stage (--stage silver) rather than as part of --stage all,
    where the in-memory bronze DataFrame is passed straight through instead.
    """
    full_table = f"{HIVE_DB}.{BRONZE_TABLE}"
    print(f"\n[SILVER] Reading bronze layer from: {full_table}")
    if not spark.catalog.tableExists(full_table):
        raise RuntimeError(
            f"{full_table} does not exist yet. Run --stage bronze (or --stage all) "
            f"at least once before running --stage silver on its own."
        )
    df = spark.table(full_table)
    print(f"[SILVER] Bronze row count available: {df.count()}")
    return df


# ── 2. TRANSFORM / PREPROCESS (nulls, dtypes, disparities, HPP-ready columns) ──
def transform(df: DataFrame) -> DataFrame:
    print("\nTRANSFORM Cleaning and preprocessing data...")

    df_clean = (
        df
        # drop rows with no video_id (corrupt/empty rows)
        .filter(F.col("video_id").isNotNull() & (F.trim(F.col("video_id")) != ""))
        # de-duplicate on video_id, keep first occurrence
        .dropDuplicates(["video_id"])
        # cast string boolean-like fields to real booleans (dtype fix)
        .withColumn("has_captions", F.col("has_captions").cast("boolean"))
        .withColumn("licensed_content", F.col("licensed_content").cast("boolean"))
        # parse published_at string -> timestamp (dtype fix)
        .withColumn("published_at_ts", F.to_timestamp("published_at"))
        .withColumn("published_year", F.year("published_at_ts"))
        .withColumn("published_month", F.month("published_at_ts"))
        # HPP OPTIMIZATION: Add processing date (ETL date) for time-based pruning
        .withColumn("etl_date", F.current_date().cast("string"))
        # fill nulls in numeric metrics with 0 (missing stats != negative engagement)
        .fillna({
            "view_count": 0,
            "like_count": 0,
            "comment_count": 0,
            "tag_count": 0,
            "duration_seconds": 0,
        })
        # normalize keyword field so keyword-based pushdown filters match reliably
        .withColumn("keyword", F.lower(F.trim(F.col("keyword"))))
        #  Normalize apostrophe variants (curly ', backtick `, etc.) to a
        # single straight apostrophe, so "director's cut" from one platform
        # doesn't silently diverge from "director's cut" from another.
        .withColumn("keyword", F.regexp_replace(F.col("keyword"), "[\u2018\u2019\u02BC`]", "'"))
        # normalize text fields (disparity fix — trims/whitespace inconsistencies)
        .withColumn("title", F.trim(F.col("title")))
        .withColumn("channel", F.trim(F.col("channel")))
        .withColumn("category_name", F.coalesce(F.col("category_name"), F.lit("Unknown")))
        # remove obviously broken rows (negative views, zero-length video ids etc.)
        .filter(F.col("view_count") >= 0)
    )

    # ── engineered / derived features ──
    df_features = (
        df_clean
        .withColumn(
            "engagement_rate",
            F.when(F.col("view_count") > 0,
                   F.round((F.col("like_count") + F.col("comment_count")) / F.col("view_count"), 6))
             .otherwise(F.lit(0.0))
        )
        .withColumn(
            "like_to_comment_ratio",
            F.round(F.try_divide(F.col("like_count"), F.col("comment_count")), 2)
        )
        .withColumn(
            "performance_tier",
            F.when(F.col("view_count") >= 1_000_000, "Viral")
             .when(F.col("view_count") >= 100_000,   "High")
             .when(F.col("view_count") >= 10_000,    "Medium")
             .otherwise("Low")
        )
        .withColumn("etl_processed_at", F.current_timestamp())
    )

    print(f"TRANSFORM Cleaned row count: {df_features.count()}")
    return df_features


# -- 3. ANALYSIS (pre-load) --------------------------------------
def analyze_pre_load(df: DataFrame):
    print("\n ANALYZE Top 10 keywords by average engagement rate:")
    df.groupBy("keyword").agg(
        F.round(F.avg("engagement_rate"), 4).alias("avg_engagement_rate"),
        F.count("*").alias("video_count")
    ).orderBy(F.desc("avg_engagement_rate")).show(10, truncate=False)

    print("[ANALYZE] Video count by duration category:")
    df.groupBy("duration_category").count().orderBy(F.desc("count")).show()


# -- 4. LOAD -> HIVE (parquet) WITH HPP OPTIMIZATIONS -------------
def load_to_hive(spark: SparkSession, df: DataFrame, drop_legacy_constraints: bool = False):
    """
    Writes cleaned data as a Delta table, partitioned by keyword.

    Uses Delta's `replaceWhere` to scope the overwrite to only the keyword
    partitions present in this batch — so re-running the pipeline for one
    keyword no longer wipes out data already loaded for other keywords.

    IMPORTANT: `df` must already be materialized (e.g. `.cache()` + an action
    called on it) before this function runs, so the keyword list collected
    for `replaceWhere` matches exactly what gets written.
    """
    warehouse_dir = spark.conf.get("spark.sql.warehouse.dir")
    full_table = f"{HIVE_DB}.{RAW_TABLE}"
    print(f"\n[LOAD] Writing cleaned data to Delta table: {full_table}")
    print(f"[LOAD] Using warehouse: {warehouse_dir} (partitioned by keyword)")

    #  PRECHECK + RE-VERIFY: catch any hand-added hardcoded CHECK constraint
    # on the table BEFORE attempting the write. This now raises immediately
    # (with a clear diagnostic pointing at possible re-provisioning sources)
    # if a constraint is found and either can't be dropped, or reappears
    # right after being dropped -- rather than let the write proceed and
    # surface a deep DELTA_VIOLATE_CONSTRAINT_WITH_VALUES stack trace.
    check_and_handle_legacy_constraints(spark, full_table, auto_drop=drop_legacy_constraints)

    table_exists = spark.catalog.tableExists(full_table)

    keywords_in_batch = sorted(
        r["keyword"] for r in df.select("keyword").distinct().collect() if r["keyword"]
    )

    if table_exists and keywords_in_batch:
        # Scope the overwrite to just this batch's keyword partitions —
        # any other keyword partitions already in the table are left untouched.
        #
        #  IMPORTANT: this MUST use .insertInto(), not .saveAsTable(). Under
        # a v2-enabled catalog (spark.sql.catalog.spark_catalog is overridden
        # to DeltaCatalog here), calling .saveAsTable() with mode("overwrite")
        # on a table that already exists routes through Spark's
        # "REPLACE TABLE AS SELECT" plan (CreateDeltaTableCommand /
        # AtomicReplaceTableAsSelectExec) instead of a scoped partial
        # overwrite. That path does NOT correctly honor `replaceWhere` as a
        # partition-scoping predicate -- it still runs the DeltaInvariantChecker
        # against it, and legitimately-included values (e.g. a keyword
        # containing an apostrophe, correctly SQL-escaped) can spuriously fail
        # with DELTA_VIOLATE_CONSTRAINT_WITH_VALUES / DELTA_REPLACE_WHERE_MISMATCH
        # even though nothing is actually wrong with the data or the escaping.
        # .insertInto() instead goes through OverwriteByExpressionExec, which
        # is the code path `replaceWhere` is actually designed for.
        quoted = _sql_quote_list(keywords_in_batch)
        replace_where = f"keyword IN ({quoted})"
        print(f"[LOAD] Scoping Delta overwrite to keyword partition(s): {keywords_in_batch}")

        def _write():
            aligned_df = _align_for_insert_into(spark, full_table, df)
            (
                aligned_df.write
                .format("delta")
                .mode("overwrite")
                .option("replaceWhere", replace_where)
                .option("mergeSchema", "true")
                .insertInto(full_table)
            )
    else:
        print("[LOAD] No existing table or no keywords in batch — writing a fresh Delta table.")

        def _write():
            (
                df.write
                .format("delta")
                .mode("overwrite")
                .partitionBy("keyword")  #  PARTITION BY KEYWORD ONLY — enables keyword pruning on re-read
                .option("mergeSchema", "true")
                .saveAsTable(full_table)
            )

    _write_with_constraint_retry(
        spark, full_table,
        write_fn=_write,
        drop_legacy_constraints=drop_legacy_constraints,
    )
    print("[LOAD] Write complete.")


def _sql_quote_list(values) -> str:
    """
    Builds a SQL-safe, comma-separated, single-quoted list for use in an
    IN (...) predicate passed to Spark (e.g. via the Delta `replaceWhere`
    write option, which is parsed with Spark's SQL expression parser).

    IMPORTANT: Spark SQL string literals use C-style BACKSLASH escaping by
    default (spark.sql.parser.escapedStringLiterals), NOT the ANSI-SQL
    convention of doubling an embedded quote. Doubling ('director''s cut')
    is silently mis-parsed inside an IN (...) list in this Spark version --
    verified directly: F.expr("keyword IN ('director''s cut')") does NOT
    match a row containing the string "director's cut", even though the
    doubled-quote form is valid standard SQL. Backslash-escaping the quote
    (and the backslash itself, to be safe) is what Spark's parser actually
    expects, e.g. 'director\\'s cut'.
    """
    escaped = []
    for kw in values:
        kw_escaped = kw.replace("\\", "\\\\").replace("'", "\\'")
        escaped.append(f"'{kw_escaped}'")
    return ", ".join(escaped)


def _parse_keyword_list(keyword: str):
    """
    Turns a raw --keyword string into a clean list of lowercase keywords.
    Accepts a single keyword or a comma-separated list.
    Returns [] if keyword is None/empty.
    """
    if not keyword:
        return []
    return [kw.strip().lower() for kw in keyword.split(",") if kw.strip()]


# -- 5. RE-READ FROM HIVE (KEYWORD PREDICATE PUSHDOWN ONLY) --------------------
def read_from_hive(spark: SparkSession,
                    min_views: int = 0,
                    keyword: str = None,
                    keyword_mode: str = "exact") -> DataFrame:
    """
    Read from Hive with automatic keyword predicate pushdown.
    `keyword` accepts a single keyword or a comma-separated list of keywords
    (e.g. "data engineering,machine learning,big data"). No time-window
    filter is applied — the table is partitioned by keyword only, and the
    keyword filter is the sole partition-pruning predicate.

    `keyword_mode="exact"` (default) matches keyword values exactly and gets
    full partition-pruning benefit. `keyword_mode="contains"` matches any
    keyword partition containing one of the terms as a substring — useful
    when your filter terms are broader than the actual keyword phrases
    stored in the data (e.g. "data engineering" vs. "data engineering
    roadmap") — but it must still scan every partition to check, so it's
    not partition-pruning-friendly.
    """
    print(f"\n[RE-READ] Reading data from Hive table: {HIVE_DB}.{RAW_TABLE}")
    keyword_list = _parse_keyword_list(keyword)
    print(f"[RE-READ] Applying filters: min views={min_views}, "
          f"keyword(s)={keyword_list or 'ALL'}, keyword_mode={keyword_mode}")

    df_hive = spark.table(f"{HIVE_DB}.{RAW_TABLE}")

    #  KEYWORD PUSHDOWN: the only partition-pruning predicate now that the
    #  table is partitioned by keyword. isin() with a list of one behaves
    #  identically to a single equality filter, so this supports both
    #  single- and multi-keyword runs through the same path.
    if keyword_list:
        if keyword_mode == "contains":
            contains_filter = None
            for kw in keyword_list:
                cond = F.col("keyword").contains(kw)
                contains_filter = cond if contains_filter is None else (contains_filter | cond)
            df_hive = df_hive.filter(contains_filter)
        else:
            df_hive = df_hive.filter(F.col("keyword").isin(keyword_list))

    #  Secondary filter on stored column (triggers Parquet zone maps)
    df_hive = df_hive.filter(F.col("view_count") >= min_views)

    row_count = df_hive.count()

    if row_count == 0 and keyword_list:
        # Nothing matched — this is almost always a keyword mismatch, not a
        # real "no data" situation, so surface what keywords DO exist rather
        # than letting downstream stats crash on an empty DataFrame.
        available = [
            r["keyword"] for r in
            spark.table(f"{HIVE_DB}.{RAW_TABLE}").select("keyword").distinct().limit(20).collect()
        ]
        print(f"\n[RE-READ] WARNING: 0 rows matched keyword(s) {keyword_list} "
              f"(mode={keyword_mode}).")
        print(f"[RE-READ] Sample of keywords actually present in the table: {available}")
        raise RuntimeError(
            f"No rows matched --keyword {keyword_list} in {keyword_mode} mode. "
            f"Check the spelling/phrasing against the sample keywords printed above, "
            f"or rerun with --keyword-mode contains for substring matching."
        )

    # Verify pushdown is working (remove in production)
    if spark.conf.get("spark.sql.debug.maxToStringFields") == "100":
        print("[RE-READ] HPP Execution Plan:")
        df_hive.explain(True)  # Shows filtered partitions

    print(f"[RE-READ] Row count from Hive (after keyword pushdown): {row_count}")
    return df_hive


# -- 6. STATISTICAL ANALYSIS (outliers, covariance, relationships) -------------
def statistical_analysis(df: DataFrame) -> DataFrame:
    """
    Runs descriptive statistics, IQR-based outlier detection, and
    correlation/covariance checks between key engagement metrics.
    Returns the input df with an added `is_outlier_views` flag column.
    """
    print("\n[STATS] Descriptive summary (count/mean/stddev/min/quartiles/max):")
    df.select(
        "view_count", "like_count", "comment_count",
        "engagement_rate", "duration_seconds"
    ).summary("count", "mean", "stddev", "min", "25%", "50%", "75%", "max").show(truncate=False)

    # Defensive guard: approxQuantile() returns [] on an empty DataFrame,
    # which would otherwise crash the unpack below. read_from_hive() already
    # raises a clear error before this point when 0 rows match, but this
    # keeps statistical_analysis() safe to call on its own too.
    if df.isEmpty():
        print("[STATS] DataFrame is empty — skipping outlier detection and correlation checks.")
        return df.withColumn("is_outlier_views", F.lit(False))

    # ── Outlier detection on view_count via IQR method ──
    q1, q3 = df.approxQuantile("view_count", [0.25, 0.75], 0.01)
    iqr = q3 - q1
    lower_bound = q1 - IQR_MULTIPLIER * iqr
    upper_bound = q3 + IQR_MULTIPLIER * iqr

    df_flagged = df.withColumn(
        "is_outlier_views",
        (F.col("view_count") < lower_bound) | (F.col("view_count") > upper_bound)
    )

    outlier_count = df_flagged.filter(F.col("is_outlier_views")).count()
    total_count = df_flagged.count()
    print(f"[STATS] IQR bounds for view_count: [{lower_bound:.2f}, {upper_bound:.2f}]")
    print(f"[STATS] Outliers detected: {outlier_count} / {total_count} rows "
          f"({(outlier_count / total_count * 100) if total_count else 0:.2f}%)")

    print("[STATS] Sample outlier rows (by view_count):")
    df_flagged.filter(F.col("is_outlier_views")) \
        .select("video_id", "keyword", "view_count", "like_count", "comment_count") \
        .orderBy(F.desc("view_count")) \
        .show(10, truncate=False)

    # ── Relationships: correlation & covariance between engagement metrics ──
    #  Guard against ArithmeticException: [DIVIDE_BY_ZERO]. Spark 4's ANSI
    # mode (on by default) makes corr()/cov() raise instead of returning
    # NULL when a column has zero variance (e.g. duration_seconds is all
    # zeros in this dataset -- stddev 0 means corr() divides by zero).
    # try_divide isn't usable here since df.stat.corr()/cov() don't expose
    # an expression-level API, so we catch the specific exception instead.
    corr_views_engagement = _safe_corr(df, "view_count", "engagement_rate")
    corr_likes_comments = _safe_corr(df, "like_count", "comment_count")
    corr_duration_views = _safe_corr(df, "duration_seconds", "view_count")
    cov_views_likes = _safe_cov(df, "view_count", "like_count")

    print("\n[STATS] Correlation / covariance between metrics:")
    print(f"  corr(view_count, engagement_rate)  = {_fmt_stat(corr_views_engagement)}")
    print(f"  corr(like_count, comment_count)    = {_fmt_stat(corr_likes_comments)}")
    print(f"  corr(duration_seconds, view_count) = {_fmt_stat(corr_duration_views)}")
    print(f"  cov(view_count, like_count)        = {_fmt_stat(cov_views_likes)}")

    return df_flagged


def _safe_corr(df: DataFrame, col1: str, col2: str):
    """
    Wraps df.stat.corr() to return None (instead of raising) when one of the
    columns has zero variance -- e.g. a constant column like an all-zero
    duration_seconds. Under Spark's ANSI mode this raises DIVIDE_BY_ZERO
    rather than the pre-ANSI behavior of returning NaN/NULL.
    """
    try:
        return df.stat.corr(col1, col2)
    except Exception as e:
        if "DIVIDE_BY_ZERO" in str(e):
            print(f"[STATS] corr({col1}, {col2}) undefined -- one column has zero variance "
                  f"(e.g. a constant/all-same value), skipping.")
            return None
        raise


def _safe_cov(df: DataFrame, col1: str, col2: str):
    """Same zero-variance guard as _safe_corr(), for df.stat.cov()."""
    try:
        return df.stat.cov(col1, col2)
    except Exception as e:
        if "DIVIDE_BY_ZERO" in str(e):
            print(f"[STATS] cov({col1}, {col2}) undefined -- one column has zero variance, skipping.")
            return None
        raise


def _fmt_stat(value) -> str:
    """Formats a stat value for display, showing N/A for None instead of crashing on format spec."""
    return f"{value:.4f}" if value is not None else "N/A (zero variance)"


# ── 7. POST-LOAD ANALYSIS (on Hive-backed data) WITH HPP ────────
def analyze_post_load(spark: SparkSession, df_hive: DataFrame, drop_legacy_constraints: bool = False) -> DataFrame:
    print("\n[POST-ANALYSIS] Building keyword/category summary from Hive data...")

    has_outlier_flag = "is_outlier_views" in df_hive.columns

    agg_exprs = [
        F.count("*").alias("total_videos"),
        F.sum("view_count").alias("total_views"),
        F.round(F.avg("view_count"), 2).alias("avg_views"),
        F.round(F.avg("engagement_rate"), 6).alias("avg_engagement_rate"),
        F.round(F.avg("duration_seconds"), 1).alias("avg_duration_sec"),
        F.sum(F.when(F.col("performance_tier") == "Viral", 1).otherwise(0)).alias("viral_video_count"),
    ]
    if has_outlier_flag:
        agg_exprs.append(
            F.sum(F.when(F.col("is_outlier_views"), 1).otherwise(0)).alias("outlier_video_count")
        )

    #  HPP OPTIMIZATION: Pushdown-friendly aggregations
    summary_df = (
        df_hive
        #  Filter early to minimize shuffle (pushdown already happened in read_from_hive)
        .groupBy("keyword", "category_name")
        .agg(*agg_exprs)
        #  Pushdown-friendly ordering (uses sorted aggregates)
        .orderBy(F.desc("total_views"))
    )

    #  Materialize the summary once here — this function fires multiple
    # actions against summary_df (show(), distinct().collect() for the
    # replaceWhere list, and the write itself). Caching keeps them consistent.
    summary_df = summary_df.cache()
    summary_df.count()

    print("[POST-ANALYSIS] Top performing keyword/category combos:")
    summary_df.show(15, truncate=False)

    # Save analysis output as its own Delta table, scoped to this batch's keywords
    full_agg_table = f"{HIVE_DB}.{KEYWORD_AGG_TABLE}"
    print(f"[POST-ANALYSIS] Saving summary to Delta table: {full_agg_table}")

    #  Same precheck + re-verify as load_to_hive() — this table could
    # independently have picked up a legacy hardcoded constraint too.
    check_and_handle_legacy_constraints(spark, full_agg_table, auto_drop=drop_legacy_constraints)

    agg_table_exists = spark.catalog.tableExists(full_agg_table)
    keywords_in_summary = sorted(
        r["keyword"] for r in summary_df.select("keyword").distinct().collect() if r["keyword"]
    )

    if agg_table_exists and keywords_in_summary:
        # Same v2-catalog gotcha as load_to_hive(): must use .insertInto(),
        # not .saveAsTable(), for a replaceWhere-scoped overwrite of an
        # EXISTING table -- see the detailed comment in load_to_hive().
        quoted = _sql_quote_list(keywords_in_summary)
        replace_where = f"keyword IN ({quoted})"

        def _write():
            aligned_df = _align_for_insert_into(spark, full_agg_table, summary_df)
            (
                aligned_df.write
                .format("delta")
                .mode("overwrite")
                .option("replaceWhere", replace_where)
                .option("mergeSchema", "true")
                .insertInto(full_agg_table)
            )
    else:
        def _write():
            (
                summary_df.write
                .format("delta")
                .mode("overwrite")
                .partitionBy("keyword")  #  HPP: Partition high-cardinality column for future filters
                .option("mergeSchema", "true")
                .saveAsTable(full_agg_table)
            )

    _write_with_constraint_retry(
        spark, full_agg_table,
        write_fn=_write,
        drop_legacy_constraints=drop_legacy_constraints,
    )

    return summary_df


# ── 8. WRITE TO HISTORY TABLE (WITH HPP) ───────────────────────
def write_history(spark: SparkSession, summary_df: DataFrame):
    run_date = datetime.now().strftime("%Y-%m-%d")
    run_ts   = datetime.now()

    print(f"\n[HISTORY] Appending run snapshot ({run_date}) to history table: {HIVE_DB}.{HISTORY_TABLE}")

    history_df = (
        summary_df
        .withColumn("run_date", F.lit(run_date))
        .withColumn("run_timestamp", F.lit(run_ts).cast(TimestampType()))
    )

    has_outlier_col = "outlier_video_count" in history_df.columns

    outlier_col_ddl = "outlier_video_count   BIGINT,\n            " if has_outlier_col else ""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {HIVE_DB}.{HISTORY_TABLE} (
            keyword              STRING,
            category_name        STRING,
            total_videos         BIGINT,
            total_views          BIGINT,
            avg_views            DOUBLE,
            avg_engagement_rate  DOUBLE,
            avg_duration_sec     DOUBLE,
            viral_video_count    BIGINT,
            {outlier_col_ddl}run_timestamp        TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (run_date STRING)
    """)

    #  IMPORTANT: CREATE TABLE IF NOT EXISTS only defines the schema the
    # FIRST time this table is created. If an earlier run created it without
    # outlier_video_count (e.g. that run's data had zero variance and never
    # produced an outlier flag), CREATE TABLE IF NOT EXISTS is a no-op on
    # subsequent runs and does NOT retroactively add the column -- so a
    # later run whose data DOES have outlier_video_count fails with
    # INSERT_COLUMN_ARITY_MISMATCH. _align_for_insert_into() handles this
    # the same way it does for the other two tables: ALTER TABLE ADD COLUMNS
    # for anything new, then reorder to match the table's actual column
    # order (insertInto is positional, not name-based).
    full_history_table = f"{HIVE_DB}.{HISTORY_TABLE}"
    aligned_history_df = _align_for_insert_into(spark, full_history_table, history_df)

    (
        aligned_history_df.write
        .mode("append")
        .insertInto(full_history_table)  # inherits Delta format from the table itself
    )

    print(f"[HISTORY] Appended {history_df.count()} rows for run_date={run_date}.")


# ── 9. EXPORT FINAL RESULT (local disk, HDFS, or S3) ────────────
def export_to_csv(summary_df: DataFrame, output_path: str):
    if output_path.startswith("s3a://") or output_path.startswith("s3://"):
        destination = "S3"
    elif output_path.startswith("hdfs://") or output_path.startswith("/user/"):
        destination = "HDFS"
    else:
        destination = "local disk"

    print(f"\n[EXPORT] Writing final analysis result to {destination}: {output_path}")
    (
        summary_df
        .coalesce(1)                      # single output file
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(output_path)
    )
    print(f"[EXPORT] CSV export to {destination} complete.")


# ── MAIN ─────────────────────────────────────────────────────────
def run_silver_stage(spark: SparkSession, bronze_df: DataFrame, drop_legacy_constraints: bool) -> DataFrame:
    """
    SILVER LAYER: cleans/conforms bronze data and writes it to the
    (legacy-named) RAW_TABLE, which is the silver table in medallion terms.
    Returns the cached, cleaned DataFrame for optional reuse by the caller.
    """
    clean_df = transform(bronze_df)

    #  Materialize clean_df ONCE so every downstream action (pre-load
    # analysis, keyword-list collection for replaceWhere, and the actual
    # write) all see the exact same rows.
    clean_df = clean_df.cache()
    clean_df.count()

    analyze_pre_load(clean_df)
    load_to_hive(spark, clean_df, drop_legacy_constraints=drop_legacy_constraints)
    return clean_df


def run_gold_stage(spark: SparkSession, args) -> DataFrame:
    """
    GOLD LAYER: reads the silver table with keyword/min-views filtering,
    runs statistics, builds the keyword/category summary, appends to the
    history table, and exports the final CSV report. Returns the summary
    DataFrame for optional reuse/cleanup by the caller.
    """
    hive_df = read_from_hive(
        spark,
        min_views=args.min_views,
        keyword=args.keyword,
        keyword_mode=args.keyword_mode,
    )
    hive_df_flagged = statistical_analysis(hive_df)
    summary_df = analyze_post_load(
        spark, hive_df_flagged, drop_legacy_constraints=args.drop_legacy_constraints
    )
    write_history(spark, summary_df)
    export_to_csv(summary_df, args.output)
    return summary_df


# NOTE: the old unified main() (which branched on args.stage and had the
# --inspect-only short-circuit) lived here. It's gone -- each of
# etl_bronze.py / etl_silver.py / etl_gold.py now has its own tiny main()
# that calls straight into ingest_bronze() / run_silver_stage() /
# run_gold_stage() above.