import os
import sys
import time
from confluent_kafka.admin import AdminClient
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    avg,
    col,
    current_timestamp,
    datediff,
    from_json,
    greatest,
    length,
    lit,
    rank,
    round,
    size,
    split,
    stddev,
    sum,
    to_timestamp,
    when,
)
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


def check_topic_exists(bootstrap_servers, topic_name):
    """Safely checks if the topic exists on Kafka before Spark attempts to read it."""
    try:
        admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})
        metadata = admin_client.list_topics(timeout=10)
        return topic_name in metadata.topics
    except Exception as e:
        print(f"⚠️ Could not query Kafka broker metadata: {e}")
        return False


def run_pyspark_etl(kafka_topic, primary_kw, output_dir):
    print("=" * 70)
    print(f"🚀 Starting PySpark STREAMING ETL Task")
    print(f"📌 Kafka Topic: '{kafka_topic}'")
    print(f"📌 Primary Keyword: '{primary_kw}'")
    print(f"📌 Output Directory: '{output_dir}'")
    print("=" * 70)

    # 1. Pre-Check Kafka Topic Existence
    if not check_topic_exists("localhost:9092", kafka_topic):
        print(f"⚠️ Topic '{kafka_topic}' does not exist on Kafka broker! Upstream producer may have yielded 0 records.")
        sys.exit(0)

    spark = (
        SparkSession.builder.appName("YouTubePopularityAndTrendsETL_Streaming")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.streaming.schemaInference", "false")
        .config("spark.sql.streaming.metricsEnabled", "true")
        .getOrCreate()
    )

    # Disable INFO logging for cleaner output
    spark.sparkContext.setLogLevel("WARN")

    try:
        # 2. READ STREAM from Kafka (instead of batch read)
        kafka_df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", kafka_topic)
            .option("startingOffsets", "latest")  # Only new messages
            .option("maxOffsetsPerTrigger", 1000)  # Control batch size
            .option("failOnDataLoss", "false")
            .load()
        )

        # 3. Producer JSON Schema (unchanged)
        schema = StructType([
            StructField("video_id", StringType(), True),
            StructField("keyword", StringType(), True),
            StructField("keyword_type", StringType(), True),
            StructField("title", StringType(), True),
            StructField("channelTitle", StringType(), True),
            StructField("description", StringType(), True),
            StructField("publishedAt", StringType(), True),
            StructField("viewCount", LongType(), True),
            StructField("likeCount", LongType(), True),
            StructField("commentCount", LongType(), True),
            StructField("duration_seconds", IntegerType(), True),
            StructField("dimension", StringType(), True),
            StructField("definition", StringType(), True),
            StructField("caption", StringType(), True),
            StructField("licensedContent", BooleanType(), True),
            StructField("projection", StringType(), True),
            StructField("categoryId", StringType(), True),
            StructField("tags", StringType(), True),
            StructField("defaultAudioLanguage", StringType(), True),
            StructField("uploadStatus", StringType(), True),
            StructField("privacyStatus", StringType(), True),
            StructField("madeForKids", BooleanType(), True),
            StructField("embeddable", BooleanType(), True),
            StructField("topicCategories", StringType(), True),
        ])

        # 4. Parse JSON & Extract Initial Fields
        raw_json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
        parsed_df = (
            raw_json_df.select(from_json(col("json_str"), schema).alias("data"))
            .select("data.*")
            .filter(col("video_id").isNotNull())
        )

        # 📌 OUTPUT 1: RAW INGESTED DATA (Streaming)
        raw_export_df = parsed_df.select(
            col("video_id"),
            col("keyword").alias("search_keyword"),
            col("keyword_type"),
            col("title"),
            col("channelTitle").alias("channel_title"),
            col("publishedAt").alias("published_at"),
            col("viewCount").alias("views"),
            col("likeCount").alias("likes"),
            col("commentCount").alias("comments"),
            col("duration_seconds"),
            col("categoryId").alias("category_id"),
            col("defaultAudioLanguage").alias("default_audio_language"),
            col("tags"),
            col("topicCategories").alias("topic_categories"),
        )

        # 📌 TRANSFORMATIONS & FEATURE ENGINEERING (unchanged)
        processed_df = (
            raw_export_df
            .withColumn("published_at_ts", to_timestamp(col("published_at")))
            .withColumn(
                "days_since_published",
                greatest(lit(1), datediff(current_timestamp(), col("published_at_ts"))),
            )
            .withColumn(
                "daily_view_velocity",
                round(col("views") / col("days_since_published"), 2),
            )
            .withColumn(
                "engagement_rate_%",
                when(
                    col("views") > 0,
                    round(((col("likes") + col("comments")) / col("views")) * 100, 2),
                ).otherwise(0.0),
            )
            .withColumn(
                "like_to_comment_ratio",
                when(
                    col("comments") > 0,
                    round(col("likes") / col("comments"), 2),
                ).otherwise(col("likes")),
            )
            .withColumn(
                "format",
                when(col("duration_seconds") <= 60, "Shorts").otherwise("Long-Form"),
            )
            .withColumn("title_char_length", length(col("title")))
            .withColumn(
                "tag_count",
                when(col("tags").isNotNull() & (col("tags") != ""), size(split(col("tags"), "\\|"))).otherwise(0),
            )
        )

        # Window Calculations for Keyword Partitions
        keyword_window = Window.partitionBy("search_keyword")
        keyword_rank_window = Window.partitionBy("search_keyword").orderBy(col("views").desc())

        windowed_df = (
            processed_df
            .withColumn("total_kw_views", sum("views").over(keyword_window))
            .withColumn(
                "view_share_%_in_keyword",
                when(col("total_kw_views") > 0, round((col("views") / col("total_kw_views")) * 100, 2)).otherwise(0.0),
            )
            .withColumn("rank_in_keyword", rank().over(keyword_rank_window))
            .withColumn("avg_kw_velocity", avg("daily_view_velocity").over(keyword_window))
            .withColumn("std_kw_velocity", stddev("daily_view_velocity").over(keyword_window))
            .withColumn(
                "velocity_z_score",
                when(
                    col("std_kw_velocity").isNotNull() & (col("std_kw_velocity") > 0),
                    round((col("daily_view_velocity") - col("avg_kw_velocity")) / col("std_kw_velocity"), 2),
                ).otherwise(0.0),
            )
            .withColumn(
                "trend_lifecycle_stage",
                when(
                    (col("velocity_z_score") > 1.0) & (col("engagement_rate_%") > 3.0), "Viral Trend"
                ).when(
                    (col("velocity_z_score") <= 1.0) & (col("engagement_rate_%") > 3.0), "Emerging Niche"
                ).when(
                    (col("velocity_z_score") > 1.0) & (col("engagement_rate_%") <= 3.0), "Mass Peak"
                ).otherwise("Declining / Baseline"),
            )
        )

        # 📌 OUTPUT 2: SUMMARY AGGREGATION (Streaming Update Mode)
        summary_df = (
            windowed_df.groupBy("search_keyword")
            .agg(
                sum("views").alias("total_views"),
                sum("likes").alias("total_likes"),
                sum("comments").alias("total_comments"),
                round(avg("daily_view_velocity"), 2).alias("avg_daily_view_velocity"),
                round(avg("engagement_rate_%"), 2).alias("avg_engagement_rate_%"),
                round(avg("duration_seconds"), 0).alias("avg_duration_seconds"),
            )
            .withColumn(
                "keyword_type",
                when(col("search_keyword") == primary_kw, "Primary").otherwise("General"),
            )
        )

        # 📌 OUTPUT 3: FULL TRANSFORMED ANALYTICS TABLE
        transformed_analytics_df = windowed_df.select(
            "video_id",
            "search_keyword",
            "keyword_type",
            "rank_in_keyword",
            "title",
            "channel_title",
            "published_at",
            "days_since_published",
            "views",
            "likes",
            "comments",
            "duration_seconds",
            "format",
            "daily_view_velocity",
            "velocity_z_score",
            "engagement_rate_%",
            "like_to_comment_ratio",
            "view_share_%_in_keyword",
            "trend_lifecycle_stage",
            "title_char_length",
            "tag_count",
            "default_audio_language",
            "tags",
            "topic_categories",
        )

        # Ensure explicit HDFS filesystem path URI
        hdfs_base = output_dir if output_dir.startswith("hdfs://") else f"hdfs://localhost:9000{output_dir}"

        raw_path = f"{hdfs_base}/raw_kafka_ingested_data"
        summary_path = f"{hdfs_base}/keyword_popularity_summary"
        transformed_path = f"{hdfs_base}/all_keywords_analytics"

        # --------------------------------------------------------------------
        # STREAMING WRITES with Checkpointing
        # --------------------------------------------------------------------
        checkpoint_dir = f"{hdfs_base}/checkpoints/{kafka_topic}"

        print("💾 Starting STREAMING CSV exports to HDFS...")
        print(f"📌 Checkpoint directory: {checkpoint_dir}")

        # Stream 1: Raw Data (Append mode - new rows only)
        raw_query = (
            raw_export_df.writeStream
            .format("csv")
            .option("header", "true")
            .option("path", raw_path)
            .option("checkpointLocation", f"{checkpoint_dir}/raw")
            .outputMode("append")
            .trigger(processingTime="10 seconds")
            .start()
        )

        # Stream 2: Summary Aggregations (Update mode - keyword stats change)
        summary_query = (
            summary_df.writeStream
            .format("csv")
            .option("header", "true")
            .option("path", summary_path)
            .option("checkpointLocation", f"{checkpoint_dir}/summary")
            .outputMode("update")
            .trigger(processingTime="10 seconds")
            .start()
        )

        # Stream 3: Transformed Analytics (Append mode - new records only)
        transformed_query = (
            transformed_analytics_df.writeStream
            .format("csv")
            .option("header", "true")
            .option("path", transformed_path)
            .option("checkpointLocation", f"{checkpoint_dir}/transformed")
            .outputMode("append")
            .trigger(processingTime="10 seconds")
            .start()
        )

        print("=" * 70)
        print("✅ PySpark STREAMING ETL Started Successfully!")
        print(f"📁 Raw Data Path: {raw_path}")
        print(f"📁 Summary Path: {summary_path}")
        print(f"📁 Transformed Analytics Path: {transformed_path}")
        print(f"📌 Checkpoint Dir: {checkpoint_dir}")
        print("⏳ Streaming will run until manually stopped or DAG timeout.")
        print("=" * 70)

        # Wait for the streams to finish (or keep running)
        # For Airflow, we'll run for a set duration then stop gracefully
        stream_timeout_seconds = 120  # Run for 2 minutes (adjust as needed)
        print(f"⏳ Running streaming for {stream_timeout_seconds} seconds...")

        start_time = time.time()
        while time.time() - start_time < stream_timeout_seconds:
            # Check if any stream has failed
            for query in [raw_query, summary_query, transformed_query]:
                if query.exception():
                    print(f"❌ Stream failed: {query.exception()}")
                    raise Exception(f"Stream failed: {query.exception()}")

            time.sleep(5)

        # Stop all streams gracefully
        print("🛑 Stopping all streams gracefully...")
        raw_query.stop()
        summary_query.stop()
        transformed_query.stop()

        # Wait for termination
        raw_query.awaitTermination(timeout=30)
        summary_query.awaitTermination(timeout=30)
        transformed_query.awaitTermination(timeout=30)

        print("✅ All streams stopped successfully.")
        print("=" * 70)

    except Exception as e:
        print(f"❌ Critical Exception during PySpark Streaming Processing: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: pyspark_etl.py <kafka_topic> <primary_kw> <output_dir>")
        sys.exit(1)

    kafka_topic_arg = sys.argv[1]
    primary_kw_arg = sys.argv[2]
    output_dir_arg = sys.argv[3]

    run_pyspark_etl(kafka_topic_arg, primary_kw_arg, output_dir_arg)