import csv
import json
import os
import re
import time
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

load_dotenv()

FIELDNAMES = [
    "video_id",
    "keyword",
    "keyword_type",
    "title",
    "channelTitle",
    "description",
    "publishedAt",
    "viewCount",
    "likeCount",
    "commentCount",
    "duration_seconds",
    "dimension",
    "definition",
    "caption",
    "licensedContent",
    "projection",
    "categoryId",
    "tags",
    "defaultAudioLanguage",
    "uploadStatus",
    "privacyStatus",
    "madeForKids",
    "embeddable",
    "topicCategories",
]


def create_kafka_topic_if_not_exists(bootstrap_servers, topic_name):
    """Explicitly creates the dynamic run topic on the Kafka broker before producing."""
    admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})
    new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)

    fs = admin_client.create_topics([new_topic])
    for topic, f in fs.items():
        try:
            f.result()
            print(f"✅ Kafka topic created/verified: '{topic}'")
        except Exception as e:
            print(f"ℹ️ Topic notice: {e}")


def parse_iso8601_duration(duration_str):
    if not duration_str:
        return 0
    match = re.match(
        r"PT(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?",
        duration_str,
    )
    if not match:
        return 0
    parts = match.groupdict(default="0")
    return (
        int(parts["hours"]) * 3600
        + int(parts["minutes"]) * 60
        + int(parts["seconds"])
    )


def batch_fetch_enriched_statistics(youtube_client, video_list):
    """Batches video IDs into chunks of 50 to minimize API calls and enrich metadata."""
    video_ids = [v["video_id"] for v in video_list if v.get("video_id")]
    details_map = {}

    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i : i + 50]
        response = (
            youtube_client.videos()
            .list(
                part="statistics,contentDetails,status,topicDetails,snippet",
                id=",".join(chunk),
            )
            .execute()
        )

        for item in response.get("items", []):
            vid_id = item["id"]
            stats = item.get("statistics", {})
            content = item.get("contentDetails", {})
            snippet = item.get("snippet", {})
            status = item.get("status", {})
            topics = item.get("topicDetails", {})

            details_map[vid_id] = {
                "viewCount": int(stats.get("viewCount", 0)),
                "likeCount": int(stats.get("likeCount", 0)),
                "commentCount": int(stats.get("commentCount", 0)),
                "duration_seconds": parse_iso8601_duration(
                    content.get("duration", "")
                ),
                "dimension": content.get("dimension"),
                "definition": content.get("definition"),
                "caption": content.get("caption"),
                "licensedContent": content.get("licensedContent", False),
                "projection": content.get("projection"),
                "categoryId": snippet.get("categoryId", ""),
                "tags": "|".join(snippet.get("tags", [])),
                "defaultAudioLanguage": snippet.get(
                    "defaultAudioLanguage", ""
                ),
                "uploadStatus": status.get("uploadStatus"),
                "privacyStatus": status.get("privacyStatus"),
                "madeForKids": status.get("madeForKids", False),
                "embeddable": status.get("embeddable", True),
                "topicCategories": "|".join(topics.get("topicCategories", [])),
            }

    for video in video_list:
        v_id = video.get("video_id")
        video.update(details_map.get(v_id, {}))


def execute_ingestion(
    keywords,
    primary_kw,
    kafka_topic,
    csv_file,
    kafka_bootstrap="localhost:9092",
    max_videos_per_kw=250,
):
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        raise ValueError("Missing YOUTUBE_API_KEY environment variable!")

    # 1. Guarantee topic exists on broker
    create_kafka_topic_if_not_exists(kafka_bootstrap, kafka_topic)

    youtube_extract = build("youtube", "v3", developerKey=api_key)
    producer = Producer({"bootstrap.servers": kafka_bootstrap, "acks": "all"})

    total_collected = 0

    with open(csv_file, "w", newline="", encoding="utf-8") as wf:
        writer = csv.DictWriter(wf, fieldnames=FIELDNAMES)
        writer.writeheader()

        try:
            for kw in keywords:
                kw_type = "Primary" if kw == primary_kw else "General"
                print(f"\n🔍 Processing [{kw_type}] Keyword: '{kw}'...")

                video_ids_seen = set()
                kw_videos = []

                sort_orders = ["relevance", "viewCount", "date"]

                for sort_order in sort_orders:
                    if len(kw_videos) >= max_videos_per_kw:
                        break

                    print(f"  ↳ Fetching sort order: '{sort_order}'...")
                    next_page_token = None
                    pages_fetched = 0

                    while pages_fetched < 3 and len(kw_videos) < max_videos_per_kw:
                        search_res = (
                            youtube_extract.search()
                            .list(
                                q=kw,
                                part="id,snippet",
                                type="video",
                                maxResults=50,
                                order=sort_order,
                                pageToken=next_page_token,
                            )
                            .execute()
                        )

                        items = search_res.get("items", [])
                        if not items:
                            break

                        for item in items:
                            v_id = item["id"].get("videoId")
                            if v_id and v_id not in video_ids_seen:
                                video_ids_seen.add(v_id)
                                kw_videos.append(
                                    {
                                        "video_id": v_id,
                                        "keyword": kw,
                                        "keyword_type": kw_type,
                                        "title": item["snippet"].get("title"),
                                        "channelTitle": item["snippet"].get(
                                            "channelTitle"
                                        ),
                                        "description": item["snippet"].get(
                                            "description"
                                        ),
                                        "publishedAt": item["snippet"].get(
                                            "publishedAt"
                                        ),
                                    }
                                )

                        next_page_token = search_res.get("nextPageToken")
                        pages_fetched += 1
                        if not next_page_token:
                            break

                if kw_videos:
                    print(f"  📥 Enriching statistics for {len(kw_videos)} unique videos...")
                    batch_fetch_enriched_statistics(youtube_extract, kw_videos)

                    writer.writerows(kw_videos)
                    wf.flush()

                    for record in kw_videos:
                        producer.produce(
                            topic=kafka_topic,
                            key=str(record["video_id"]).encode("utf-8"),
                            value=json.dumps(record).encode("utf-8"),
                        )

                    producer.poll(0)
                    total_collected += len(kw_videos)
                    print(f"  ✅ Finished '{kw}': Harvested {len(kw_videos)} videos.")

        except HttpError as e:
            print(f"⚠️ YouTube API Error / Quota Reached: {e}")
        finally:
            print("⏳ Flushing Kafka producer queue...")
            producer.flush(15)
            time.sleep(2)

    print(f"\n🚀 Ingestion Complete! Total {total_collected} videos streamed to Kafka topic '{kafka_topic}'.")