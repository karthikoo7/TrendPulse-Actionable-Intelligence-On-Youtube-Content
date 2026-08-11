import csv
import io
import os
import subprocess
import gspread

# Base path where Spark saves the output
HDFS_BASE_DIR = "/home/kritix/bigdata/test-jupyter/Live_Streaming/tableau_exports"
CREDS_FILE = "/home/kritix/bigdata/test-jupyter/Live_Streaming/gsheets_credentials.json"

# ⚠️ YOUR GOOGLE SPREADSHEET KEY ⚠️
SPREADSHEET_KEY = "16CfGRsvIBbH9RiycAhqgpfz5XS6sEJayJU19ROLQqqY"


def read_csv_from_source(folder_path):
    """Reads directly from HDFS (source of truth). Spark always writes here
    via the hdfs://localhost:9000 prefix, so we skip any local-disk check —
    a stale local folder with the same path was previously masking fresh
    HDFS output every run."""
    print(f"🔍 Checking HDFS cluster for: {folder_path}...")
    list_cmd = f"hdfs dfs -ls {folder_path}"
    result = subprocess.run(
        list_cmd, shell=True, capture_output=True, text=True
    )

    if result.returncode == 0 and result.stdout.strip():
        hdfs_file_path = None
        for line in result.stdout.strip().split("\n"):
            if "part-" in line and line.endswith(".csv"):
                hdfs_file_path = line.split()[-1]
                break

        if hdfs_file_path:
            print(f"📥 Fetching from HDFS: {hdfs_file_path}")
            cat_cmd = f"hdfs dfs -cat {hdfs_file_path}"
            cat_result = subprocess.run(
                cat_cmd, shell=True, capture_output=True, text=True
            )

            if cat_result.returncode == 0:
                f = io.StringIO(cat_result.stdout)
                reader = csv.reader(f)
                return list(reader)

    print(f"⚠️ No CSV file found in HDFS directory: {folder_path}")
    return []


def sync_hdfs_to_gsheet():
    gc = gspread.service_account(filename=CREDS_FILE)
    sh = gc.open_by_key(SPREADSHEET_KEY)

    exports = {
        "raw_kafka_ingested_data": "Raw Data",
        "keyword_popularity_summary": "Popularity Summary",
        "all_keywords_analytics": "Transformed Analytics",
    }

    for folder_name, worksheet_name in exports.items():
        export_path = f"{HDFS_BASE_DIR}/{folder_name}"
        rows = read_csv_from_source(export_path)

        if not rows:
            print(f"⚠️ Skipping '{worksheet_name}' because no rows were found.")
            continue

        try:
            worksheet = sh.worksheet(worksheet_name)
            worksheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            print(f"➕ Creating new worksheet tab: '{worksheet_name}'")
            worksheet = sh.add_worksheet(
                title=worksheet_name, rows="5000", cols="30"
            )

        # Force write with USER_ENTERED formatting so Tableau parses numbers & dates
        worksheet.update(values=rows, value_input_option="USER_ENTERED")
        print(
            f"✅ Successfully synced {len(rows)} rows to worksheet '{worksheet_name}'."
        )


if __name__ == "__main__":
    sync_hdfs_to_gsheet()