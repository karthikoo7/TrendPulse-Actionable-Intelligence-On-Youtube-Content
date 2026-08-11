"""
etl_upload_gsheet.py
Pushes the Gold-layer CSV export (part-*.csv glob) into a Google Sheet
so Tableau Public can auto-refresh from it (Google Drive connector,
~24h refresh cycle).

Usage:
    python etl_upload_gsheet.py \
        --csv-dir /path/to/gold/output_dir \
        --sheet-id <google_sheet_id> \
        --worksheet Sheet1 \
        --creds /path/to/service_account.json
"""
import argparse
import glob
import sys
import numpy as np
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def load_gold_csv(csv_dir: str) -> pd.DataFrame:
    """Spark's .write.csv() produces a directory of part-*.csv, not a
    single file -- glob for it (documented gotcha in etl_common.py)."""
    parts = glob.glob(f"{csv_dir}/part-*.csv")
    if not parts:
        print(f"No part-*.csv files found in {csv_dir}", file=sys.stderr)
        sys.exit(1)
    return pd.concat((pd.read_csv(p) for p in parts), ignore_index=True)


def push_to_sheet(df: pd.DataFrame, sheet_id: str, worksheet: str, creds_path: str):
    creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    client = gspread.authorize(creds)
    sh = client.open_by_key(sheet_id)

    try:
        ws = sh.worksheet(worksheet)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=worksheet, rows=1, cols=1)

    ws.clear()

    # --- FIX: Force numeric columns to proper int/float dtypes ---
    numeric_cols = [
        'total_videos', 'total_views', 'avg_views',
        'avg_engagement_rate', 'avg_duration_sec', 'viral_video_count'
    ]
    # outlier_video_count may or may not exist depending on the data
    if 'outlier_video_count' in df.columns:
        numeric_cols.append('outlier_video_count')

    for col in numeric_cols:
        if col in df.columns:
            # Convert to numeric; invalid entries become NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Replace inf/-inf with NaN, then replace all NaN/None with Python None
    # This keeps actual numbers as int/float for the Sheets API
    df_clean = df.replace([np.inf, -np.inf], np.nan)
    rows = [df_clean.columns.tolist()] + df_clean.where(pd.notnull(df_clean), None).values.tolist()

    ws.update(rows)
    print(f"Pushed {len(df)} rows to sheet '{worksheet}'.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv-dir", required=True, help="Gold layer output directory (contains part-*.csv)")
    parser.add_argument("--sheet-id", required=True, help="Google Sheet ID (from its URL)")
    parser.add_argument("--worksheet", default="Sheet1")
    parser.add_argument("--creds", required=True, help="Path to service account JSON key")
    args = parser.parse_args()

    df = load_gold_csv(args.csv_dir)
    push_to_sheet(df, args.sheet_id, args.worksheet, args.creds)


if __name__ == "__main__":
    main()
