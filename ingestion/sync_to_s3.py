"""
Sync local_bronze/ to s3://prajnov-talent-lake/bronze/
Run this once AWS credentials are refreshed.

Usage:
  python sync_to_s3.py
  python sync_to_s3.py --dry-run
"""
import argparse
import json
import sys
from pathlib import Path

import boto3

LOCAL_ROOT = Path(__file__).parent / "local_bronze"
BUCKET = "prajnov-talent-lake"


def run(dry_run: bool):
    if not LOCAL_ROOT.exists():
        print("No local_bronze/ directory found — nothing to sync.")
        return

    files = list(LOCAL_ROOT.rglob("*.json"))
    print(f"Found {len(files)} files to sync to s3://{BUCKET}/")

    s3 = boto3.client("s3", region_name="ap-south-1")
    stats = {"uploaded": 0, "errors": 0}

    for i, path in enumerate(files):
        key = str(path.relative_to(LOCAL_ROOT))
        if dry_run:
            print(f"[DRY-RUN] {key}")
            stats["uploaded"] += 1
            continue
        try:
            with open(path, "rb") as f:
                s3.put_object(Bucket=BUCKET, Key=key, Body=f.read(), ContentType="application/json")
            stats["uploaded"] += 1
            if stats["uploaded"] % 100 == 0:
                print(f"  uploaded {stats['uploaded']}/{len(files)}")
        except Exception as e:
            stats["errors"] += 1
            print(f"[ERROR] {key}: {e}")

    print(f"\nDone: {stats}")
    if not dry_run and stats["errors"] == 0:
        print("All files synced. You can delete local_bronze/ now.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(args.dry_run)
