"""
Pipeline: LinkedIn (Brightdata batch)
Source:   s3://prajnov-brightdata-datasets/servicenow-us/bd_*.json
Output:   s3://prajnov-talent-lake/bronze/linkedin/profiles/YYYY/MM/DD/{uuid}.json

Usage:
  python ingest.py
  python ingest.py --dry-run --limit 100
  python ingest.py --dataset s3://prajnov-brightdata-datasets/servicenow-us/bd_20260628_065102.json
"""
import argparse
import sys
from pathlib import Path

import ijson

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import bronze_path, hash_record, make_meta, s3_put, write_manifest, SeenSet, get_s3, BRONZE_BUCKET

DEFAULT_DATASET = "s3://prajnov-brightdata-datasets/servicenow-us/bd_20260628_065102.json"
SOURCE_BUCKET = "prajnov-brightdata-datasets"


def stream_brightdata(s3_key: str):
    """Stream JSON array from S3 without loading into memory."""
    s3 = get_s3()
    resp = s3.get_object(Bucket=SOURCE_BUCKET, Key=s3_key)
    for profile in ijson.items(resp["Body"], "item"):
        yield profile


def build_record(profile: dict) -> dict:
    linkedin_id = profile.get("linkedin_id") or profile.get("id", "")
    url = profile.get("url") or profile.get("linkedin_url", "")
    meta = make_meta(
        source="brightdata_linkedin",
        retention_class="pii",
        source_url=url,
        extra={
            "record_id": linkedin_id,
            "dataset": "brightdata_linkedin",
        },
    )
    record = {"_meta": meta, **profile}
    record["_meta"]["raw_hash"] = hash_record(profile)
    return record, linkedin_id or url


def run(dataset_uri: str, dry_run: bool, limit: int):
    # Parse s3://bucket/key
    path = dataset_uri.replace("s3://", "").split("/", 1)
    s3_key = path[1] if len(path) > 1 else path[0]

    seen = SeenSet()
    stats = {"written": 0, "skipped_dup": 0, "errors": 0}

    for i, profile in enumerate(stream_brightdata(s3_key)):
        if limit and i >= limit:
            break

        try:
            record, uid = build_record(profile)
            if uid in seen:
                stats["skipped_dup"] += 1
                continue
            seen.add(uid)

            key = bronze_path("linkedin", "profiles")
            s3_put(key, record, dry_run=dry_run)
            stats["written"] += 1

            if stats["written"] % 500 == 0:
                print(f"  written={stats['written']} skipped={stats['skipped_dup']}")

        except Exception as e:
            stats["errors"] += 1
            print(f"[ERROR] record {i}: {e}")

    write_manifest("linkedin", stats, dry_run=dry_run)
    print(f"Done: {stats}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", default=DEFAULT_DATASET)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()
    run(args.dataset, args.dry_run, args.limit)
