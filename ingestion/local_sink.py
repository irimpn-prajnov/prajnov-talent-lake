"""
Local sink — writes bronze records to ./local_bronze/ when AWS is unavailable.
Run sync_to_s3.py once AWS credentials are refreshed to push everything up.
"""
import json
import os
from pathlib import Path

LOCAL_ROOT = Path(__file__).parent.parent / "local_bronze"


def local_put(key: str, obj: dict) -> str:
    path = LOCAL_ROOT / key
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(obj, f, default=str, ensure_ascii=False)
    return str(path)


def count_local() -> dict:
    counts = {}
    if not LOCAL_ROOT.exists():
        return counts
    for p in LOCAL_ROOT.rglob("*.json"):
        parts = p.relative_to(LOCAL_ROOT).parts
        pipeline = parts[1] if len(parts) > 1 else "unknown"
        counts[pipeline] = counts.get(pipeline, 0) + 1
    return counts
