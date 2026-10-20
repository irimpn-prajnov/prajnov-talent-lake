"""
Pipeline: Resumes (Own Data Migration)
Source:   s3://prajnov-ai-recuiter-candidate-profiles/
            - resumes/{candidate_id}/*.pdf
            - candidate_db/candidates.json
Output:   s3://prajnov-talent-lake/bronze/candidates/

Two modes:
  1. resumes    — copy PDF files to bronze, extract text
  2. candidates — migrate candidates.json records to bronze

Usage:
  python ingest.py --mode resumes
  python ingest.py --mode candidates
  python ingest.py --mode all --dry-run --limit 100
"""
import argparse
import io
import json
import sys
import time
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import bronze_path, hash_record, make_meta, s3_put, write_manifest, SeenSet, get_s3, BRONZE_BUCKET

SOURCE_BUCKET = "prajnov-ai-recuiter-candidate-profiles"


def list_s3_keys(prefix: str) -> list:
    s3 = get_s3()
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=SOURCE_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def extract_text_pdfplumber(pdf_bytes: bytes) -> str:
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            return "\n".join(p.extract_text() or "" for p in pdf.pages).strip()
    except Exception:
        return ""


def migrate_resume(s3_key: str, dry_run: bool) -> dict:
    s3 = get_s3()
    obj = s3.get_object(Bucket=SOURCE_BUCKET, Key=s3_key)
    pdf_bytes = obj["Body"].read()

    filename = s3_key.split("/")[-1]
    candidate_id = s3_key.split("/")[-2] if "/" in s3_key else ""
    extracted_text = extract_text_pdfplumber(pdf_bytes)

    # Copy original PDF to bronze
    pdf_key = bronze_path("candidates", "resumes").replace(".json", ".pdf")
    if not dry_run:
        get_s3().put_object(
            Bucket=BRONZE_BUCKET,
            Key=pdf_key,
            Body=pdf_bytes,
            ContentType="application/pdf",
        )

    data = {
        "original_filename": filename,
        "source_s3_key": s3_key,
        "candidate_id_hint": candidate_id,
        "file_type": "pdf",
        "bronze_pdf_key": pdf_key,
        "extracted_text": extracted_text[:50000],  # cap at 50K chars
        "extraction_method": "pdfplumber",
    }
    meta = make_meta(
        source="recruiter_upload",
        retention_class="pii",
        source_url=f"s3://{SOURCE_BUCKET}/{s3_key}",
        extra={"raw_hash": hash_record(data), "record_id": candidate_id},
    )
    return {"_meta": meta, **data}


def migrate_candidates_json(dry_run: bool, limit: int):
    s3 = get_s3()
    obj = s3.get_object(Bucket=SOURCE_BUCKET, Key="candidate_db/candidates.json")
    raw = json.loads(obj["Body"].read())
    candidates = list(raw.values()) if isinstance(raw, dict) else raw

    seen = SeenSet()
    stats = {"written": 0, "skipped_dup": 0, "errors": 0}

    for i, c in enumerate(candidates):
        if limit and i >= limit:
            break

        cid = c.get("id", "")
        if cid in seen:
            stats["skipped_dup"] += 1
            continue
        seen.add(cid)

        try:
            meta = make_meta(
                source="recruiter_upload",
                retention_class="pii",
                source_url=f"s3://{SOURCE_BUCKET}/candidate_db/candidates.json",
                extra={"raw_hash": hash_record(c), "record_id": cid},
            )
            record = {"_meta": meta, **c}
            key = bronze_path("candidates", "profiles")
            s3_put(key, record, dry_run=dry_run)
            stats["written"] += 1
        except Exception as e:
            stats["errors"] += 1
            print(f"[ERROR] candidate {cid}: {e}")

    write_manifest("resumes_candidates", stats, dry_run=dry_run)
    print(f"Candidates done: {stats}")


def migrate_resumes(dry_run: bool, limit: int):
    keys = list_s3_keys("resumes/")
    pdf_keys = [k for k in keys if k.lower().endswith(".pdf")]
    print(f"Found {len(pdf_keys)} PDFs")

    seen = SeenSet()
    stats = {"written": 0, "skipped_dup": 0, "errors": 0}

    for i, s3_key in enumerate(pdf_keys):
        if limit and i >= limit:
            break
        if s3_key in seen:
            stats["skipped_dup"] += 1
            continue
        seen.add(s3_key)

        try:
            record = migrate_resume(s3_key, dry_run)
            key = bronze_path("candidates", "resumes")
            s3_put(key, record, dry_run=dry_run)
            stats["written"] += 1
            if stats["written"] % 50 == 0:
                print(f"  written={stats['written']}")
            time.sleep(0.1)
        except Exception as e:
            stats["errors"] += 1
            print(f"[ERROR] {s3_key}: {e}")

    write_manifest("resumes_pdfs", stats, dry_run=dry_run)
    print(f"Resumes done: {stats}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["resumes", "candidates", "all"], default="all")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    if args.mode in ("candidates", "all"):
        migrate_candidates_json(args.dry_run, args.limit)
    if args.mode in ("resumes", "all"):
        migrate_resumes(args.dry_run, args.limit)
