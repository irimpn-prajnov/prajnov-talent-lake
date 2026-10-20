"""
Glue Job: brightdata_bronze_export
Exports BrightData candidates from pgvector → S3 bronze as batched JSONL files.
Each file: 1,000 records → bronze/candidates/brightdata/YYYY/MM/DD/batch_{n}.jsonl
Incremental via watermark on added_at/updated_at.
Schedule: Weekly (Sunday 01:00 UTC)
"""
import json, logging, os
from datetime import datetime, timezone

import boto3, psycopg2, psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BUCKET     = os.getenv("BUCKET", "prajnov-talent-lake")
REGION     = os.getenv("AWS_REGION", "ap-south-1")
DB_HOST    = os.getenv("PGVECTOR_HOST", "ai-recruiter.c7q8sg00wqte.ap-south-1.rds.amazonaws.com")
DB_NAME    = os.getenv("PGVECTOR_DB",   "recruiter")
DB_USER    = os.getenv("PGVECTOR_USER",  "prajnov")
DB_PASS    = os.getenv("PGVECTOR_PASSWORD", "")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))


def s3_client():
    return boto3.client("s3", region_name=REGION)


def db_conn():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


def get_watermark() -> str:
    try:
        obj = s3_client().get_object(Bucket=BUCKET, Key="glue-state/brightdata_bronze_export/last_run.json")
        return json.loads(obj["Body"].read())["last_run"]
    except Exception:
        return "1970-01-01T00:00:00+00:00"


def set_watermark(ts: str):
    s3_client().put_object(
        Bucket=BUCKET,
        Key="glue-state/brightdata_bronze_export/last_run.json",
        Body=json.dumps({"last_run": ts}).encode(),
        ContentType="application/json",
    )


def already_exported_uids() -> set:
    """
    Build done set from the watermark run's batch files only (today's date prefix).
    For a full re-export use --force which resets the watermark to epoch.
    Reading all historical JSONL files would be too slow at scale.
    """
    seen = set()
    date_prefix = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    pag = s3_client().get_paginator("list_objects_v2")
    for page in pag.paginate(Bucket=BUCKET, Prefix=f"bronze/candidates/brightdata/{date_prefix}/"):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith(".jsonl"):
                continue
            try:
                body = s3_client().get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read().decode()
                for line in body.splitlines():
                    if line.strip():
                        rec = json.loads(line)
                        uid = rec.get("_meta", {}).get("record_id") or rec.get("uid", "")
                        if uid:
                            seen.add(uid)
            except Exception as e:
                logger.warning("Could not read %s: %s", obj["Key"], e)
    return seen


def serialize_row(row: dict, now: str) -> dict:
    JSONB_FIELDS = {"certifications_raw", "experience", "education", "signalhire_data", "sn_portal_certs"}
    rec = {"_meta": {
        "source": "brightdata",
        "ingestion_timestamp": now,
        "connector_version": "2.0.0",
        "retention_class": "pii",
        "source_url": row.get("linkedin_url") or "",
        "record_id": row["uid"],
    }}
    for k, v in row.items():
        if hasattr(v, "isoformat"):
            rec[k] = v.isoformat()
        elif k in JSONB_FIELDS:
            rec[k] = v if isinstance(v, (list, dict)) else ([] if k != "signalhire_data" else {})
        else:
            rec[k] = v
    return rec


def run():
    watermark = get_watermark()
    run_start = datetime.now(timezone.utc).isoformat()
    date_path = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    logger.info("brightdata_bronze_export | watermark=%s", watermark)

    logger.info("Building UID index from existing bronze files...")
    done = already_exported_uids()
    logger.info("Already exported: %d UIDs", len(done))

    conn    = db_conn()
    client  = s3_client()
    now     = datetime.now(timezone.utc).isoformat()

    total_written = 0
    batch_num     = 0
    buffer        = []

    def flush_buffer(buf, batch_idx):
        if not buf:
            return
        key  = f"bronze/candidates/brightdata/{date_path}/batch_{batch_idx:04d}.jsonl"
        body = "\n".join(json.dumps(r, default=str) for r in buf)
        client.put_object(Bucket=BUCKET, Key=key, Body=body.encode(), ContentType="application/x-ndjson")
        logger.info("  Wrote %s (%d records)", key, len(buf))

    with conn.cursor() as cur:
        cur.execute("""
            SELECT uid, name, first_name, last_name, linkedin_id, linkedin_url,
                   current_title, current_company, headline,
                   city, state, country, location_raw, about,
                   certifications, certifications_raw, companies, industries,
                   experience, experience_text, experience_years, education,
                   skills, email, phone, visa_status, rate, availability,
                   signalhire_enriched, signalhire_data,
                   sn_portal_verified, sn_portal_certs, sn_portal_checked_at,
                   added_at, updated_at
            FROM candidates
            WHERE source = 'brightdata'
              AND (added_at > %s OR updated_at > %s)
            ORDER BY added_at
        """, (watermark, watermark))

        pg = cur.fetchmany(BATCH_SIZE)
        while pg:
            for row in pg:
                if row["uid"] in done:
                    continue
                buffer.append(serialize_row(dict(row), now))
                done.add(row["uid"])
                total_written += 1

                if len(buffer) >= BATCH_SIZE:
                    flush_buffer(buffer, batch_num)
                    batch_num += 1
                    buffer = []

            pg = cur.fetchmany(BATCH_SIZE)

    flush_buffer(buffer, batch_num)  # final partial batch
    conn.close()
    set_watermark(run_start)
    logger.info("Done. total_written=%d batches=%d", total_written, batch_num + (1 if buffer else 0))


if __name__ == "__main__":
    run()
