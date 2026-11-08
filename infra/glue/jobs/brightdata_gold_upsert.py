"""
Glue Job: brightdata-gold-upsert
Source:  silver/candidate/source=brightdata/
         silver/candidate_certification/source=brightdata/
         silver/candidate_skill/source=brightdata/
         silver/candidate_experience/source=brightdata/
         silver/candidate_education/source=brightdata/
Output:  candidates table (pgvector DB) — upsert

Entity resolution:
  - Primary key: linkedin_id  (uid = "bd_{linkedin_id}")
  - Fallback key: name + state (for records without linkedin_id)
  - Credly verified_certifications[] is NEVER overwritten
  - BrightData self-reported certs go into certifications[] only
  - BrightData wins on: experience, education, about, skills, title, company
  - New candidates (not in DB) are inserted with source='brightdata'
  - Existing candidates get enriched fields updated

Silver rows are classified as:
  - new:     linkedin_id not in candidates table
  - updated: linkedin_id exists, fields differ
  - skipped: linkedin_id exists, nothing changed
"""
import io, json, logging, os, re, sys
from collections import defaultdict
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError
import psycopg2
import psycopg2.extras
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

def _arg(name, default=""):
    key = f"--{name}"
    try:
        idx = sys.argv.index(key)
        return sys.argv[idx + 1]
    except (ValueError, IndexError):
        return os.getenv(name, default)

BUCKET   = _arg("BUCKET",            "prajnov-talent-lake")
REGION   = _arg("AWS_REGION",        "ap-south-1")
DB_HOST  = _arg("PGVECTOR_HOST",     "ai-recruiter.c7q8sg00wqte.ap-south-1.rds.amazonaws.com")
DB_NAME  = _arg("PGVECTOR_DB",       "recruiter")
DB_USER  = _arg("PGVECTOR_USER",     "prajnov")
SECRET_ARN = _arg("PGVECTOR_SECRET_ARN", "arn:aws:secretsmanager:ap-south-1:201727487159:secret:prajnov/recruiter-F8sc3X")

def _get_db_pass() -> str:
    env_pass = os.getenv("PGVECTOR_PASSWORD", "")
    if env_pass:
        return env_pass
    client = boto3.client("secretsmanager", region_name=REGION)
    secret = json.loads(client.get_secret_value(SecretId=SECRET_ARN)["SecretString"])
    return secret.get("PGVECTOR_PASSWORD", "")

STATE_KEY = "glue-state/brightdata_gold_upsert/last_run.json"
BATCH_SIZE = 2000


def s3():
    return boto3.client("s3", region_name=REGION)


def db():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=_get_db_pass(),
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


# ── Watermark ─────────────────────────────────────────────────────────────────

def get_watermark() -> str:
    try:
        body = s3().get_object(Bucket=BUCKET, Key=STATE_KEY)["Body"].read()
        return json.loads(body)["last_run"]
    except Exception:
        return "1970-01-01T00:00:00+00:00"


def set_watermark(ts: str):
    s3().put_object(
        Bucket=BUCKET, Key=STATE_KEY,
        Body=json.dumps({"last_run": ts}).encode(),
        ContentType="application/json",
    )


# ── Silver loader ─────────────────────────────────────────────────────────────

def list_parquet_keys(prefix: str, watermark: str) -> list:
    client = s3()
    pag    = client.get_paginator("list_objects_v2")
    wm_dt  = datetime.fromisoformat(watermark.replace("Z", "+00:00"))
    keys   = []
    for page in pag.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet") and obj["LastModified"] > wm_dt:
                keys.append(obj["Key"])
    return keys


def read_parquet(keys: list) -> list:
    if not keys:
        return []
    client  = s3()
    records = []
    for key in keys:
        try:
            body  = client.get_object(Bucket=BUCKET, Key=key)["Body"].read()
            table = pq.read_table(io.BytesIO(body))
            records.extend(table.to_pylist())
        except Exception as e:
            logger.warning("read error %s: %s", key, e)
    return records


def jlist(val) -> list:
    """Parquet stores list fields as JSON strings — parse back."""
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            return json.loads(val)
        except Exception:
            return []
    return []


# ── DB helpers ────────────────────────────────────────────────────────────────

def build_row(cand: dict, certs_by, skills_by, exp_by, edu_by) -> tuple:
    cid         = cand["candidate_id"]
    lid         = cand.get("linkedin_id", "") or ""
    uid         = f"bd_{lid}" if lid else f"bd_{cand['candidate_id'][:16]}"
    experience  = exp_by.get(cid, [])
    clean_certs = jlist(cand.get("certifications_clean"))
    skill_names = sorted({r["skill_name"] for r in skills_by.get(cid, [])})
    companies   = sorted({r["company"] for r in experience if r.get("company")})
    exp_text    = " | ".join(
        f"{e['title']} at {e['company']}" for e in experience[:10]
        if e.get("title") and e.get("company")
    )
    return (
        uid,
        cand.get("name", ""), cand.get("first_name", ""), cand.get("last_name", ""),
        lid, cand.get("linkedin_url", ""),
        cand.get("current_title", ""), cand.get("current_company", ""),
        cand.get("city", ""), cand.get("state", ""), cand.get("country", "US"),
        cand.get("location_raw", ""),
        cand.get("about", ""),
        clean_certs,
        skill_names, companies,
        json.dumps(experience), exp_text,
        cand.get("experience_years") or -1,
        json.dumps(edu_by.get(cid, [])),
        cand.get("email", ""), cand.get("phone", ""),
    )


UPSERT_SQL = """
    INSERT INTO candidates (
        uid, name, first_name, last_name,
        linkedin_id, linkedin_url,
        current_title, current_company,
        city, state, country, location_raw,
        about, certifications, verified_certifications,
        cert_verification_status,
        skills, companies,
        experience, experience_text, experience_years,
        education, email, phone,
        source, is_internal,
        added_at, updated_at
    ) VALUES (
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        ARRAY[]::text[], 'unverified',
        %s,%s,%s,%s,%s,%s,%s,%s,
        'brightdata', false,
        now(), now()
    )
    ON CONFLICT (uid) DO UPDATE SET
        current_title   = CASE WHEN EXCLUDED.current_title != ''
                               THEN EXCLUDED.current_title
                               ELSE candidates.current_title END,
        current_company = CASE WHEN EXCLUDED.current_company != ''
                               THEN EXCLUDED.current_company
                               ELSE candidates.current_company END,
        city  = CASE WHEN EXCLUDED.city != '' AND candidates.city = ''
                     THEN EXCLUDED.city ELSE candidates.city END,
        state = CASE WHEN EXCLUDED.state != '' AND candidates.state = ''
                     THEN EXCLUDED.state ELSE candidates.state END,
        country = CASE WHEN EXCLUDED.country != '' AND candidates.country = ''
                       THEN EXCLUDED.country ELSE candidates.country END,
        about = CASE WHEN length(EXCLUDED.about) > length(COALESCE(candidates.about,''))
                     THEN EXCLUDED.about ELSE candidates.about END,
        certifications  = EXCLUDED.certifications,
        skills = CASE
            WHEN cardinality(EXCLUDED.skills) > cardinality(COALESCE(candidates.skills, ARRAY[]::text[]))
            THEN EXCLUDED.skills ELSE candidates.skills END,
        companies       = EXCLUDED.companies,
        experience      = EXCLUDED.experience,
        experience_text = EXCLUDED.experience_text,
        experience_years = CASE WHEN EXCLUDED.experience_years > 0
                                THEN EXCLUDED.experience_years
                                ELSE candidates.experience_years END,
        education       = EXCLUDED.education,
        email = CASE WHEN EXCLUDED.email != '' AND candidates.email = ''
                     THEN EXCLUDED.email ELSE candidates.email END,
        phone = CASE WHEN EXCLUDED.phone != '' AND candidates.phone = ''
                     THEN EXCLUDED.phone ELSE candidates.phone END,
        source = CASE WHEN candidates.source NOT LIKE '%%brightdata%%'
                      THEN candidates.source || '+brightdata'
                      ELSE candidates.source END,
        updated_at = now()
"""


def upsert_batch(conn, rows: list):
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, UPSERT_SQL, rows, page_size=500)
    conn.commit()


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    run_start = datetime.now(timezone.utc).isoformat()
    watermark = get_watermark()
    logger.info("=== brightdata-gold-upsert started | watermark=%s ===", watermark)

    # Load silver
    logger.info("Loading silver Parquet files...")
    cand_keys = list_parquet_keys("silver/candidate/source=brightdata/", watermark)
    cert_keys = list_parquet_keys("silver/candidate_certification/source=brightdata/", watermark)
    skill_keys = list_parquet_keys("silver/candidate_skill/source=brightdata/", watermark)
    exp_keys  = list_parquet_keys("silver/candidate_experience/source=brightdata/", watermark)
    edu_keys  = list_parquet_keys("silver/candidate_education/source=brightdata/", watermark)

    candidates = read_parquet(cand_keys)
    logger.info("Silver candidates to process: %d", len(candidates))

    if not candidates:
        logger.info("Nothing to process.")
        return

    # Index related tables by candidate_id
    certs_by  = defaultdict(list)
    skills_by = defaultdict(list)
    exp_by    = defaultdict(list)
    edu_by    = defaultdict(list)

    for r in read_parquet(cert_keys):
        certs_by[r["candidate_id"]].append(r)
    for r in read_parquet(skill_keys):
        skills_by[r["candidate_id"]].append(r)
    for r in read_parquet(exp_keys):
        exp_by[r["candidate_id"]].append(r)
    for r in read_parquet(edu_keys):
        edu_by[r["candidate_id"]].append(r)

    conn = db()
    total = len(candidates)
    processed = 0

    for batch_start in range(0, total, BATCH_SIZE):
        batch = candidates[batch_start:batch_start + BATCH_SIZE]
        rows = [build_row(c, certs_by, skills_by, exp_by, edu_by) for c in batch]
        try:
            upsert_batch(conn, rows)
        except Exception as e:
            conn.rollback()
            logger.warning("batch error at %d: %s", batch_start, e)
            continue
        processed += len(batch)
        logger.info("  %d/%d upserted", processed, total)

    conn.close()
    set_watermark(run_start)
    logger.info("=== brightdata-gold-upsert done | processed=%d ===", processed)


if __name__ == "__main__":
    run()
