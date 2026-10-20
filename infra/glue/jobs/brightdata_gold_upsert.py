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

import boto3, psycopg2, psycopg2.extras
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
DB_PASS  = _arg("PGVECTOR_PASSWORD", "")
STATE_KEY = "glue-state/brightdata_gold_upsert/last_run.json"
COMMIT_EVERY = 500


def s3():
    return boto3.client("s3", region_name=REGION)


def db():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS,
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

def load_existing_linkedin_ids(conn) -> set:
    with conn.cursor() as cur:
        cur.execute("SELECT linkedin_id FROM candidates WHERE linkedin_id IS NOT NULL AND linkedin_id != ''")
        return {row["linkedin_id"] for row in cur.fetchall()}


def upsert_candidate(conn, cand: dict, certs: list, skills: list,
                     experience: list, education: list, is_new: bool):
    lid = cand.get("linkedin_id", "")
    uid = f"bd_{lid}" if lid else f"bd_{cand['candidate_id'][:16]}"

    raw_certs   = jlist(cand.get("certifications_raw"))
    clean_certs = jlist(cand.get("certifications_clean"))
    skill_names = sorted({r["skill_name"] for r in skills})
    companies   = sorted({r["company"] for r in experience if r.get("company")})
    exp_text    = " | ".join(
        f"{e['title']} at {e['company']}" for e in experience[:10]
        if e.get("title") and e.get("company")
    )

    with conn.cursor() as cur:
        cur.execute("""
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
        """, (
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
            json.dumps(education),
            cand.get("email", ""), cand.get("phone", ""),
        ))


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
    logger.info("Loading existing linkedin_ids from DB...")
    existing_lids = load_existing_linkedin_ids(conn)
    logger.info("Existing candidates in DB: %d", len(existing_lids))

    stats = {"new": 0, "updated": 0, "errors": 0}

    for i, cand in enumerate(candidates):
        cid = cand["candidate_id"]
        lid = cand.get("linkedin_id", "")
        is_new = lid not in existing_lids

        try:
            upsert_candidate(
                conn, cand,
                certs_by.get(cid, []),
                skills_by.get(cid, []),
                exp_by.get(cid, []),
                edu_by.get(cid, []),
                is_new,
            )
            if is_new:
                stats["new"] += 1
                if lid:
                    existing_lids.add(lid)
            else:
                stats["updated"] += 1
        except Exception as e:
            conn.rollback()
            stats["errors"] += 1
            if stats["errors"] <= 5:
                logger.warning("upsert error lid=%s: %s", lid, e)
            continue

        if (i + 1) % COMMIT_EVERY == 0:
            conn.commit()
            logger.info("  %d/%d processed (new=%d updated=%d errors=%d)",
                        i + 1, len(candidates), stats["new"], stats["updated"], stats["errors"])

    conn.commit()
    conn.close()
    set_watermark(run_start)

    logger.info("=== brightdata-gold-upsert done ===")
    logger.info("  new=%d  updated=%d  errors=%d", stats["new"], stats["updated"], stats["errors"])
    logger.info("  Silver classification:")
    logger.info("    new candidates (not in DB)     : %d", stats["new"])
    logger.info("    updated candidates (enriched)  : %d", stats["updated"])
    logger.info("  Note: verified_certifications[] untouched — Credly owns that field")


if __name__ == "__main__":
    run()
