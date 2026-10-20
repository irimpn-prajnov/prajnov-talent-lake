"""
Silver → Gold pipeline.
Reads all 5 silver tables from S3, assembles Candidate 360,
writes to Aurora PostgreSQL gold tables.

Usage:
  python processing/gold_profiles.py --cert CIS-CSM
  python processing/gold_profiles.py --cert CIS-VR
  python processing/gold_profiles.py  # all certs

Requires:
  pip install psycopg2-binary boto3
  env: DATABASE_URL=postgresql://user:pass@host:5432/prajnov
       or AWS_SECRET_ARN pointing to prajnov/aurora/credentials
"""
import argparse, json, os, sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import boto3
import psycopg2
import psycopg2.extras

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion"))
from common import BRONZE_BUCKET

BUCKET = BRONZE_BUCKET


# ── DB connection ─────────────────────────────────────────────────────────────

def get_db_url():
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    # Fall back to Secrets Manager
    secret_arn = os.getenv("AWS_SECRET_ARN", "prajnov/aurora/credentials")
    sm = boto3.client("secretsmanager", region_name="ap-south-1")
    secret = json.loads(sm.get_secret_value(SecretId=secret_arn)["SecretString"])
    return f"postgresql://{secret['username']}:{secret['password']}@{secret['host']}:{secret['port']}/{secret['dbname']}"


def get_conn():
    return psycopg2.connect(get_db_url(), cursor_factory=psycopg2.extras.RealDictCursor)


# ── S3 readers ────────────────────────────────────────────────────────────────

def s3():
    from botocore.session import get_session
    bs = get_session()
    bs.set_config_variable("profile", os.getenv("AWS_PROFILE", "prajnov"))
    return boto3.Session(botocore_session=bs).client("s3", region_name="ap-south-1")


def list_keys(prefix):
    pag = s3().get_paginator("list_objects_v2")
    return [
        o["Key"] for page in pag.paginate(Bucket=BUCKET, Prefix=prefix)
        for o in page.get("Contents", [])
        if o["Key"].endswith(".json") and "_manifests" not in o["Key"]
    ]


def load_table(prefix):
    rows = []
    for key in list_keys(prefix):
        try:
            rows.append(json.loads(s3().get_object(Bucket=BUCKET, Key=key)["Body"].read()))
        except Exception as e:
            print(f"  [WARN] skip {key}: {e}")
    return rows


# ── Data quality score ────────────────────────────────────────────────────────

def quality_score(c, has_linkedin, has_skills, has_exp):
    score = 0.0
    if c.get("canonical_name"):   score += 0.10
    if c.get("current_title"):    score += 0.10
    if c.get("current_company"):  score += 0.10
    if c.get("location_city"):    score += 0.05
    if c.get("location_state"):   score += 0.05
    if has_linkedin:               score += 0.20
    if c.get("credly_url"):        score += 0.10
    if c.get("summary"):           score += 0.05
    if c.get("years_experience"):  score += 0.05
    if has_skills:                 score += 0.10
    if has_exp:                    score += 0.10
    return round(score, 2)


# ── Gold writer ───────────────────────────────────────────────────────────────

def upsert_candidate(cur, c, skills, experience, education, certs):
    has_linkedin = bool(c.get("linkedin_url"))
    has_skills   = len(skills) > 0
    has_exp      = len(experience) > 0
    dq           = quality_score(c, has_linkedin, has_skills, has_exp)

    cur.execute("""
        INSERT INTO candidate (
            id, canonical_name, first_name, last_name,
            current_title, current_company, years_experience, summary,
            data_quality_score, discovery_status, openai_confidence,
            enriched_by, source_cert, badge_count, created_at, updated_at
        ) VALUES (
            %(id)s, %(canonical_name)s, %(first_name)s, %(last_name)s,
            %(current_title)s, %(current_company)s, %(years_experience)s, %(summary)s,
            %(dq)s, %(discovery_status)s, %(openai_confidence)s,
            %(enriched_by)s, %(source_cert)s, %(badge_count)s, now(), now()
        )
        ON CONFLICT (id) DO UPDATE SET
            current_title       = EXCLUDED.current_title,
            current_company     = EXCLUDED.current_company,
            years_experience    = EXCLUDED.years_experience,
            summary             = EXCLUDED.summary,
            data_quality_score  = EXCLUDED.data_quality_score,
            discovery_status    = EXCLUDED.discovery_status,
            openai_confidence   = EXCLUDED.openai_confidence,
            updated_at          = now()
    """, {
        "id":               c["candidate_id"],
        "canonical_name":   c.get("canonical_name", ""),
        "first_name":       c.get("first_name", ""),
        "last_name":        c.get("last_name", ""),
        "current_title":    c.get("current_title", ""),
        "current_company":  c.get("current_company", ""),
        "years_experience": c.get("years_experience"),
        "summary":          c.get("summary", ""),
        "dq":               dq,
        "discovery_status": c.get("discovery_status", "discovered"),
        "openai_confidence": c.get("openai_confidence", ""),
        "enriched_by":      c.get("enriched_by", ""),
        "source_cert":      c.get("source_cert", ""),
        "badge_count":      c.get("badge_count", 0),
    })

    cid = c["candidate_id"]

    # Location
    if c.get("location_city") or c.get("location_state") or c.get("location_raw"):
        cur.execute("""
            INSERT INTO candidate_location (candidate_id, city, state, country, location_raw, source, confidence)
            VALUES (%s, %s, %s, %s, %s, 'openai_web_search', 0.75)
            ON CONFLICT DO NOTHING
        """, (cid, c.get("location_city",""), c.get("location_state",""),
              c.get("location_country",""), c.get("location_raw","")))

    # External identities
    for platform, url, ext_id, discovered_from, verified in [
        ("credly",     c.get("credly_url"),     c.get("credly_username"), "credly_directory",  True),
        ("linkedin",   c.get("linkedin_url"),   None,                     "openai_web_search", False),
        ("signalhire", c.get("signalhire_url"), c.get("signalhire_uid"),  "signalhire_search", False),
    ]:
        if url:
            cur.execute("""
                INSERT INTO candidate_external_identity
                    (candidate_id, platform, profile_url, external_id, discovered_from, is_verified)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (candidate_id, platform) DO UPDATE SET
                    profile_url = EXCLUDED.profile_url,
                    external_id = EXCLUDED.external_id
            """, (cid, platform, url, ext_id, discovered_from, verified))

    # Certifications
    for cert in certs:
        cur.execute("""
            INSERT INTO candidate_certification
                (candidate_id, cert_code, cert_name, issuer, issued_date,
                 verification_url, verification_status, source, last_verified)
            VALUES (%s, %s, %s, %s, %s::date, %s, %s, %s, now())
            ON CONFLICT DO NOTHING
        """, (
            cid,
            cert.get("cert_code", ""),
            cert.get("cert_name", ""),
            cert.get("issuer", "ServiceNow"),
            cert.get("issued_date") or None,
            cert.get("verification_url", ""),
            cert.get("verification_status", "verified"),
            cert.get("source", "credly"),
        ))

    # Skills
    for sk in skills:
        cur.execute("""
            INSERT INTO candidate_skill
                (candidate_id, skill_name, raw_name, is_inferred, sources, confidence, evidence)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (
            cid,
            sk.get("skill_name", ""),
            sk.get("raw_name", ""),
            sk.get("is_inferred", False),
            sk.get("sources", []),
            sk.get("confidence", 0.0),
            json.dumps(json.loads(sk["evidence"])) if sk.get("evidence") else None,
        ))

    # Experience
    for exp in experience:
        cur.execute("""
            INSERT INTO candidate_experience
                (candidate_id, company, title, start_year, end_year, is_current, location, description, source, confidence)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (
            cid,
            exp.get("company", ""),
            exp.get("title", ""),
            exp.get("start_year"),
            exp.get("end_year"),
            exp.get("is_current", False),
            exp.get("location", ""),
            exp.get("description", ""),
            exp.get("source", ""),
            exp.get("confidence", 0.0),
        ))

    # Education
    for edu in education:
        cur.execute("""
            INSERT INTO candidate_education
                (candidate_id, institution, degree, field, start_year, end_year, source)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (
            cid,
            edu.get("institution", ""),
            edu.get("degree", ""),
            edu.get("field", ""),
            edu.get("start_year"),
            edu.get("end_year"),
            edu.get("source", ""),
        ))

    return dq


# ── Main ──────────────────────────────────────────────────────────────────────

def run(cert_filter, dry_run):
    print(f"\n{'='*60}")
    print(f"  Silver → Gold: {cert_filter or 'all certs'} | dry_run={dry_run}")
    print(f"{'='*60}\n")

    cert_prefix = f"silver/candidate/source_cert={cert_filter}/" if cert_filter else "silver/candidate/"
    candidates  = load_table(cert_prefix)
    print(f"Silver candidates loaded : {len(candidates)}")
    if not candidates:
        print("No silver candidates — run enrich_linkedin.py first.")
        return

    cert_p  = f"silver/candidate_certification/cert_code={cert_filter}/" if cert_filter else "silver/candidate_certification/"
    skill_p = f"silver/candidate_skill/source_cert={cert_filter}/"       if cert_filter else "silver/candidate_skill/"
    exp_p   = f"silver/candidate_experience/source_cert={cert_filter}/"  if cert_filter else "silver/candidate_experience/"
    edu_p   = f"silver/candidate_education/source_cert={cert_filter}/"   if cert_filter else "silver/candidate_education/"

    certs_by      = defaultdict(list)
    skills_by     = defaultdict(list)
    experience_by = defaultdict(list)
    education_by  = defaultdict(list)

    for r in load_table(cert_p):  certs_by[r["candidate_id"]].append(r)
    for r in load_table(skill_p): skills_by[r["candidate_id"]].append(r)
    for r in load_table(exp_p):   experience_by[r["candidate_id"]].append(r)
    for r in load_table(edu_p):   education_by[r["candidate_id"]].append(r)

    print(f"  certs={sum(len(v) for v in certs_by.values())} "
          f"skills={sum(len(v) for v in skills_by.values())} "
          f"exp={sum(len(v) for v in experience_by.values())} "
          f"edu={sum(len(v) for v in education_by.values())}\n")

    if dry_run:
        print("[DRY-RUN] Would write to Aurora. Showing first 5 candidates:\n")
        for c in candidates[:5]:
            cid = c["candidate_id"]
            print(f"  {c.get('canonical_name','?'):<35} | "
                  f"li={'✓' if c.get('linkedin_url') else '✗'} | "
                  f"sk={len(skills_by.get(cid,[]))} "
                  f"exp={len(experience_by.get(cid,[]))} "
                  f"edu={len(education_by.get(cid,[]))}")
        return

    conn = get_conn()
    stats = {"written": 0, "errors": 0}

    try:
        with conn:
            with conn.cursor() as cur:
                for c in candidates:
                    cid = c["candidate_id"]
                    try:
                        dq = upsert_candidate(
                            cur, c,
                            skills_by.get(cid, []),
                            experience_by.get(cid, []),
                            education_by.get(cid, []),
                            certs_by.get(cid, []),
                        )
                        stats["written"] += 1
                        li  = "✓ LI" if c.get("linkedin_url") else "✗ LI"
                        sk  = len(skills_by.get(cid, []))
                        exp = len(experience_by.get(cid, []))
                        print(f"  {c.get('canonical_name','?'):<35} | {li} | sk={sk} exp={exp} | dq={dq}")
                    except Exception as e:
                        print(f"  [ERROR] {c.get('canonical_name', cid)}: {e}")
                        stats["errors"] += 1
    finally:
        conn.close()

    print(f"\n{'='*60}")
    print(f"  DONE | written={stats['written']} errors={stats['errors']}")
    print(f"  Aurora: postgresql://.../{BUCKET.split('-')[0]}")
    print(f"  Query:  SELECT * FROM v_candidate_us WHERE source_cert = '{cert_filter or 'CIS-CSM'}';")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cert",    default=None)
    parser.add_argument("--dry-run", action="store_true")
    args, _ = parser.parse_known_args()
    run(args.cert, args.dry_run)
