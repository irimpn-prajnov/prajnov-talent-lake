"""
Glue Job: gold-merge
Entity resolution + gold upsert into pgvector candidates table.

Rules:
  1. Dedup key: linkedin_id (primary), name+state (fallback)
  2. Credly source wins on: verified_certifications, cert_verification_status
  3. BrightData wins on: about, experience, experience_text, experience_years,
                         education, skills, current_title, current_company
  4. Either source wins on: city, state (first non-empty value)
  5. verified_certifications[] is the ONLY cert field AIR should use
  6. certifications[] kept as raw claims for audit — never used for matching

Schedule: Weekly (Sunday 04:00 UTC) — runs after cert-verify job
"""
import json, logging, os, sys, re
from datetime import datetime, timezone

import psycopg2, psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

def _arg(name, default=""):
    key = f"--{name}"
    try:
        idx = sys.argv.index(key)
        return sys.argv[idx + 1]
    except (ValueError, IndexError):
        return os.getenv(name, default)

DB_HOST = _arg("PGVECTOR_HOST",     "ai-recruiter.c7q8sg00wqte.ap-south-1.rds.amazonaws.com")
DB_NAME = _arg("PGVECTOR_DB",       "recruiter")
DB_USER = _arg("PGVECTOR_USER",     "prajnov")
DB_PASS = _arg("PGVECTOR_PASSWORD", "")


def db_conn():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


def fetch_all_candidates(conn) -> dict:
    """Load all candidates grouped by linkedin_id for entity resolution."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT uid, name, first_name, last_name, linkedin_id, linkedin_url,
                   current_title, current_company, city, state, country, location_raw,
                   about, certifications, verified_certifications, cert_verification_status,
                   cert_verification_sources, companies, industries,
                   experience, experience_text, experience_years, education,
                   skills, email, phone, source, is_internal,
                   signalhire_enriched, signalhire_data,
                   sn_portal_verified, sn_portal_certs,
                   added_at, updated_at
            FROM candidates
            ORDER BY
                CASE source
                    WHEN 'credly'     THEN 1
                    WHEN 'signalhire' THEN 2
                    WHEN 'brightdata' THEN 3
                    ELSE 4
                END
        """)
        rows = cur.fetchall()

    # Group by linkedin_id
    by_lid   = {}  # linkedin_id → list of rows
    no_lid   = []  # rows without linkedin_id

    for row in rows:
        lid = (row["linkedin_id"] or "").strip().lower()
        if lid:
            by_lid.setdefault(lid, []).append(row)
        else:
            no_lid.append(row)

    return by_lid, no_lid


def merge_candidate_group(rows: list) -> dict:
    """
    Merge multiple source records for the same person into one gold record.
    Priority: credly > signalhire > brightdata > resume_upload
    """
    # rows already sorted by source priority (from SQL ORDER BY)
    primary = rows[0]

    def best(field, *sources_in_order):
        for row in rows:
            if row["source"] in sources_in_order and row.get(field):
                return row[field]
        for row in rows:
            if row.get(field):
                return row[field]
        return primary.get(field)

    # Merge verified_certifications from ALL sources
    all_verified = set()
    for row in rows:
        for c in (row["verified_certifications"] or []):
            all_verified.add(c)

    # Merge raw certifications (for audit only)
    all_raw_certs = set()
    for row in rows:
        for c in (row["certifications"] or []):
            all_raw_certs.add(c)

    # Merge skills
    all_skills = set()
    for row in rows:
        for s in (row["skills"] or []):
            all_skills.add(s)

    # Merge companies
    all_companies = set()
    for row in rows:
        for c in (row["companies"] or []):
            all_companies.add(c)

    # Best experience: longest experience_text wins
    best_exp_row = max(rows, key=lambda r: len(r.get("experience_text") or ""))

    # Cert verification status: verified > checked_not_found > unverified
    status_priority = {"verified": 3, "checked_not_found": 2, "unverified": 1}
    best_status = max(
        (row.get("cert_verification_status") or "unverified" for row in rows),
        key=lambda s: status_priority.get(s, 0),
    )

    # Merge cert_verification_sources
    merged_sources = {}
    for row in rows:
        for k, v in (row.get("cert_verification_sources") or {}).items():
            merged_sources[k] = v

    # Data quality score
    score = 0.0
    merged = {
        "uid":                      primary["uid"],
        "name":                     best("name"),
        "first_name":               best("first_name"),
        "last_name":                best("last_name"),
        "linkedin_id":              best("linkedin_id"),
        "linkedin_url":             best("linkedin_url"),
        "current_title":            best("current_title", "brightdata", "signalhire", "credly"),
        "current_company":          best("current_company", "brightdata", "signalhire", "credly"),
        "city":                     best("city"),
        "state":                    best("state"),
        "country":                  best("country") or "US",
        "location_raw":             best("location_raw"),
        "about":                    best_exp_row.get("about") or best("about"),
        "certifications":           sorted(all_raw_certs),       # raw claims — audit only
        "verified_certifications":  sorted(all_verified),        # TRUSTED — AIR uses this
        "cert_verification_status": best_status,
        "cert_verification_sources": merged_sources,
        "companies":                sorted(all_companies),
        "industries":               list({i for r in rows for i in (r.get("industries") or [])}),
        "experience":               best_exp_row.get("experience") or [],
        "experience_text":          best_exp_row.get("experience_text") or "",
        "experience_years":         best("experience_years", "brightdata", "signalhire"),
        "education":                best("education", "brightdata", "signalhire"),
        "skills":                   sorted(all_skills),
        "email":                    best("email", "signalhire", "brightdata"),
        "phone":                    best("phone", "signalhire"),
        "source":                   "+".join(sorted({r["source"] for r in rows})),
        "is_internal":              any(r.get("is_internal") for r in rows),
        "sn_portal_verified":       any(r.get("sn_portal_verified") for r in rows),
        "sn_portal_certs":          best("sn_portal_certs"),
    }

    # Data quality score
    if merged.get("name"):              score += 0.10
    if merged.get("current_title"):     score += 0.10
    if merged.get("current_company"):   score += 0.10
    if merged.get("state"):             score += 0.05
    if merged.get("linkedin_url"):      score += 0.15
    if merged.get("verified_certifications"): score += 0.25
    if merged.get("experience_text"):   score += 0.10
    if merged.get("skills"):            score += 0.05
    if merged.get("email"):             score += 0.10
    merged["data_quality_score"] = round(score, 2)

    return merged


def upsert_gold(conn, merged: dict):
    """Upsert merged candidate into candidates table."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE candidates SET
                verified_certifications   = %s,
                cert_verification_status  = %s,
                cert_verification_sources = %s,
                current_title    = CASE WHEN %s != '' THEN %s ELSE current_title END,
                current_company  = CASE WHEN %s != '' THEN %s ELSE current_company END,
                about            = CASE WHEN length(%s) > length(COALESCE(about,'')) THEN %s ELSE about END,
                experience_text  = CASE WHEN length(%s) > length(COALESCE(experience_text,'')) THEN %s ELSE experience_text END,
                experience_years = CASE WHEN %s > 0 THEN %s ELSE experience_years END,
                skills           = CASE WHEN cardinality(%s) > cardinality(COALESCE(skills,'{}')) THEN %s ELSE skills END,
                companies        = %s,
                industries       = %s,
                email            = CASE WHEN %s != '' THEN %s ELSE email END,
                source           = %s,
                updated_at       = now()
            WHERE uid = %s
        """, (
            merged["verified_certifications"],
            merged["cert_verification_status"],
            json.dumps(merged["cert_verification_sources"]),
            merged["current_title"] or "", merged["current_title"] or "",
            merged["current_company"] or "", merged["current_company"] or "",
            merged["about"] or "", merged["about"] or "",
            merged["experience_text"] or "", merged["experience_text"] or "",
            merged["experience_years"] or 0, merged["experience_years"] or 0,
            merged["skills"], merged["skills"],
            merged["companies"],
            merged["industries"],
            merged["email"] or "", merged["email"] or "",
            merged["source"],
            merged["uid"],
        ))


def mark_duplicate(conn, duplicate_uid: str, canonical_uid: str):
    """Mark a duplicate candidate — keep canonical, flag duplicate."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE candidates SET
                source   = source || '+duplicate',
                updated_at = now()
            WHERE uid = %s AND uid != %s
        """, (duplicate_uid, canonical_uid))


def run():
    logger.info("=== gold-merge job started ===")
    conn = db_conn()

    by_lid, no_lid = fetch_all_candidates(conn)
    logger.info("Unique linkedin_ids: %d | No linkedin_id: %d",
                len(by_lid), len(no_lid))

    merged_count = deduped_count = 0

    for lid, rows in by_lid.items():
        try:
            if len(rows) > 1:
                # Multiple sources for same person — merge
                merged = merge_candidate_group(rows)
                # Canonical = first row (highest priority source)
                canonical_uid = rows[0]["uid"]
                upsert_gold(conn, {**merged, "uid": canonical_uid})
                # Mark duplicates
                for dup_row in rows[1:]:
                    if dup_row["uid"] != canonical_uid:
                        mark_duplicate(conn, dup_row["uid"], canonical_uid)
                        deduped_count += 1
                merged_count += 1
            else:
                # Single source — just ensure verified_certifications is set
                row    = rows[0]
                merged = merge_candidate_group([row])
                upsert_gold(conn, merged)
                merged_count += 1
        except Exception as e:
            logger.error("Error merging lid=%s: %s", lid, e)

    conn.commit()
    conn.close()

    logger.info("=== gold-merge done ===")
    logger.info("  Merged/updated : %d", merged_count)
    logger.info("  Duplicates     : %d", deduped_count)
    logger.info("")
    logger.info("AIR query — verified cert holders only:")
    logger.info("  SELECT name, current_title, state, verified_certifications")
    logger.info("  FROM candidates")
    logger.info("  WHERE verified_certifications @> ARRAY['cis-csm']")
    logger.info("    AND cert_verification_status = 'verified'")
    logger.info("    AND state = 'texas';")


if __name__ == "__main__":
    run()
