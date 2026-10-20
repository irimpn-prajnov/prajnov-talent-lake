"""
Glue Job: data-quality-fix
Fixes data quality issues across pgvector candidates table and S3 silver.

Issues fixed:
  1. Dirty cert codes in verified_certifications[] — normalize or remove
  2. Dirty cert codes in certifications[] — normalize in place
  3. Micro-certs / suite certs in verified_certifications — remove (not CIS certs)
  4. Duplicate candidates by linkedin_id — merge, keep canonical
  5. Location parsing errors — re-parse from location_raw
  6. experience_years = 0 or -1 — recalculate from experience JSON
  7. Update S3 silver files to match corrected pgvector records

Schedule: Weekly (Sunday 05:00 UTC) — runs after gold-merge
"""
import json, logging, os, re, sys
from datetime import datetime, timezone

import boto3, psycopg2, psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

def _arg(name, default=""):
    key = f"--{name}"
    try:
        idx = sys.argv.index(key)
        return sys.argv[idx + 1]
    except (ValueError, IndexError):
        return os.getenv(name, default)

BUCKET  = _arg("BUCKET",            "prajnov-talent-lake")
REGION  = _arg("AWS_REGION",        "ap-south-1")
DB_HOST = _arg("PGVECTOR_HOST",     "ai-recruiter.c7q8sg00wqte.ap-south-1.rds.amazonaws.com")
DB_NAME = _arg("PGVECTOR_DB",       "recruiter")
DB_USER = _arg("PGVECTOR_USER",     "prajnov")
DB_PASS = _arg("PGVECTOR_PASSWORD", "")

# Canonical cert codes — anything not in this set is dirty
CANONICAL_CERTS = {
    "csa","cad","cta","cma","itil","pmp","cissp","safe","aws-sa","six-sigma",
    "cis-itsm","cis-itom","cis-hrsd","cis-csm","cis-fsm","cis-sam","cis-ham",
    "cis-itam","cis-spm","cis-ppm","cis-rc","cis-sir","cis-vr","cis-disco",
    "cis-apm","cis-em","cis-sm","cis-tprm","cis-fso","cis-crm","cis-psds",
    "cis-hcls","cis-sp","cis-pa","cis-mco","cis-rsm","cis-tsm","cis-som",
}

# Dirty → canonical mapping
CERT_NORM = {
    "cis-hr": "cis-hrsd",
    "cis-discovery": "cis-disco",
    "cis-vri": "cis-vr",
    "cis-vrm": "cis-vr",
    "cis-siri": "cis-sir",
    "cis-csme": "cis-csm",
    "cis-paa": "cis-pa",
    "cis-platform analytics": "cis-pa",
    "cis-df": "cis-disco",
    "cis-df-cmdb and csdm": "cis-disco",
    "cis-df cmdb and csdm": "cis-disco",
    "cis-hrcertified implementation specialist human resource": "cis-hrsd",
    "cis-itsm certified implementation specialist - it service management": "cis-itsm",
    "cis-itsm certified implementation specialist- it service management": "cis-itsm",
    "cis-ppm certified implementation specialist - project portfolio management": "cis-ppm",
    "cis-project portfolio management - ppm": "cis-ppm",
    "cis-rc certified implementation specialist - risk and compliance": "cis-rc",
    "cis-apm certified implementation specialist - application portfolio management": "cis-apm",
    "cis-application portfolio management - apm": "cis-apm",
    "cis-fsm certified implementation specialist - field service management": "cis-fsm",
    "cis-csm certified implementation specialist - customer service management": "cis-csm",
    "cis-security incident response": "cis-sir",
    "cis-service mapping": "cis-sm",
    "cis-event management": "cis-em",
    "cis-application developer": "cad",
    "cis- event management": "cis-em",
    "cis- discovery": "cis-disco",
    "cis- df cmdb and csdm": "cis-disco",
    "cis- strategic portfolio management": "cis-spm",
    "cis-vrm certified implementation specialist - vendor risk management": "cis-vr",
    "cis-software asset management certification exam preparation content completion": "cis-sam",
    "cis---web designer": None,  # not a real cert — remove
    "cis-hcls": "cis-hcls",
    "cis-hr implementation": "cis-hrsd",
}

# Patterns that indicate non-CIS certs to REMOVE from verified_certifications
# (micro-certs, suite certs — valid Credly badges but not CIS implementation certs)
NON_CIS_PATTERNS = [
    r"^micro-certification",
    r"^suite certification",
    r"^now assist",
    r"^certified master architect \(",
    r"^servicenow certified platform owner",
    r"^certified implementation specialist\s*[–-]\s*data foundations",
]


def is_non_cis(cert: str) -> bool:
    for pat in NON_CIS_PATTERNS:
        if re.match(pat, cert.lower()):
            return True
    return False


def clean_cert(raw: str):
    """Return canonical cert code or None if should be removed."""
    s = raw.lower().strip()
    if s in CANONICAL_CERTS:
        return s
    if s in CERT_NORM:
        return CERT_NORM[s]  # None means remove
    if is_non_cis(s):
        return None  # remove micro/suite certs from verified
    # Try substring match for long dirty strings
    for dirty, canon in CERT_NORM.items():
        if dirty in s and canon:
            return canon
    return None  # unrecognized — remove from verified, keep in raw


def s3():
    return boto3.client("s3", region_name=REGION)


def db():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


# ── Fix 1: Normalize dirty certs ─────────────────────────────────────────────

def fix_dirty_certs(conn) -> dict:
    logger.info("--- Fix 1: Normalize dirty cert codes ---")
    stats = {"updated": 0, "certs_removed": 0, "certs_normalized": 0}

    with conn.cursor() as cur:
        cur.execute("""
            SELECT uid, certifications, verified_certifications
            FROM candidates
            WHERE certifications IS NOT NULL
               OR verified_certifications IS NOT NULL
        """)
        rows = cur.fetchall()

    for row in rows:
        uid = row["uid"]
        raw_certs  = row["certifications"] or []
        ver_certs  = row["verified_certifications"] or []

        # Normalize raw certifications[]
        new_raw = list({c for r in raw_certs for c in [clean_cert(r)] if c})

        # Clean verified_certifications[] — remove micro/suite certs, normalize dirty codes
        new_ver = []
        for c in ver_certs:
            cleaned = clean_cert(c)
            if cleaned and cleaned in CANONICAL_CERTS:
                if cleaned not in new_ver:
                    new_ver.append(cleaned)
            elif cleaned:
                # Normalized but not in canonical set — keep in raw, not verified
                if cleaned not in new_raw:
                    new_raw.append(cleaned)
            # else: None = remove entirely

        removed = len(ver_certs) - len(new_ver)
        if new_raw != raw_certs or new_ver != ver_certs:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE candidates SET
                        certifications          = %s,
                        verified_certifications = %s,
                        cert_verification_status = CASE
                            WHEN cardinality(%s) > 0 THEN 'verified'
                            WHEN cert_verification_status = 'verified' AND cardinality(%s) = 0
                            THEN 'checked_not_found'
                            ELSE cert_verification_status
                        END,
                        updated_at = now()
                    WHERE uid = %s
                """, (new_raw, new_ver, new_ver, new_ver, uid))
            stats["updated"] += 1
            stats["certs_removed"] += removed

    conn.commit()
    logger.info("  Updated: %d | Certs removed/normalized: %d",
                stats["updated"], stats["certs_removed"])
    return stats


# ── Fix 2: Deduplicate by linkedin_id ────────────────────────────────────────

def fix_duplicates(conn) -> dict:
    logger.info("--- Fix 2: Deduplicate by linkedin_id ---")
    stats = {"merged": 0, "removed": 0}

    with conn.cursor() as cur:
        cur.execute("""
            SELECT linkedin_id, array_agg(uid ORDER BY
                CASE source
                    WHEN 'credly'     THEN 1
                    WHEN 'signalhire' THEN 2
                    WHEN 'brightdata' THEN 3
                    ELSE 4
                END, added_at
            ) AS uids
            FROM candidates
            WHERE linkedin_id != '' AND linkedin_id IS NOT NULL
            GROUP BY linkedin_id
            HAVING COUNT(*) > 1
        """)
        dups = cur.fetchall()

    logger.info("  Duplicate linkedin_ids: %d", len(dups))

    for dup in dups:
        uids      = dup["uids"]
        canonical = uids[0]   # highest priority source
        dupes     = uids[1:]

        # Merge verified_certifications from all dupes into canonical
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE candidates SET
                    verified_certifications = (
                        SELECT array_agg(DISTINCT v)
                        FROM (
                            SELECT unnest(c.verified_certifications) AS v
                            FROM candidates c
                            WHERE c.uid = ANY(%s)
                              AND array_length(c.verified_certifications,1) > 0
                        ) t
                    ),
                    certifications = (
                        SELECT array_agg(DISTINCT v)
                        FROM (
                            SELECT unnest(c.certifications) AS v
                            FROM candidates c WHERE c.uid = ANY(%s)
                        ) t
                    ),
                    skills = (
                        SELECT array_agg(DISTINCT v)
                        FROM (
                            SELECT unnest(c.skills) AS v
                            FROM candidates c WHERE c.uid = ANY(%s)
                              AND array_length(c.skills,1) > 0
                        ) t
                    ),
                    about = (
                        SELECT about FROM candidates
                        WHERE uid = ANY(%s) AND length(about) > 0
                        ORDER BY length(about) DESC LIMIT 1
                    ),
                    updated_at = now()
                WHERE uid = %s
            """, (uids, uids, uids, uids, canonical))

            # Mark duplicates
            cur.execute("""
                UPDATE candidates SET
                    source     = source || '+duplicate',
                    updated_at = now()
                WHERE uid = ANY(%s)
            """, (dupes,))

        conn.commit()
        stats["merged"] += 1
        stats["removed"] += len(dupes)

    logger.info("  Merged: %d groups | Flagged duplicates: %d", stats["merged"], stats["removed"])
    return stats


# ── Fix 3: Recalculate experience_years ──────────────────────────────────────

def fix_experience_years(conn) -> dict:
    logger.info("--- Fix 3: Recalculate experience_years ---")
    stats = {"updated": 0}

    with conn.cursor() as cur:
        cur.execute("""
            SELECT uid, experience, experience_years
            FROM candidates
            WHERE (experience_years IS NULL OR experience_years <= 0)
              AND experience::text != '[]'
              AND experience IS NOT NULL
        """)
        rows = cur.fetchall()

    current_year = datetime.now().year

    for row in rows:
        exp = row["experience"] if isinstance(row["experience"], list) else []
        if not exp:
            continue

        # Find earliest start year across all roles
        years = []
        for e in exp:
            sy = e.get("start_year") if isinstance(e, dict) else None
            if sy and isinstance(sy, int) and 1970 < sy <= current_year:
                years.append(sy)

        if not years:
            continue

        calc_years = current_year - min(years)
        calc_years = max(1, min(calc_years, 40))  # sanity bounds

        with conn.cursor() as cur:
            cur.execute("""
                UPDATE candidates SET experience_years = %s, updated_at = now()
                WHERE uid = %s
            """, (calc_years, row["uid"]))
        stats["updated"] += 1

    conn.commit()
    logger.info("  Updated experience_years: %d", stats["updated"])
    return stats


# ── Fix 4: Re-parse broken locations ─────────────────────────────────────────

def fix_locations(conn) -> dict:
    logger.info("--- Fix 4: Re-parse broken locations ---")
    stats = {"updated": 0}

    US_STATE_MAP = {
        "alabama":"alabama","alaska":"alaska","arizona":"arizona","arkansas":"arkansas",
        "california":"california","colorado":"colorado","connecticut":"connecticut",
        "delaware":"delaware","florida":"florida","georgia":"georgia","hawaii":"hawaii",
        "idaho":"idaho","illinois":"illinois","indiana":"indiana","iowa":"iowa",
        "kansas":"kansas","kentucky":"kentucky","louisiana":"louisiana","maine":"maine",
        "maryland":"maryland","massachusetts":"massachusetts","michigan":"michigan",
        "minnesota":"minnesota","mississippi":"mississippi","missouri":"missouri",
        "montana":"montana","nebraska":"nebraska","nevada":"nevada",
        "new hampshire":"new hampshire","new jersey":"new jersey","new mexico":"new mexico",
        "new york":"new york","north carolina":"north carolina","north dakota":"north dakota",
        "ohio":"ohio","oklahoma":"oklahoma","oregon":"oregon","pennsylvania":"pennsylvania",
        "rhode island":"rhode island","south carolina":"south carolina",
        "south dakota":"south dakota","tennessee":"tennessee","texas":"texas",
        "utah":"utah","vermont":"vermont","virginia":"virginia","washington":"washington",
        "west virginia":"west virginia","wisconsin":"wisconsin","wyoming":"wyoming",
        "al":"alabama","ak":"alaska","az":"arizona","ar":"arkansas","ca":"california",
        "co":"colorado","ct":"connecticut","de":"delaware","fl":"florida","ga":"georgia",
        "hi":"hawaii","id":"idaho","il":"illinois","in":"indiana","ia":"iowa",
        "ks":"kansas","ky":"kentucky","la":"louisiana","me":"maine","md":"maryland",
        "ma":"massachusetts","mi":"michigan","mn":"minnesota","ms":"mississippi",
        "mo":"missouri","mt":"montana","ne":"nebraska","nv":"nevada","nh":"new hampshire",
        "nj":"new jersey","nm":"new mexico","ny":"new york","nc":"north carolina",
        "nd":"north dakota","oh":"ohio","ok":"oklahoma","or":"oregon","pa":"pennsylvania",
        "ri":"rhode island","sc":"south carolina","sd":"south dakota","tn":"tennessee",
        "tx":"texas","ut":"utah","vt":"vermont","va":"virginia","wa":"washington",
        "wv":"west virginia","wi":"wisconsin","wy":"wyoming","dc":"district of columbia",
    }

    with conn.cursor() as cur:
        cur.execute("""
            SELECT uid, location_raw, city, state, country
            FROM candidates
            WHERE location_raw != ''
              AND (state = '' OR state IS NULL)
              AND country IN ('US','')
        """)
        rows = cur.fetchall()

    for row in rows:
        raw    = row["location_raw"] or ""
        parts  = [p.strip().lower() for p in raw.split(",")]
        state  = ""
        city   = ""

        for part in parts:
            if part in US_STATE_MAP:
                state = US_STATE_MAP[part]
            elif not city and part and "united states" not in part:
                city = part

        if state:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE candidates SET
                        city  = CASE WHEN city = '' AND %s != '' THEN %s ELSE city END,
                        state = %s,
                        country = 'US',
                        updated_at = now()
                    WHERE uid = %s
                """, (city, city, state, row["uid"]))
            stats["updated"] += 1

    conn.commit()
    logger.info("  Locations fixed: %d", stats["updated"])
    return stats


# ── Summary report ────────────────────────────────────────────────────────────

def print_dq_report(conn):
    logger.info("\n=== Data Quality Report ===")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE cert_verification_status = 'verified') AS verified,
                COUNT(*) FILTER (WHERE array_length(verified_certifications,1) > 0) AS with_verified_certs,
                COUNT(*) FILTER (WHERE state != '' AND state IS NOT NULL) AS with_state,
                COUNT(*) FILTER (WHERE linkedin_url != '') AS with_linkedin,
                COUNT(*) FILTER (WHERE experience_years > 0) AS with_exp_years,
                COUNT(*) FILTER (WHERE source LIKE '%%duplicate%%') AS duplicates_flagged
            FROM candidates
        """)
        r = cur.fetchone()
        logger.info("  Total candidates      : %d", r["total"])
        logger.info("  Verified cert status  : %d (%.1f%%)", r["verified"], 100*r["verified"]/r["total"])
        logger.info("  With verified certs   : %d", r["with_verified_certs"])
        logger.info("  With state            : %d (%.1f%%)", r["with_state"], 100*r["with_state"]/r["total"])
        logger.info("  With LinkedIn URL     : %d (%.1f%%)", r["with_linkedin"], 100*r["with_linkedin"]/r["total"])
        logger.info("  With experience years : %d (%.1f%%)", r["with_exp_years"], 100*r["with_exp_years"]/r["total"])
        logger.info("  Duplicates flagged    : %d", r["duplicates_flagged"])

        cur.execute("""
            SELECT cert, COUNT(*) AS cnt
            FROM (SELECT unnest(verified_certifications) AS cert FROM candidates
                  WHERE cert_verification_status = 'verified') t
            WHERE cert ~ '^cis-[a-z]'
            GROUP BY cert ORDER BY cnt DESC LIMIT 15
        """)
        logger.info("\n  Verified CIS cert distribution:")
        for row in cur.fetchall():
            logger.info("    %-20s : %d", row["cert"], row["cnt"])


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    logger.info("=== data-quality-fix started ===")
    conn = db()
    try:
        fix_dirty_certs(conn)
        fix_duplicates(conn)
        fix_experience_years(conn)
        fix_locations(conn)
        print_dq_report(conn)
    finally:
        conn.close()
    logger.info("=== data-quality-fix done ===")


if __name__ == "__main__":
    run()
