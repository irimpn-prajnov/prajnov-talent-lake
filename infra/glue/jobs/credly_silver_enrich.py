"""
Glue Job: credly-silver-enrich
Reads consolidated bronze Credly files, matches against candidates table via
2-tier entity resolution, enriches verified_certifications[].
Inserts unmatched as new candidate rows.

Tier 1: LinkedIn URL (exact linkedin_id match)
Tier 2: normalized name + state
Tier 3: DISABLED — false positives on common names
"""
import json, logging, os, re, sys, uuid
from datetime import datetime, timezone

import boto3, psycopg2, psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Glue Python Shell passes --ARG_NAME via sys.argv, not os.environ.
# Parse them out manually so the script works both locally and in Glue.
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

CREDLY_PREFIX = "bronze/discovery/credly/certs/"

# All 19 cert slugs → canonical codes
SLUG_TO_CODE = {
    "csa":          "csa",
    "cad":          "cad",
    "cta":          "cta",
    "cma":          "cma",
    "cis_itsm":     "cis-itsm",
    "cis_itom":     "cis-itom",
    "cis_hrsd":     "cis-hrsd",
    "cis_csm":      "cis-csm",
    "cis_fsm":      "cis-fsm",
    "cis_sam":      "cis-sam",
    "cis_ham":      "cis-ham",
    "cis_itam":     "cis-itam",
    "cis_spm":      "cis-spm",
    "cis_ppm":      "cis-ppm",
    "cis_rc":       "cis-rc",
    "cis_sir":      "cis-sir",
    "cis_vr":       "cis-vr",
    "cis_disco":    "cis-disco",
    "cis_apm":      "cis-apm",
}


def s3():
    return boto3.client("s3", region_name=REGION)


def db():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


def norm_name(name: str) -> str:
    return re.sub(r"\s+", " ", (name or "").lower().strip())


def linkedin_id_from_url(url: str) -> str:
    m = re.search(r"linkedin\.com/in/([^/?#\s]+)", url or "")
    return m.group(1).rstrip("/").lower() if m else ""


def parse_location(raw: str):
    US_STATES = {
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
        "district of columbia":"district of columbia",
    }
    if not raw:
        return "", "", ""
    parts = [p.strip().lower() for p in raw.split(",")]
    country = ""
    if parts and parts[-1] in ("united states", "usa", "us"):
        country = "US"
        parts = parts[:-1]
    state, city = "", ""
    if len(parts) >= 2:
        city  = parts[0]
        state = US_STATES.get(parts[1], parts[1])
        if not country and parts[1] in US_STATES:
            country = "US"
    elif len(parts) == 1 and parts[0] in US_STATES:
        state, country = US_STATES[parts[0]], "US"
    elif len(parts) == 1:
        city = parts[0]
    return city, state, country or ("US" if state else "")


# ── Load consolidated bronze files ────────────────────────────────────────────

def load_all_bronze() -> dict:
    """Returns {slug: [records]} for all certs that have a consolidated file."""
    client = s3()
    result = {}
    for slug in SLUG_TO_CODE:
        key = f"{CREDLY_PREFIX}{slug}/consolidated/all.json"
        try:
            body = client.get_object(Bucket=BUCKET, Key=key)["Body"].read()
            records = json.loads(body)
            result[slug] = records
            logger.info("  %s: %d records", slug, len(records))
        except client.exceptions.NoSuchKey:
            logger.warning("  %s: no consolidated file — skipping", slug)
        except Exception as e:
            logger.warning("  %s: error reading — %s", slug, e)
    return result


# ── Build staging records (cross-cert dedup) ──────────────────────────────────

def build_staging(all_cert_records: dict) -> list:
    merged = {}  # credly_id/key → record

    for slug, records in all_cert_records.items():
        cert_code = SLUG_TO_CODE[slug]
        for rec in records:
            cid = rec.get("credly_id", "").strip()
            key = cid if cid else f"nocredly_{norm_name(rec.get('name',''))}"
            if not key or key == "nocredly_":
                continue

            loc = rec.get("location", "")
            city, state, country = parse_location(loc)

            if key not in merged:
                merged[key] = {
                    "credly_id":       cid,
                    "credly_username": rec.get("credly_username", ""),
                    "name":            rec.get("name", ""),
                    "name_norm":       norm_name(rec.get("name", "")),
                    "location_raw":    loc,
                    "city":            city,
                    "state":           state,
                    "country":         country or "US",
                    "linkedin_url":    rec.get("linkedin_url", ""),
                    "linkedin_id":     linkedin_id_from_url(rec.get("linkedin_url", "")),
                    "cert_codes":      {cert_code},
                }
            else:
                merged[key]["cert_codes"].add(cert_code)

    staging = []
    for r in merged.values():
        r["cert_codes"] = sorted(r["cert_codes"])
        staging.append(r)

    return staging


# ── Stage into temp table ─────────────────────────────────────────────────────

def create_staging_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TEMP TABLE credly_staging (
                credly_id       TEXT,
                credly_username TEXT,
                name            TEXT,
                name_norm       TEXT,
                location_raw    TEXT,
                city            TEXT,
                state           TEXT,
                country         TEXT,
                linkedin_url    TEXT,
                linkedin_id     TEXT,
                cert_codes      TEXT[]
            ) ON COMMIT PRESERVE ROWS
        """)
    conn.commit()


def insert_staging(conn, records: list):
    with conn.cursor() as cur:
        for r in records:
            cur.execute("""
                INSERT INTO credly_staging VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                r["credly_id"], r["credly_username"],
                r["name"], r["name_norm"], r["location_raw"],
                r["city"], r["state"], r["country"],
                r["linkedin_url"], r["linkedin_id"],
                r["cert_codes"],
            ))
    conn.commit()
    logger.info("  Staged %d records", len(records))


# ── Match + Enrich ────────────────────────────────────────────────────────────

def enrich_tier(conn, query: str, params, tier: str) -> set:
    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()

    if not rows:
        logger.info("  [%s] matched=0", tier)
        return set()

    matched = set()
    with conn.cursor() as cur:
        for row in rows:
            username   = row["credly_username"]
            cert_codes = row["cert_codes"]
            uid        = row["uid"]
            matched.add(username)
            cur.execute("""
                UPDATE candidates SET
                    verified_certifications = (
                        SELECT array_agg(DISTINCT v ORDER BY v)
                        FROM unnest(
                            COALESCE(verified_certifications, ARRAY[]::text[]) || %s::text[]
                        ) v
                    ),
                    cert_verification_status  = 'credly_verified',
                    cert_verification_sources = COALESCE(cert_verification_sources, '{}'::jsonb)
                                                || %s::jsonb,
                    source     = CASE WHEN source NOT LIKE '%%credly%%'
                                 THEN source || '+credly' ELSE source END,
                    updated_at = now()
                WHERE uid = %s
            """, (
                cert_codes,
                json.dumps({"credly_verified": True, "credly_username": username}),
                uid,
            ))
    conn.commit()
    logger.info("  [%s] matched=%d", tier, len(matched))
    return matched


# ── Insert unmatched ──────────────────────────────────────────────────────────

def insert_unmatched(conn, records: list, matched: set) -> int:
    unmatched = [r for r in records if r["credly_username"] not in matched]
    logger.info("  Unmatched (new): %d", len(unmatched))
    if not unmatched:
        return 0

    inserted = 0
    with conn.cursor() as cur:
        for r in unmatched:
            uid = f"credly_{r['credly_username']}" if r["credly_username"] \
                  else f"credly_{uuid.uuid4().hex[:16]}"
            cur.execute("""
                INSERT INTO candidates (
                    uid, name, linkedin_url, linkedin_id,
                    city, state, country, location_raw,
                    certifications, verified_certifications,
                    cert_verification_status, cert_verification_sources,
                    source, added_at, updated_at
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,
                    'credly_verified', %s,
                    'credly', now(), now()
                )
                ON CONFLICT (uid) DO UPDATE SET
                    verified_certifications = (
                        SELECT array_agg(DISTINCT v ORDER BY v)
                        FROM unnest(
                            COALESCE(candidates.verified_certifications, ARRAY[]::text[])
                            || EXCLUDED.verified_certifications
                        ) v
                    ),
                    cert_verification_status = 'credly_verified',
                    updated_at = now()
            """, (
                uid, r["name"], r["linkedin_url"], r["linkedin_id"],
                r["city"], r["state"], r["country"], r["location_raw"],
                r["cert_codes"], r["cert_codes"],
                json.dumps({"credly_verified": True, "credly_username": r["credly_username"]}),
            ))
            inserted += cur.rowcount
    conn.commit()
    return inserted


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    logger.info("=== credly-silver-enrich started ===")

    logger.info("Step 1: Loading consolidated bronze files...")
    all_cert_records = load_all_bronze()
    logger.info("  Certs loaded: %d", len(all_cert_records))

    logger.info("Step 2: Building staging records (cross-cert dedup)...")
    staging = build_staging(all_cert_records)
    logger.info("  Unique Credly candidates: %d", len(staging))

    conn = db()

    logger.info("Step 3: Staging into temp table...")
    create_staging_table(conn)
    insert_staging(conn, staging)

    logger.info("Step 4: Matching & enriching...")
    matched = set()

    # Tier 1: LinkedIn ID
    matched |= enrich_tier(conn, """
        SELECT s.credly_username, s.cert_codes, c.uid
        FROM credly_staging s
        JOIN candidates c ON c.linkedin_id = s.linkedin_id
        WHERE s.linkedin_id != ''
    """, None, "linkedin_id")

    # Tier 2: Name + State (exclude already matched)
    already = list(matched) or ["__none__"]
    matched |= enrich_tier(conn, """
        SELECT s.credly_username, s.cert_codes, c.uid
        FROM credly_staging s
        JOIN candidates c
          ON lower(trim(c.name)) = s.name_norm
         AND lower(trim(c.state)) = lower(trim(s.state))
        WHERE s.state != ''
          AND s.credly_username != ALL(%s)
    """, (already,), "name+state")

    logger.info("  Total matched: %d", len(matched))

    logger.info("Step 5: Inserting unmatched as new candidates...")
    inserted = insert_unmatched(conn, staging, matched)
    logger.info("  Inserted: %d", inserted)

    # Ensure GIN index
    with conn.cursor() as cur:
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_candidates_verified_certs_gin
            ON candidates USING GIN (verified_certifications)
        """)
    conn.commit()
    conn.close()

    logger.info("=== credly-silver-enrich done ===")
    logger.info("  Unique Credly candidates : %d", len(staging))
    logger.info("  Matched+enriched         : %d", len(matched))
    logger.info("  New from Credly          : %d", inserted)


if __name__ == "__main__":
    run()
