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


def _arg(name, default=""):
    key = f"--{name}"
    try:
        idx = sys.argv.index(key)
        return sys.argv[idx + 1]
    except (ValueError, IndexError):
        return os.getenv(name, default)


BUCKET  = _arg("BUCKET",        "prajnov-talent-lake")
DB_HOST = _arg("PGVECTOR_HOST", "ai-recruiter.c7q8sg00wqte.ap-south-1.rds.amazonaws.com")
DB_NAME = _arg("PGVECTOR_DB",   "recruiter")
DB_USER = _arg("PGVECTOR_USER", "prajnov")

CREDLY_PREFIX = "bronze/discovery/credly/certs/"

SLUG_TO_CODE = {
    "csa":          "csa",
    "cad":          "cad",
    "cta":          "cta",
    "cma":          "cma",
    "cis_csm":      "cis-csm",
    "cis_df":       "cis-df",
    "cis_disco":    "cis-disco",
    "cis_em":       "cis-em",
    "cis_fsm":      "cis-fsm",
    "cis_ham":      "cis-ham",
    "cis_hr":       "cis-hr",
    "cis_itsm":     "cis-itsm",
    "cis_pa":       "cis-pa",
    "cis_rc":       "cis-rc",
    "cis_sam":      "cis-sam",
    "cis_sir":      "cis-sir",
    "cis_sm":       "cis-sm",
    "cis_sp":       "cis-sp",
    "cis_spm":      "cis-spm",
    "cis_tprm":     "cis-tprm",
    "cis_vr":       "cis-vr",
    "cis_apm":      "cis-apm",
    "cis_cpg":      "cis-cpg",
    "suite_csm":    "suite-csm",
    "suite_df":     "suite-df",
    "suite_ea":     "suite-ea",
    "suite_hr":     "suite-hr",
    "suite_itsm":   "suite-itsm",
    "suite_itom_v": "suite-itom-v",
    "suite_itom_p": "suite-itom-p",
    "suite_itom_e": "suite-itom-e",
    "suite_lsd":    "suite-lsd",
    "suite_pi":     "suite-pi",
    "suite_psds":   "suite-psds",
    "suite_wsd":    "suite-wsd",
    "suite_fso_b":  "suite-fso-b",
    "suite_fso_i":  "suite-fso-i",
    "suite_apo":    "suite-apo",
    "suite_spo":    "suite-spo",
    "suite_slo":    "suite-slo",
    "suite_tsm":    "suite-tsm",
    "cpoa":         "cpoa",
    "cpop":         "cpop",
    "cpoe":         "cpoe",
}


def _get_db_pass() -> str:
    env_pass = os.getenv("PGVECTOR_PASSWORD", "")
    if env_pass:
        return env_pass
    region     = _arg("AWS_REGION", "ap-south-1")
    secret_arn = _arg("PGVECTOR_SECRET_ARN", "arn:aws:secretsmanager:ap-south-1:201727487159:secret:prajnov/recruiter-F8sc3X")
    client = boto3.client("secretsmanager", region_name=region)
    secret = json.loads(client.get_secret_value(SecretId=secret_arn)["SecretString"])
    return secret.get("PGVECTOR_PASSWORD", "")


def s3():
    return boto3.client("s3", region_name=_arg("AWS_REGION", "ap-south-1"))


def db():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=_get_db_pass(),
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


def load_all_bronze() -> dict:
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


def build_staging(all_cert_records: dict) -> list:
    merged = {}
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
        psycopg2.extras.execute_values(cur, """
            INSERT INTO credly_staging VALUES %s
        """, [
            (r["credly_id"], r["credly_username"], r["name"], r["name_norm"],
             r["location_raw"], r["city"], r["state"], r["country"],
             r["linkedin_url"], r["linkedin_id"], r["cert_codes"])
            for r in records
        ])
    conn.commit()
    print(f"STEP3b done: staged {len(records)} records", flush=True)


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


def insert_unmatched(conn, records: list, matched: set) -> int:
    unmatched = [r for r in records if r["credly_username"] not in matched]
    print(f"STEP5: unmatched={len(unmatched)}", flush=True)
    if not unmatched:
        return 0

    BATCH = 500
    inserted = 0
    for i in range(0, len(unmatched), BATCH):
        batch = unmatched[i:i + BATCH]
        rows = []
        for r in batch:
            uid = f"credly_{r['credly_username']}" if r["credly_username"] \
                  else f"credly_{uuid.uuid4().hex[:16]}"
            rows.append((
                uid, r["name"], r["linkedin_url"], r["linkedin_id"],
                r["city"], r["state"], r["country"], r["location_raw"],
                r["cert_codes"], r["cert_codes"],
                json.dumps({"credly_verified": True, "credly_username": r["credly_username"]}),
            ))
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO candidates (
                    uid, name, linkedin_url, linkedin_id,
                    city, state, country, location_raw,
                    certifications, verified_certifications,
                    cert_verification_status, cert_verification_sources,
                    source, added_at, updated_at
                ) VALUES %s
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
            """, rows, template="(%s,%s,%s,%s,%s,%s,%s,%s,%s::text[],%s::text[],'credly_verified',%s::jsonb,'credly',now(),now())")
            inserted += cur.rowcount
        conn.commit()
        print(f"STEP5: batch {min(i+BATCH,len(unmatched))}/{len(unmatched)} inserted={cur.rowcount}", flush=True)
    return inserted


def run():
    logger.info("=== credly-silver-enrich started ===")
    print("STEP0: run() started", flush=True)

    print("STEP1: loading bronze", flush=True)
    all_cert_records = load_all_bronze()
    print(f"STEP1 done: {len(all_cert_records)} certs loaded", flush=True)

    print("STEP2: building staging", flush=True)
    staging = build_staging(all_cert_records)
    print(f"STEP2 done: {len(staging)} unique candidates", flush=True)

    print("STEP3: connecting to DB", flush=True)
    conn = db()
    print("STEP3: DB connected", flush=True)

    print("STEP3b: staging into temp table", flush=True)
    create_staging_table(conn)
    insert_staging(conn, staging)

    print("STEP4: matching", flush=True)
    matched = set()

    matched |= enrich_tier(conn, """
        SELECT s.credly_username, s.cert_codes, c.uid
        FROM credly_staging s
        JOIN candidates c ON c.linkedin_id = s.linkedin_id
        WHERE s.linkedin_id != ''
    """, None, "linkedin_id")

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

    print(f"STEP4 done: matched={len(matched)}", flush=True)

    print("STEP5: inserting unmatched", flush=True)
    inserted = insert_unmatched(conn, staging, matched)
    print(f"STEP5 done: inserted={inserted}", flush=True)

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


import traceback as _tb
print("=== credly-silver-enrich: invoking run() ===", flush=True)
try:
    run()
    print("=== credly-silver-enrich: run() completed ===", flush=True)
except Exception as _e:
    print(f"FATAL ERROR: {_e}", flush=True)
    _tb.print_exc()
    raise

# Force stdout flush so Glue captures output
import sys; sys.stdout.flush()
