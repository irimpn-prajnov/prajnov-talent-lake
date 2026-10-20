"""
Cert Verification Engine.

Two verification tiers:
  Tier 1 — Credly bronze match (instant, free, confidence=1.0)
            Match candidate linkedin_id or name against S3 bronze Credly earners.
  Tier 2 — SN Partner Portal (Playwright, confidence=1.0)
            Used only when Tier 1 fails and candidate claims a cert.

Result written to:
  - cert_verification_log (audit trail of every check)
  - candidates.verified_certifications[] (only proven certs)
  - candidates.cert_verification_status
"""
import json, logging, os, re, time
from datetime import datetime, timezone
from typing import Optional

import boto3, psycopg2, psycopg2.extras
from botocore.session import get_session

logger = logging.getLogger(__name__)

BUCKET = "prajnov-talent-lake"
REGION = "ap-south-1"

# Canonical cert code normalization
CERT_NORM = {
    "csa": "csa", "certified system administrator": "csa",
    "cad": "cad", "certified application developer": "cad",
    "cta": "cta", "cma": "cma",
    "cis-itsm": "cis-itsm", "it service management": "cis-itsm",
    "cis-itom": "cis-itom",
    "cis-hrsd": "cis-hrsd", "cis-hr": "cis-hrsd", "hr service delivery": "cis-hrsd",
    "cis-csm": "cis-csm", "customer service management": "cis-csm",
    "cis-fsm": "cis-fsm", "field service management": "cis-fsm",
    "cis-sam": "cis-sam", "software asset management": "cis-sam",
    "cis-ham": "cis-ham", "cis-itam": "cis-itam",
    "cis-spm": "cis-spm", "cis-ppm": "cis-ppm",
    "cis-rc": "cis-rc", "cis-sir": "cis-sir",
    "cis-vr": "cis-vr", "vulnerability response": "cis-vr",
    "cis-disco": "cis-disco", "cis-discovery": "cis-disco",
    "cis-apm": "cis-apm", "cis-em": "cis-em", "cis-sm": "cis-sm",
    "cis-tprm": "cis-tprm", "cis-fso": "cis-fso",
    "itil": "itil", "pmp": "pmp", "cissp": "cissp",
}

SN_CERT_MAP = {
    "customer service management": "cis-csm",
    "field service management":    "cis-fsm",
    "it service management":       "cis-itsm",
    "hr service delivery":         "cis-hrsd",
    "software asset management":   "cis-sam",
    "hardware asset management":   "cis-ham",
    "vulnerability response":      "cis-vr",
    "risk and compliance":         "cis-rc",
    "security incident response":  "cis-sir",
    "strategic portfolio":         "cis-spm",
    "discovery":                   "cis-disco",
    "system administrator":        "csa",
    "application developer":       "cad",
    "technical architect":         "cta",
}


def normalize_cert(raw: str) -> Optional[str]:
    s = re.sub(r"[^\w\s-]", "", raw.lower().strip())
    if s in CERT_NORM:
        return CERT_NORM[s]
    for key in sorted(CERT_NORM.keys(), key=len, reverse=True):
        if key in s:
            return CERT_NORM[key]
    if re.match(r"^cis-[a-z]{2,8}$", s):
        return s
    return None


def s3_client():
    bs = get_session()
    bs.set_config_variable("profile", os.getenv("AWS_PROFILE", "prajnov"))
    return boto3.Session(botocore_session=bs).client("s3", region_name=REGION)


def db_conn():
    return psycopg2.connect(
        host=os.getenv("PGVECTOR_HOST", "ai-recruiter.c7q8sg00wqte.ap-south-1.rds.amazonaws.com"),
        dbname=os.getenv("PGVECTOR_DB", "recruiter"),
        user=os.getenv("PGVECTOR_USER", "prajnov"),
        password=os.getenv("PGVECTOR_PASSWORD", ""),
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


# ── Tier 1: Credly bronze index ───────────────────────────────────────────────

_credly_index = None  # {linkedin_id: {cert_code: {credly_username, badge_url, issued_date}}}
_credly_name_index = None  # {normalized_name: [...]}


def _build_credly_index():
    global _credly_index, _credly_name_index
    if _credly_index is not None:
        return

    logger.info("Building Credly bronze index from S3...")
    _credly_index      = {}
    _credly_name_index = {}
    client = s3_client()
    pag    = client.get_paginator("list_objects_v2")

    for page in pag.paginate(Bucket=BUCKET, Prefix="bronze/discovery/credly/certs/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not (key.endswith(".json") or key.endswith(".jsonl")):
                continue
            try:
                body = client.get_object(Bucket=BUCKET, Key=key)["Body"].read()
                lines = body.decode().splitlines() if key.endswith(".jsonl") else [body.decode()]
                for line in lines:
                    if not line.strip():
                        continue
                    rec = json.loads(line)
                    cert_code = normalize_cert(rec.get("cert", ""))
                    if not cert_code:
                        continue

                    linkedin_id = ""
                    credly_url  = rec.get("credly_url", "")
                    if credly_url:
                        m = re.search(r"linkedin\.com/in/([^/?#]+)", credly_url)
                        if m:
                            linkedin_id = m.group(1).rstrip("/")

                    username    = rec.get("credly_username", "")
                    issued_date = ""
                    badge_url   = credly_url
                    for badge in (rec.get("badges") or []):
                        issued_date = badge.get("date", "")[:10]
                        badge_url   = badge.get("url", credly_url)
                        break

                    entry = {
                        "cert_code":       cert_code,
                        "credly_username": username,
                        "badge_url":       badge_url,
                        "issued_date":     issued_date,
                        "credly_url":      credly_url,
                    }

                    if linkedin_id:
                        _credly_index.setdefault(linkedin_id, {})[cert_code] = entry

                    name_key = (rec.get("name") or "").lower().strip()
                    if name_key:
                        _credly_name_index.setdefault(name_key, []).append(entry)

            except Exception as e:
                logger.debug("Skip %s: %s", obj["Key"], e)

    logger.info("Credly index built: %d linkedin_ids, %d names",
                len(_credly_index), len(_credly_name_index))


def verify_tier1_credly(linkedin_id: str, name: str, claimed_certs: list) -> dict:
    """
    Match against Credly bronze index.
    Returns {cert_code: evidence_dict} for every cert that matches.
    """
    _build_credly_index()
    verified = {}

    # Primary match: linkedin_id
    lid_clean = re.sub(r"https?://.*linkedin\.com/in/", "", linkedin_id or "").rstrip("/").lower()
    if lid_clean and lid_clean in _credly_index:
        for cert_code, entry in _credly_index[lid_clean].items():
            if not claimed_certs or cert_code in claimed_certs:
                verified[cert_code] = {**entry, "match_method": "linkedin_id"}

    # Secondary match: name (only if no linkedin match)
    if not verified:
        name_key = (name or "").lower().strip()
        if name_key in (_credly_name_index or {}):
            for entry in _credly_name_index[name_key]:
                cert_code = entry["cert_code"]
                if not claimed_certs or cert_code in claimed_certs:
                    verified[cert_code] = {**entry, "match_method": "name"}

    return verified


# ── Tier 2: SN Partner Portal ─────────────────────────────────────────────────

def verify_tier2_portal(name: str, email: str = "") -> dict:
    """
    Check SN Partner Portal via Playwright.
    Returns {cert_code: evidence_dict} for every cert confirmed.
    Only called when Tier 1 fails and candidate claims a cert.
    """
    try:
        import sys, os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "prajnov-recruiter"))
        from scripts.sn_cert_verify import verify_candidate, SN_CERT_MAP as PORTAL_MAP

        result = verify_candidate(name=name, email=email, headless=True, timeout_ms=25000)
        verified = {}
        for pr in result.get("portal_results", []):
            for cert in pr.get("certs", []):
                raw  = cert.get("name", "")
                code = normalize_cert(cert.get("abbr", "") or raw)
                if code:
                    verified[code] = {
                        "cert_code":    code,
                        "cert_name":    raw,
                        "issued_date":  cert.get("date", ""),
                        "portal_name":  pr.get("portal_name", ""),
                        "match_method": "sn_portal",
                    }
        return verified
    except Exception as e:
        logger.warning("Portal verify failed for %s: %s", name, e)
        return {}


# ── Write results ─────────────────────────────────────────────────────────────

def write_verification_result(conn, uid: str, verified: dict, method: str):
    """
    Write verified certs to candidates table and audit log.
    verified = {cert_code: evidence_dict}
    """
    now = datetime.now(timezone.utc)

    with conn.cursor() as cur:
        for cert_code, evidence in verified.items():
            cur.execute("""
                INSERT INTO cert_verification_log
                    (candidate_uid, cert_code, method, result, confidence, evidence, checked_at)
                VALUES (%s, %s, %s, 'verified', %s, %s, %s)
            """, (uid, cert_code, method,
                  1.0 if method in ("credly_bronze", "sn_portal") else 0.85,
                  json.dumps(evidence), now))

        if verified:
            verified_codes = list(verified.keys())
            sources        = {c: method for c in verified_codes}
            cur.execute("""
                UPDATE candidates SET
                    verified_certifications   = (
                        SELECT array_agg(DISTINCT v) FROM unnest(
                            COALESCE(verified_certifications, '{}') || %s::text[]
                        ) v
                    ),
                    cert_verification_status  = 'verified',
                    cert_verified_at          = %s,
                    cert_verification_sources = COALESCE(cert_verification_sources, '{}') || %s
                WHERE uid = %s
            """, (verified_codes, now, json.dumps(sources), uid))
        else:
            cur.execute("""
                UPDATE candidates SET
                    cert_verification_status = CASE
                        WHEN cert_verification_status = 'verified' THEN 'verified'
                        ELSE 'checked_not_found'
                    END,
                    cert_verified_at = %s
                WHERE uid = %s
            """, (now, uid))

    conn.commit()


def log_not_found(conn, uid: str, cert_code: str, method: str, reason: str = ""):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO cert_verification_log
                (candidate_uid, cert_code, method, result, confidence, evidence, checked_at)
            VALUES (%s, %s, %s, 'not_found', 0.0, %s, now())
        """, (uid, cert_code, method, json.dumps({"reason": reason})))
    conn.commit()
