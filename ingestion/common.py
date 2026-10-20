"""
Shared utilities for all ingestion pipelines and Glue jobs.
Single source of truth for S3 helpers, DB connections, normalizers, and watermarks.
"""
import hashlib
import json
import uuid
from datetime import datetime, timezone

import boto3

BRONZE_BUCKET = "prajnov-talent-lake"
CONNECTOR_VERSION = "1.0.0"


# ── Bronze helpers ────────────────────────────────────────────────────────────

def make_meta(
    source: str,
    retention_class: str = "standard",
    source_url: str = "",
    provenance: str = "",
    extra: dict = None,
) -> dict:
    meta = {
        "source": source,
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
        "connector_version": CONNECTOR_VERSION,
        "retention_class": retention_class,
    }
    if source_url:
        meta["source_url"] = source_url
    if provenance:
        meta["provenance"] = provenance
    if extra:
        meta.update(extra)
    return meta


def hash_record(record: dict) -> str:
    raw = json.dumps(record, sort_keys=True, default=str).encode()
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def bronze_path(source: str, entity: str, dt: datetime = None) -> str:
    dt = dt or datetime.now(timezone.utc)
    return f"bronze/{source}/{entity}/{dt.year}/{dt.month:02d}/{dt.day:02d}/{uuid.uuid4()}.json"


# ── S3 helpers ────────────────────────────────────────────────────────────────

def get_s3():
    import os
    profile = os.getenv("AWS_PROFILE", "")
    if profile:
        from botocore.session import get_session
        botocore_session = get_session()
        botocore_session.set_config_variable("profile", profile)
        return boto3.Session(botocore_session=botocore_session).client("s3", region_name="ap-south-1")
    # On EC2 use instance profile credentials
    return boto3.client("s3", region_name="ap-south-1")


def s3_put(key: str, obj: dict, dry_run: bool = False) -> str:
    """Write a single JSON record to S3. Used for bronze layer only."""
    body = json.dumps(obj, default=str, ensure_ascii=False)
    if dry_run:
        print(f"[DRY-RUN] would write s3://{BRONZE_BUCKET}/{key} ({len(body)} bytes)")
        return key
    try:
        get_s3().put_object(
            Bucket=BRONZE_BUCKET,
            Key=key,
            Body=body.encode(),
            ContentType="application/json",
        )
        return key
    except Exception as e:
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parent))
        from local_sink import local_put
        local_path = local_put(key, obj)
        print(f"[LOCAL] {key} → {local_path}")
        return local_path


def parquet_write(prefix: str, records: list, run_id: str = None) -> str:
    """
    Write a list of dicts to a single Parquet file on S3.
    Used for silver and gold layers.
    prefix: e.g. 'silver/candidate/source=credly/cert=CIS-CSM/2025/01/15'
    Returns the S3 key written.
    """
    import io
    import pyarrow as pa
    import pyarrow.parquet as pq

    if not records:
        return None

    run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    key    = f"{prefix}/{run_id}.parquet"

    # Normalise all values to JSON-safe scalars so Arrow can infer schema
    def _clean(v):
        if v is None or isinstance(v, (str, int, float, bool)):
            return v
        if hasattr(v, "isoformat"):
            return v.isoformat()
        return json.dumps(v, default=str)

    cleaned = [{k: _clean(v) for k, v in r.items()} for r in records]
    table   = pa.Table.from_pylist(cleaned)

    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    get_s3().put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=buf.read(),
        ContentType="application/octet-stream",
    )
    return key


def parquet_read(prefix: str) -> list:
    """
    Read all Parquet files under a prefix and return as a list of dicts.
    Used by gold assemblers to read silver partitions.
    """
    import io
    import pyarrow.parquet as pq

    records = []
    for key in s3_list(prefix, ext=".parquet"):
        try:
            body = get_s3().get_object(Bucket=BRONZE_BUCKET, Key=key)["Body"].read()
            table = pq.read_table(io.BytesIO(body))
            records.extend(table.to_pylist())
        except Exception as e:
            print(f"  [WARN] parquet_read skip {key}: {e}")
    return records


def s3_key_exists(key: str) -> bool:
    import botocore.exceptions
    try:
        get_s3().head_object(Bucket=BRONZE_BUCKET, Key=key)
        return True
    except botocore.exceptions.ClientError:
        return False


# ── Manifest ──────────────────────────────────────────────────────────────────

def write_manifest(pipeline: str, stats: dict, dry_run: bool = False):
    dt = datetime.now(timezone.utc)
    manifest = {
        "pipeline": pipeline,
        "run_timestamp": dt.isoformat(),
        "dry_run": dry_run,
        **stats,
    }
    key = f"bronze/{pipeline}/_manifests/{dt.strftime('%Y%m%d_%H%M%S')}.json"
    s3_put(key, manifest, dry_run=dry_run)
    print(f"Manifest → s3://{BRONZE_BUCKET}/{key}")
    return manifest


# ── Dedup index (in-memory set of seen linkedin URLs / IDs per run) ───────────

class SeenSet:
    def __init__(self):
        self._seen: set = set()

    def add(self, key: str):
        self._seen.add(key)

    def __contains__(self, key: str):
        return key in self._seen


# ── Glue / pipeline helpers ───────────────────────────────────────────────────
# Used by all jobs in infra/glue/jobs/

import os as _os
import re as _re

PGVECTOR_HOST = _os.getenv("PGVECTOR_HOST", "ai-recruiter.c7q8sg00wqte.ap-south-1.rds.amazonaws.com")
PGVECTOR_DB   = _os.getenv("PGVECTOR_DB",   "recruiter")
PGVECTOR_USER = _os.getenv("PGVECTOR_USER",  "prajnov")
PGVECTOR_PASS = _os.getenv("PGVECTOR_PASSWORD", "")


def get_db():
    import psycopg2, psycopg2.extras
    return psycopg2.connect(
        host=PGVECTOR_HOST, dbname=PGVECTOR_DB,
        user=PGVECTOR_USER, password=PGVECTOR_PASS,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


def get_watermark(job_name: str) -> str:
    """Return ISO timestamp of last successful run. Returns epoch on first run."""
    try:
        obj = get_s3().get_object(Bucket=BRONZE_BUCKET, Key=f"glue-state/{job_name}/last_run.json")
        return json.loads(obj["Body"].read())["last_run"]
    except Exception:
        return "1970-01-01T00:00:00+00:00"


def set_watermark(job_name: str, ts: str):
    get_s3().put_object(
        Bucket=BRONZE_BUCKET,
        Key=f"glue-state/{job_name}/last_run.json",
        Body=json.dumps({"last_run": ts, "job": job_name}).encode(),
        ContentType="application/json",
    )


def s3_list(prefix: str, ext: str = ".json") -> list:
    pag = get_s3().get_paginator("list_objects_v2")
    return [
        o["Key"]
        for page in pag.paginate(Bucket=BRONZE_BUCKET, Prefix=prefix)
        for o in page.get("Contents", [])
        if o["Key"].endswith(ext) and "_manifests" not in o["Key"]
    ]


def s3_read(key: str) -> dict:
    return json.loads(get_s3().get_object(Bucket=BRONZE_BUCKET, Key=key)["Body"].read())


def s3_exists_set(prefix: str) -> set:
    """Return set of filenames (without .json) already under a prefix."""
    return {k.split("/")[-1].replace(".json", "") for k in s3_list(prefix)}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def date_path() -> str:
    return datetime.now(timezone.utc).strftime("%Y/%m/%d")


# ── Canonical cert normalization — single source of truth ────────────────────

CERT_NORM = {
    "csa": "csa", "certified system administrator": "csa",
    "servicenow certified system administrator": "csa",
    "cad": "cad", "certified application developer": "cad",
    "cta": "cta", "certified technical architect": "cta",
    "cma": "cma", "certified master architect": "cma",
    "cis-itsm": "cis-itsm", "it service management": "cis-itsm",
    "cis-itom": "cis-itom", "it operations management": "cis-itom",
    "cis-hrsd": "cis-hrsd", "cis-hr": "cis-hrsd", "hr service delivery": "cis-hrsd",
    "cis-csm": "cis-csm", "customer service management": "cis-csm",
    "cis-fsm": "cis-fsm", "field service management": "cis-fsm",
    "cis-sam": "cis-sam", "software asset management": "cis-sam",
    "cis-ham": "cis-ham", "hardware asset management": "cis-ham",
    "cis-itam": "cis-itam", "it asset management": "cis-itam",
    "cis-spm": "cis-spm", "strategic portfolio management": "cis-spm",
    "cis-ppm": "cis-ppm", "project portfolio management": "cis-ppm",
    "cis-rc": "cis-rc", "risk and compliance": "cis-rc",
    "cis-sir": "cis-sir", "security incident response": "cis-sir",
    "cis-vr": "cis-vr", "vulnerability response": "cis-vr",
    "cis-disco": "cis-disco", "cis-discovery": "cis-disco",
    "cis-apm": "cis-apm", "application portfolio management": "cis-apm",
    "cis-em": "cis-em", "event management": "cis-em",
    "cis-sm": "cis-sm", "service mapping": "cis-sm",
    "cis-tprm": "cis-tprm", "cis-fso": "cis-fso",
    "cis-pa": "cis-pa", "platform analytics": "cis-pa",
    "itil": "itil", "pmp": "pmp", "cissp": "cissp",
    "safe": "safe", "six sigma": "six-sigma", "six-sigma": "six-sigma",
    "aws certified solutions architect": "aws-sa",
    "aws solutions architect": "aws-sa",
}

VALID_CERT_RE = _re.compile(r"^(cis-[a-z]{2,8}|csa|cad|cta|cma|itil|pmp|cissp|safe|aws-sa|six-sigma)$")

CERT_DISPLAY_NAMES = {
    "csa": "ServiceNow Certified System Administrator",
    "cad": "ServiceNow Certified Application Developer",
    "cta": "ServiceNow Certified Technical Architect",
    "cma": "ServiceNow Certified Master Architect",
    "cis-itsm": "CIS - IT Service Management",
    "cis-itom": "CIS - IT Operations Management",
    "cis-hrsd": "CIS - HR Service Delivery",
    "cis-csm":  "CIS - Customer Service Management",
    "cis-fsm":  "CIS - Field Service Management",
    "cis-sam":  "CIS - Software Asset Management",
    "cis-ham":  "CIS - Hardware Asset Management",
    "cis-itam": "CIS - IT Asset Management",
    "cis-spm":  "CIS - Strategic Portfolio Management",
    "cis-ppm":  "CIS - Project Portfolio Management",
    "cis-rc":   "CIS - Risk and Compliance",
    "cis-sir":  "CIS - Security Incident Response",
    "cis-vr":   "CIS - Vulnerability Response",
    "cis-disco":"CIS - Discovery",
    "cis-apm":  "CIS - Application Portfolio Management",
    "cis-em":   "CIS - Event Management",
    "cis-sm":   "CIS - Service Mapping",
    "cis-tprm": "CIS - Third-Party Risk Management",
    "cis-fso":  "CIS - Financial Services Operations",
    "cis-pa":   "CIS - Platform Analytics",
}


def normalize_cert(raw: str):
    """Normalize raw cert string → canonical code. Returns None if unrecognizable."""
    if not raw:
        return None
    s = _re.sub(r"[^\w\s-]", "", raw.lower().strip())
    s = _re.sub(r"\s+", " ", s).strip()
    if s in CERT_NORM:
        return CERT_NORM[s]
    for key in sorted(CERT_NORM.keys(), key=len, reverse=True):
        if key in s:
            return CERT_NORM[key]
    if VALID_CERT_RE.match(s):
        return s
    return None


def normalize_certs(raw_list: list) -> tuple:
    """Returns (clean_codes[], dirty_strings[])."""
    clean, dirty, seen = [], [], set()
    for raw in (raw_list or []):
        code = normalize_cert(str(raw))
        if code and code not in seen:
            clean.append(code)
            seen.add(code)
        elif not code:
            dirty.append(str(raw))
    return clean, dirty


# ── Location parsing ──────────────────────────────────────────────────────────

US_STATES = {
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

COUNTRY_CODES = {
    "united states":"US","usa":"US","us":"US","united kingdom":"GB","uk":"GB",
    "canada":"CA","australia":"AU","india":"IN","germany":"DE","france":"FR",
    "netherlands":"NL","singapore":"SG","ireland":"IE","new zealand":"NZ",
    "south africa":"ZA","uae":"AE","united arab emirates":"AE","saudi arabia":"SA",
    "philippines":"PH","mexico":"MX",
}


def parse_location(raw: str) -> tuple:
    """Returns (city, state, country) from raw location string."""
    if not raw:
        return "", "", ""
    parts   = [p.strip().lower() for p in raw.split(",")]
    country = ""
    if parts and parts[-1] in COUNTRY_CODES:
        country = COUNTRY_CODES[parts[-1]]
        parts   = parts[:-1]
    state, city = "", ""
    if len(parts) >= 2:
        city  = parts[0]
        state = US_STATES.get(parts[1], parts[1])
        if not country and (parts[1] in US_STATES or state in US_STATES.values()):
            country = "US"
    elif len(parts) == 1:
        if parts[0] in US_STATES:
            state, country = US_STATES[parts[0]], "US"
        else:
            city = parts[0]
    return city, state, country or ("US" if state in US_STATES.values() else "")


def parse_year(s) -> int:
    m = _re.search(r"\b(19|20)\d{2}\b", str(s or ""))
    return int(m.group()) if m else None


def linkedin_id_from_url(url: str) -> str:
    m = _re.search(r"linkedin\.com/in/([^/?#\s]+)", url or "")
    return m.group(1).rstrip("/") if m else ""


def make_candidate_uid(source_prefix: str, linkedin_id: str, name: str) -> str:
    if linkedin_id:
        return f"{source_prefix}_{linkedin_id}"
    return f"{source_prefix}_{hashlib.md5(name.lower().strip().encode()).hexdigest()[:16]}"


def compute_dq_score(record: dict) -> float:
    """Data quality score 0.0–1.0."""
    score = 0.0
    if record.get("canonical_name"):   score += 0.10
    if record.get("current_title"):    score += 0.10
    if record.get("current_company"):  score += 0.10
    if record.get("location_state"):   score += 0.05
    if record.get("location_city"):    score += 0.05
    if record.get("linkedin_url"):     score += 0.15
    if record.get("verified_certs"):   score += 0.20
    if record.get("experience"):       score += 0.10
    if record.get("skills"):           score += 0.10
    if record.get("email"):            score += 0.05
    return round(score, 2)
