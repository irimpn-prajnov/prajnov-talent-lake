"""
Glue Job: brightdata-bronze-ingest
Scans s3://prajnov-brightdata-datasets/ for any LinkedIn dataset files (.json / .parquet).
Streams each unprocessed file into bronze JSONL batches of 1,000 records.

Output: s3://prajnov-talent-lake/bronze/candidates/brightdata/{dataset_slug}/batch_{n}.jsonl
Watermark: glue-state/brightdata_bronze_ingest/processed_files.json  (set of S3 ETags)

Incremental: a dataset file is only processed once (keyed by ETag).
Drop a new BrightData file into the source bucket → next run picks it up automatically.
"""
import io, json, logging, os, re, sys
from datetime import datetime, timezone

import boto3, pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

def _arg(name, default=""):
    key = f"--{name}"
    try:
        idx = sys.argv.index(key)
        return sys.argv[idx + 1]
    except (ValueError, IndexError):
        return os.getenv(name, default)

SRC_BUCKET  = _arg("BRIGHTDATA_BUCKET", "prajnov-brightdata-datasets")
DST_BUCKET  = _arg("BUCKET",            "prajnov-talent-lake")
REGION      = _arg("AWS_REGION",        "ap-south-1")
BATCH_SIZE  = 1000
STATE_KEY   = "glue-state/brightdata_bronze_ingest/processed_files.json"


def s3():
    return boto3.client("s3", region_name=REGION)


# ── Watermark ─────────────────────────────────────────────────────────────────

def load_processed() -> dict:
    """Returns {etag: {key, processed_at}} for all already-processed files."""
    try:
        body = s3().get_object(Bucket=DST_BUCKET, Key=STATE_KEY)["Body"].read()
        return json.loads(body)
    except Exception:
        return {}


def save_processed(processed: dict):
    s3().put_object(
        Bucket=DST_BUCKET, Key=STATE_KEY,
        Body=json.dumps(processed, indent=2).encode(),
        ContentType="application/json",
    )


# ── Source file discovery ─────────────────────────────────────────────────────

def list_dataset_files() -> list:
    """List all .json and .parquet files in the source bucket (skip test_ files)."""
    client = s3()
    pag    = client.get_paginator("list_objects_v2")
    files  = []
    for page in pag.paginate(Bucket=SRC_BUCKET):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.startswith("test_") or "/test_" in key:
                continue
            if key.endswith(".json") or key.endswith(".parquet"):
                files.append({"key": key, "etag": obj["ETag"].strip('"'), "size": obj["Size"]})
    return files


def dataset_slug(key: str) -> str:
    """Turn s3 key into a safe folder name: servicenow-us/bd_20260628.json → servicenow-us_bd_20260628"""
    base = key.replace("/", "_").rsplit(".", 1)[0]
    return re.sub(r"[^a-zA-Z0-9_-]", "_", base)


# ── Record streaming ──────────────────────────────────────────────────────────

def stream_json(key: str):
    """Stream a large JSON array from S3 without loading it all into memory."""
    import ijson
    resp = s3().get_object(Bucket=SRC_BUCKET, Key=key)
    yield from ijson.items(resp["Body"], "item")


def stream_parquet(key: str):
    """Read a Parquet file from S3 and yield rows as dicts."""
    body = s3().get_object(Bucket=SRC_BUCKET, Key=key)["Body"].read()
    table = pq.read_table(io.BytesIO(body))
    for batch in table.to_batches(max_chunksize=500):
        for row in batch.to_pylist():
            yield row


def stream_file(key: str):
    if key.endswith(".parquet"):
        yield from stream_parquet(key)
    else:
        yield from stream_json(key)


# ── Record normalization ──────────────────────────────────────────────────────

def normalize_record(raw: dict, dataset_key: str) -> dict:
    """
    Flatten a raw BrightData LinkedIn record into a consistent bronze schema.
    Handles both field name variants seen across BrightData dataset versions.
    """
    # Identity
    linkedin_id  = (raw.get("id") or raw.get("linkedin_id") or "").strip()
    linkedin_url = (raw.get("url") or raw.get("input_url") or raw.get("linkedin_url") or "").strip()

    # Normalise URL to standard linkedin.com domain
    if linkedin_url:
        m = re.search(r"linkedin\.com/in/([^/?#\s]+)", linkedin_url)
        if m:
            linkedin_id  = linkedin_id or m.group(1).rstrip("/").lower()
            linkedin_url = f"https://www.linkedin.com/in/{m.group(1).rstrip('/')}"

    # Name
    name       = (raw.get("name") or "").strip()
    first_name = (raw.get("first_name") or "").strip()
    last_name  = (raw.get("last_name") or "").strip()
    if not first_name and name:
        parts      = name.split(" ", 1)
        first_name = parts[0]
        last_name  = parts[1] if len(parts) > 1 else ""

    # Location — BrightData puts full "City, State, Country" in `city` field
    location_raw = (raw.get("city") or raw.get("location") or raw.get("location_raw") or "").strip()

    # Current role
    current_title   = (raw.get("position") or raw.get("current_title") or raw.get("headline") or "").strip()
    current_company = ""
    cc = raw.get("current_company")
    if isinstance(cc, dict):
        current_company = (cc.get("name") or "").strip()
    elif isinstance(cc, str):
        current_company = cc.strip()
    if not current_company:
        current_company = (raw.get("current_company_name") or "").strip()

    # Experience — BrightData uses start_date/end_date strings like "Mar 2023" / "Present"
    experience = []
    for exp in (raw.get("experience") or []):
        if not isinstance(exp, dict):
            continue
        company = (exp.get("company") or "").strip()
        title   = (exp.get("title") or "").strip()
        if not company and not title:
            continue
        start_str = str(exp.get("start_date") or "")
        end_str   = str(exp.get("end_date") or "")
        start_year = _parse_year(start_str)
        end_year   = _parse_year(end_str)
        is_current = end_str.lower() in ("present", "", "none", "null")
        experience.append({
            "company":    company,
            "title":      title,
            "start_date": start_str,
            "end_date":   end_str,
            "start_year": start_year,
            "end_year":   end_year,
            "is_current": is_current,
            "location":   (exp.get("location") or "").strip(),
        })

    # Experience years — derive from earliest start_year
    exp_years = None
    start_years = [e["start_year"] for e in experience if e["start_year"]]
    if start_years:
        exp_years = datetime.now().year - min(start_years)
        exp_years = max(1, min(exp_years, 50))

    # Education
    education = []
    for edu in (raw.get("education") or []):
        if not isinstance(edu, dict):
            continue
        institution = (edu.get("title") or edu.get("school") or edu.get("institution") or "").strip()
        if not institution:
            continue
        education.append({
            "institution": institution,
            "degree":      (edu.get("degree") or edu.get("subtitle") or "").strip(),
            "field":       (edu.get("field_of_study") or edu.get("field") or "").strip(),
            "start_year":  _parse_year(str(edu.get("start_year") or "")),
            "end_year":    _parse_year(str(edu.get("end_year") or "")),
        })

    # Skills
    skills = []
    raw_skills = raw.get("skills") or []
    if isinstance(raw_skills, list):
        for s in raw_skills:
            if isinstance(s, str) and s.strip():
                skills.append(s.strip())
            elif isinstance(s, dict):
                name_val = s.get("name") or s.get("skill") or ""
                if name_val.strip():
                    skills.append(name_val.strip())

    # Certifications (self-reported — NOT verified)
    certifications = []
    for c in (raw.get("certifications") or []):
        if isinstance(c, str) and c.strip():
            certifications.append(c.strip())
        elif isinstance(c, dict):
            n = c.get("name") or c.get("title") or ""
            if n.strip():
                certifications.append(n.strip())

    # Contact
    email = (raw.get("email") or "").strip()
    phone = (raw.get("phone") or "").strip()

    return {
        "_meta": {
            "source":               "brightdata",
            "dataset":              dataset_key,
            "ingestion_timestamp":  datetime.now(timezone.utc).isoformat(),
            "connector_version":    "3.0.0",
            "retention_class":      "pii",
            "record_id":            linkedin_id or linkedin_url,
        },
        "linkedin_id":      linkedin_id,
        "linkedin_url":     linkedin_url,
        "name":             name,
        "first_name":       first_name,
        "last_name":        last_name,
        "location_raw":     location_raw,
        "current_title":    current_title,
        "current_company":  current_company,
        "about":            (raw.get("about") or "").strip()[:2000],
        "experience":       experience,
        "experience_years": exp_years,
        "education":        education,
        "skills":           skills,
        "certifications":   certifications,
        "email":            email,
        "phone":            phone,
        "connections":      raw.get("connections"),
        "followers":        raw.get("followers"),
        "country_code":     (raw.get("country_code") or "").strip(),
    }


def _parse_year(s):
    m = re.search(r"\b(19|20)\d{2}\b", s or "")
    return int(m.group()) if m else None


# ── Bronze writer ─────────────────────────────────────────────────────────────

def flush_batch(client, batch: list, slug: str, run_date: str, batch_num: int):
    key  = f"bronze/candidates/brightdata/{slug}/{run_date}/batch_{batch_num:04d}.jsonl"
    body = "\n".join(json.dumps(r, default=str) for r in batch)
    client.put_object(
        Bucket=DST_BUCKET, Key=key,
        Body=body.encode(), ContentType="application/x-ndjson",
    )
    logger.info("  wrote %s (%d records)", key, len(batch))


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    logger.info("=== brightdata-bronze-ingest started ===")
    processed = load_processed()
    logger.info("Already processed files: %d", len(processed))

    files = list_dataset_files()
    logger.info("Dataset files found in source bucket: %d", len(files))

    new_files = [f for f in files if f["etag"] not in processed]
    logger.info("New files to process: %d", len(new_files))

    if not new_files:
        logger.info("Nothing to do.")
        return

    run_date  = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    s3_client = s3()
    total_records = 0

    for f in new_files:
        key  = f["key"]
        slug = dataset_slug(key)
        logger.info("Processing: %s (%.1f MB)", key, f["size"] / 1e6)

        batch     = []
        seen      = set()
        errors    = 0
        batch_num = 0
        count     = 0

        # Stream and flush — never accumulate full file in memory
        for raw in stream_file(key):
            try:
                rec = normalize_record(raw, key)
                uid = rec["linkedin_id"] or rec["linkedin_url"]
                if not uid or uid in seen:
                    continue
                seen.add(uid)
                batch.append(rec)
                count += 1
                if len(batch) >= BATCH_SIZE:
                    flush_batch(s3_client, batch, slug, run_date, batch_num)
                    batch_num += 1
                    batch = []
            except Exception as e:
                errors += 1
                if errors <= 5:
                    logger.warning("  normalize error: %s", e)

        if batch:
            flush_batch(s3_client, batch, slug, run_date, batch_num)
            batch_num += 1

        logger.info("  %d unique records | %d batches | errors=%d", count, batch_num, errors)
        processed[f["etag"]] = {
            "key":          key,
            "slug":         slug,
            "records":      count,
            "processed_at": datetime.now(timezone.utc).isoformat(),
        }
        total_records += count

    save_processed(processed)
    logger.info("=== brightdata-bronze-ingest done | total_records=%d ===", total_records)


if __name__ == "__main__":
    run()
