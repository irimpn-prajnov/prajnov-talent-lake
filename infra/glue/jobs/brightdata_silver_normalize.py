"""
Glue Job: brightdata-silver-normalize
Source:  bronze/candidates/brightdata/{dataset_slug}/{date}/batch_*.jsonl
Output:  silver/candidate/source=brightdata/{run_date}/{run_id}.parquet
         silver/candidate_certification/source=brightdata/{run_date}/{run_id}.parquet
         silver/candidate_skill/source=brightdata/{run_date}/{run_id}.parquet
         silver/candidate_experience/source=brightdata/{run_date}/{run_id}.parquet
         silver/candidate_education/source=brightdata/{run_date}/{run_id}.parquet

Incremental: watermark tracks last processed bronze key prefix.
Dedup: linkedin_id is the primary key — one silver row per person.
"""
import io, json, logging, os, re, uuid, sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import boto3
import pyarrow as pa
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
STATE_KEY = "glue-state/brightdata_silver_normalize/last_run.json"
WORKERS   = 20


def s3():
    return boto3.client("s3", region_name=REGION)


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


# ── Bronze loader ─────────────────────────────────────────────────────────────

def list_bronze_keys(watermark: str) -> list:
    """List all bronze JSONL keys modified after watermark."""
    client = s3()
    pag    = client.get_paginator("list_objects_v2")
    keys   = []
    wm_dt  = datetime.fromisoformat(watermark.replace("Z", "+00:00"))
    for page in pag.paginate(Bucket=BUCKET, Prefix="bronze/candidates/brightdata/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".jsonl") and obj["LastModified"] > wm_dt:
                keys.append(obj["Key"])
    return keys


def fetch_jsonl(key: str) -> list:
    body = s3().get_object(Bucket=BUCKET, Key=key)["Body"].read().decode()
    records = []
    for line in body.splitlines():
        line = line.strip()
        if line:
            try:
                records.append(json.loads(line))
            except Exception:
                pass
    return records


def load_bronze(watermark: str) -> list:
    keys = list_bronze_keys(watermark)
    logger.info("Bronze JSONL files to process: %d", len(keys))
    if not keys:
        return []

    all_records = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(fetch_jsonl, k): k for k in keys}
        for i, f in enumerate(as_completed(futures), 1):
            try:
                all_records.extend(f.result())
            except Exception as e:
                logger.warning("fetch error %s: %s", futures[f], e)
            if i % 20 == 0:
                logger.info("  %d/%d files fetched, %d records so far", i, len(keys), len(all_records))

    # Dedup by linkedin_id — last write wins (most recent dataset)
    seen, deduped = set(), []
    for rec in all_records:
        uid = rec.get("linkedin_id") or rec.get("_meta", {}).get("record_id") or ""
        if uid and uid not in seen:
            seen.add(uid)
            deduped.append(rec)

    logger.info("Total: %d raw | %d after dedup", len(all_records), len(deduped))
    return deduped


# ── Normalizer ────────────────────────────────────────────────────────────────

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

COUNTRY_CODES = {
    "united states":"US","usa":"US","us":"US","united kingdom":"GB","uk":"GB",
    "canada":"CA","australia":"AU","india":"IN","germany":"DE","france":"FR",
    "netherlands":"NL","singapore":"SG","ireland":"IE","new zealand":"NZ",
    "south africa":"ZA","uae":"AE","united arab emirates":"AE",
}

CERT_NORM = {
    "csa":"csa","certified system administrator":"csa",
    "servicenow certified system administrator":"csa",
    "cad":"cad","certified application developer":"cad",
    "cta":"cta","cma":"cma",
    "cis-itsm":"cis-itsm","it service management":"cis-itsm",
    "cis-itom":"cis-itom","it operations management":"cis-itom",
    "cis-hrsd":"cis-hrsd","cis-hr":"cis-hrsd","hr service delivery":"cis-hrsd",
    "cis-csm":"cis-csm","customer service management":"cis-csm",
    "cis-fsm":"cis-fsm","field service management":"cis-fsm",
    "cis-sam":"cis-sam","software asset management":"cis-sam",
    "cis-ham":"cis-ham","hardware asset management":"cis-ham",
    "cis-itam":"cis-itam","it asset management":"cis-itam",
    "cis-spm":"cis-spm","strategic portfolio management":"cis-spm",
    "cis-ppm":"cis-ppm","project portfolio management":"cis-ppm",
    "cis-rc":"cis-rc","risk and compliance":"cis-rc",
    "cis-sir":"cis-sir","security incident response":"cis-sir",
    "cis-vr":"cis-vr","vulnerability response":"cis-vr",
    "cis-disco":"cis-disco","cis-discovery":"cis-disco","discovery":"cis-disco",
    "cis-apm":"cis-apm","application portfolio management":"cis-apm",
    "cis-em":"cis-em","event management":"cis-em",
    "cis-sm":"cis-sm","service mapping":"cis-sm",
    "cis-tprm":"cis-tprm","cis-fso":"cis-fso","cis-pa":"cis-pa",
}


def parse_location(raw: str):
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
        if not country and parts[1] in US_STATES:
            country = "US"
    elif len(parts) == 1:
        if parts[0] in US_STATES:
            state, country = US_STATES[parts[0]], "US"
        else:
            city = parts[0]
    return city, state, country or ("US" if state else "")


def normalize_cert(raw: str):
    s = re.sub(r"[^\w\s-]", "", (raw or "").lower().strip())
    s = re.sub(r"\s+", " ", s).strip()
    if s in CERT_NORM:
        return CERT_NORM[s]
    for key in sorted(CERT_NORM, key=len, reverse=True):
        if key in s:
            return CERT_NORM[key]
    return None


def normalize_record(b: dict, now: str) -> tuple:
    """Returns (candidate, certs[], skills[], experience[], education[])."""
    cid = str(uuid.uuid4())
    lid = b.get("linkedin_id", "")

    loc_raw = b.get("location_raw", "")
    city, state, country = parse_location(loc_raw)
    # BrightData country_code field is reliable
    if b.get("country_code"):
        country = b["country_code"].upper()

    clean_certs, dirty_certs = [], []
    for raw_cert in (b.get("certifications") or []):
        code = normalize_cert(raw_cert)
        if code and code not in clean_certs:
            clean_certs.append(code)
        elif not code:
            dirty_certs.append(raw_cert)

    candidate = {
        "candidate_id":         cid,
        "linkedin_id":          lid,
        "linkedin_url":         b.get("linkedin_url", ""),
        "name":                 b.get("name", ""),
        "first_name":           b.get("first_name", ""),
        "last_name":            b.get("last_name", ""),
        "current_title":        b.get("current_title", ""),
        "current_company":      b.get("current_company", ""),
        "location_raw":         loc_raw,
        "city":                 city,
        "state":                state,
        "country":              country,
        "about":                (b.get("about") or "")[:2000],
        "experience_years":     b.get("experience_years"),
        "skills":               json.dumps(b.get("skills") or []),
        "certifications_raw":   json.dumps(b.get("certifications") or []),
        "certifications_clean": json.dumps(clean_certs),
        "certifications_dirty": json.dumps(dirty_certs),
        "connections":          b.get("connections"),
        "followers":            b.get("followers"),
        "email":                b.get("email", ""),
        "phone":                b.get("phone", ""),
        "source_dataset":       b.get("_meta", {}).get("dataset", ""),
        "source":               "brightdata",
        "created_at":           now,
        "updated_at":           now,
    }

    certs = []
    for code in clean_certs:
        certs.append({
            "id":                  str(uuid.uuid4()),
            "candidate_id":        cid,
            "linkedin_id":         lid,
            "cert_code":           code,
            "verification_status": "self_reported",
            "source":              "brightdata_self_reported",
            "confidence":          0.50,
            "created_at":          now,
        })

    skills = []
    seen_skills = set()
    for s in (b.get("skills") or []):
        k = s.lower().strip()
        if k and k not in seen_skills:
            seen_skills.add(k)
            skills.append({
                "id":           str(uuid.uuid4()),
                "candidate_id": cid,
                "linkedin_id":  lid,
                "skill_name":   s.strip(),
                "source":       "brightdata",
                "confidence":   0.80,
                "created_at":   now,
            })

    experience = []
    seen_exp = set()
    for exp in (b.get("experience") or []):
        company = (exp.get("company") or "").strip()
        title   = (exp.get("title") or "").strip()
        key     = (company.lower(), title.lower())
        if not company and not title:
            continue
        if key in seen_exp:
            continue
        seen_exp.add(key)
        experience.append({
            "id":           str(uuid.uuid4()),
            "candidate_id": cid,
            "linkedin_id":  lid,
            "company":      company,
            "title":        title,
            "start_date":   exp.get("start_date", ""),
            "end_date":     exp.get("end_date", ""),
            "start_year":   exp.get("start_year"),
            "end_year":     exp.get("end_year"),
            "is_current":   exp.get("is_current", False),
            "location":     exp.get("location", ""),
            "source":       "brightdata",
            "created_at":   now,
        })

    education = []
    seen_edu = set()
    for edu in (b.get("education") or []):
        institution = (edu.get("institution") or "").strip()
        degree      = (edu.get("degree") or "").strip()
        key         = (institution.lower(), degree.lower())
        if not institution:
            continue
        if key in seen_edu:
            continue
        seen_edu.add(key)
        education.append({
            "id":           str(uuid.uuid4()),
            "candidate_id": cid,
            "linkedin_id":  lid,
            "institution":  institution,
            "degree":       degree,
            "field":        (edu.get("field") or "").strip(),
            "start_year":   edu.get("start_year"),
            "end_year":     edu.get("end_year"),
            "source":       "brightdata",
            "created_at":   now,
        })

    return candidate, certs, skills, experience, education


# ── Parquet writer ────────────────────────────────────────────────────────────

def write_parquet(records: list, prefix: str, run_id: str):
    if not records:
        return
    key = f"{prefix}/{run_id}.parquet"
    # Ensure all rows have same keys (fill missing with None)
    all_keys = set()
    for r in records:
        all_keys.update(r.keys())
    normalized = [{k: r.get(k) for k in all_keys} for r in records]

    table = pa.Table.from_pylist(normalized)
    buf   = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    s3().put_object(
        Bucket=BUCKET, Key=key,
        Body=buf.read(), ContentType="application/octet-stream",
    )
    logger.info("  wrote %s (%d rows)", key, len(records))


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    run_start = datetime.now(timezone.utc).isoformat()
    run_id    = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    run_date  = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    watermark = get_watermark()

    logger.info("=== brightdata-silver-normalize started | watermark=%s ===", watermark)

    records = load_bronze(watermark)
    if not records:
        logger.info("Nothing to process.")
        return

    logger.info("Normalizing %d records...", len(records))
    now = datetime.now(timezone.utc).isoformat()

    buf_candidate, buf_cert, buf_skill, buf_exp, buf_edu = [], [], [], [], []
    errors = 0

    for i, b in enumerate(records):
        try:
            c, ce, sk, ex, ed = normalize_record(b, now)
            buf_candidate.append(c)
            buf_cert.extend(ce)
            buf_skill.extend(sk)
            buf_exp.extend(ex)
            buf_edu.extend(ed)
        except Exception as e:
            errors += 1
            if errors <= 5:
                logger.warning("normalize error lid=%s: %s", b.get("linkedin_id", "?"), e)
        if (i + 1) % 10000 == 0:
            logger.info("  %d/%d normalized...", i + 1, len(records))

    logger.info("Writing Parquet to S3...")
    dp = f"silver/candidate/source=brightdata/{run_date}"
    write_parquet(buf_candidate, dp,                                                    run_id)
    write_parquet(buf_cert,      f"silver/candidate_certification/source=brightdata/{run_date}", run_id)
    write_parquet(buf_skill,     f"silver/candidate_skill/source=brightdata/{run_date}",         run_id)
    write_parquet(buf_exp,       f"silver/candidate_experience/source=brightdata/{run_date}",    run_id)
    write_parquet(buf_edu,       f"silver/candidate_education/source=brightdata/{run_date}",     run_id)

    set_watermark(run_start)

    logger.info("=== brightdata-silver-normalize done ===")
    logger.info("  candidates=%d certs=%d skills=%d exp=%d edu=%d errors=%d",
                len(buf_candidate), len(buf_cert), len(buf_skill),
                len(buf_exp), len(buf_edu), errors)


if __name__ == "__main__":
    run()
