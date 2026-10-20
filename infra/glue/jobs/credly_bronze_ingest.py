"""
Glue Job: credly-bronze-ingest
Scrapes all ServiceNow cert earners from Credly public directory API.
For each cert:
  1. Fetch all pages from Credly directory
  2. Dedup by credly_id against existing consolidated file
  3. Write one consolidated/all.json per cert to S3 bronze

Output: s3://prajnov-talent-lake/bronze/discovery/credly/certs/{slug}/consolidated/all.json

Schedule: Weekly (Sunday 01:00 UTC) via Step Functions
"""
import json, logging, os, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import boto3, requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BUCKET         = os.getenv("BUCKET", "prajnov-talent-lake")
REGION         = os.getenv("AWS_REGION", "ap-south-1")
CREDLY_ORG_ID  = "687e2b74-213e-42f1-9f9c-71bd4fe8bea4"
CREDLY_DIR_URL = "https://www.credly.com/api/v1/directory"
PAGE_SIZE      = 20
RATE_SLEEP     = 0.3   # seconds between pages per cert
MAX_WORKERS    = 4     # parallel certs (be polite to Credly)

HEADERS = {
    "Accept":     "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer":    "https://www.credly.com/organizations/servicenow-new/directory",
}

# All 19 ServiceNow certs — code → badge name as it appears in Credly directory
CERT_CONFIG = {
    "CIS-VR":    "Certified Implementation Specialist - Vulnerability Response (CIS-VR)",
    "CIS-CSM":   "Certified Implementation Specialist - Customer Service Management (CIS-CSM)",
    "CIS-ITSM":  "Certified Implementation Specialist - IT Service Management (CIS-ITSM)",
    "CIS-HR":    "Certified Implementation Specialist - Human Resources (CIS-HR)",
    "CIS-DISCO": "Certified Implementation Specialist - Discovery (CIS-DISCO)",
    "CIS-HAM":   "Certified Implementation Specialist - Hardware Asset Management (CIS-HAM)",
    "CIS-SAM":   "Certified Implementation Specialist - Software Asset Management (CIS-SAM)",
    "CIS-EM":    "Certified Implementation Specialist - Event Management (CIS-EM)",
    "CIS-SPM":   "Certified Implementation Specialist - Strategic Portfolio Management (CIS-SPM)",
    "CIS-RC":    "Certified Implementation Specialist - Risk and Compliance (CIS-RC)",
    "CIS-SM":    "Certified Implementation Specialist - Service Mapping (CIS-SM)",
    "CIS-SIR":   "Certified Implementation Specialist - Security Incident Response (CIS-SIR)",
    "CIS-FSM":   "Certified Implementation Specialist - Field Service Management (CIS-FSM)",
    "CIS-PA":    "Certified Implementation Specialist - Platform Analytics (CIS-PA)",
    "CIS-TPRM":  "Certified Implementation Specialist - Third-party Risk Management (CIS-TPRM)",
    "CIS-SP":    "Certified Implementation Specialist - Service Provider (CIS-SP)",
    "CSA":       "ServiceNow Certified System Administrator (CSA)",
    "CAD":       "Certified Application Developer (CAD)",
    "CMA":       "Certified Master Architect (CMA)",
}


def get_s3():
    return boto3.client("s3", region_name=REGION)


def load_existing(s3_client, slug: str) -> dict:
    """Load existing consolidated file → {credly_id: record}. Empty dict if not found."""
    key = f"bronze/discovery/credly/certs/{slug}/consolidated/all.json"
    try:
        body = s3_client.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        records = json.loads(body)
        return {r.get("credly_id", ""): r for r in records if r.get("credly_id")}
    except s3_client.exceptions.NoSuchKey:
        return {}
    except Exception as e:
        logger.warning("Could not load existing %s: %s", key, e)
        return {}


def fetch_page(badge_name: str, page: int) -> dict:
    resp = requests.get(
        CREDLY_DIR_URL,
        params={
            "organization_id":    CREDLY_ORG_ID,
            "filter[badge_name]": badge_name,
            "page":               page,
            "page_size":          PAGE_SIZE,
        },
        headers=HEADERS,
        timeout=20,
    )
    resp.raise_for_status()
    return resp.json()


def build_record(earner: dict, cert_code: str, now: str) -> dict:
    vanity   = earner.get("vanity_url", "").lstrip("/")
    username = vanity.replace("users/", "") if vanity else ""
    badges   = [
        {
            "name": b.get("name"),
            "date": b.get("date"),
            "url":  f"https://www.credly.com{b.get('url', '')}",
        }
        for b in (earner.get("highlighted_badges") or [])
    ]
    return {
        "credly_id":       earner.get("id", ""),
        "credly_username": username,
        "credly_url":      f"https://www.credly.com/{vanity}",
        "name":            f"{earner.get('first_name','')} {earner.get('last_name','')}".strip(),
        "first_name":      earner.get("first_name", ""),
        "last_name":       earner.get("last_name", ""),
        "location":        earner.get("location", ""),
        "badge_count":     earner.get("total_badge_count", 0),
        "skills":          [s.get("name", "") for s in (earner.get("skills") or [])],
        "badges":          badges,
        "cert":            cert_code,
        "discovered_at":   now,
    }


def scrape_cert(cert_code: str, badge_name: str) -> tuple:
    """
    Scrape all earners for one cert.
    Returns (cert_code, slug, records_dict) where records_dict = {credly_id: record}
    """
    slug = cert_code.lower().replace("-", "_")
    now  = datetime.now(timezone.utc).isoformat()
    s3c  = get_s3()

    # Load existing records — incremental update
    existing = load_existing(s3c, slug)
    logger.info("  %s: %d existing records", cert_code, len(existing))

    try:
        first        = fetch_page(badge_name, page=1)
        meta         = first.get("metadata", {})
        total_count  = meta.get("total_count", 0)
        total_pages  = meta.get("total_pages", 1)
        logger.info("  %s: %d earners across %d pages", cert_code, total_count, total_pages)
    except Exception as e:
        logger.error("  %s: failed to fetch page 1: %s", cert_code, e)
        return cert_code, slug, existing

    new_count = 0
    for page in range(1, total_pages + 1):
        try:
            result  = first if page == 1 else fetch_page(badge_name, page)
            earners = result.get("data", [])
            if not earners:
                break
            for earner in earners:
                cid = earner.get("id", "")
                if not cid:
                    continue
                if cid not in existing:
                    existing[cid] = build_record(earner, cert_code, now)
                    new_count += 1
            time.sleep(RATE_SLEEP)
        except Exception as e:
            logger.error("  %s page %d error: %s", cert_code, page, e)

    logger.info("  %s: %d new earners | %d total", cert_code, new_count, len(existing))
    return cert_code, slug, existing


def write_consolidated(s3_client, slug: str, records: dict):
    """Write all records as one consolidated JSON array to S3."""
    key  = f"bronze/discovery/credly/certs/{slug}/consolidated/all.json"
    body = json.dumps(list(records.values()), ensure_ascii=False, default=str).encode()
    s3_client.put_object(
        Bucket=BUCKET, Key=key, Body=body, ContentType="application/json"
    )
    logger.info("  Written → s3://%s/%s (%d records)", BUCKET, key, len(records))


def write_manifest(s3_client, stats: dict):
    now = datetime.now(timezone.utc)
    key = f"bronze/discovery/credly/_manifests/{now.strftime('%Y%m%d_%H%M%S')}.json"
    s3_client.put_object(
        Bucket=BUCKET, Key=key,
        Body=json.dumps({
            "job":          "credly_bronze_ingest",
            "run_at":       now.isoformat(),
            "certs_run":    len(stats),
            "total_records": sum(v["total"] for v in stats.values()),
            "new_records":   sum(v["new"] for v in stats.values()),
            "per_cert":      stats,
        }, default=str).encode(),
        ContentType="application/json",
    )
    logger.info("Manifest → s3://%s/%s", BUCKET, key)


def run(certs_filter: list = None):
    logger.info("=== credly-bronze-ingest started ===")
    s3c   = get_s3()
    certs = {k: v for k, v in CERT_CONFIG.items()
             if not certs_filter or k in certs_filter}

    logger.info("Scraping %d certs with %d parallel workers", len(certs), MAX_WORKERS)

    stats = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {
            ex.submit(scrape_cert, code, badge): code
            for code, badge in certs.items()
        }
        for future in as_completed(futures):
            cert_code, slug, records = future.result()
            existing_count = len(load_existing(s3c, slug))
            write_consolidated(s3c, slug, records)
            stats[cert_code] = {
                "slug":  slug,
                "total": len(records),
                "new":   len(records) - existing_count,
            }

    write_manifest(s3c, stats)

    logger.info("=== credly-bronze-ingest done ===")
    for code, s in sorted(stats.items()):
        logger.info("  %-12s total=%-6d new=%d", code, s["total"], s["new"])
    logger.info("  TOTAL: %d records across %d certs",
                sum(v["total"] for v in stats.values()), len(stats))


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--certs", nargs="+", default=None,
                        help="Subset of certs to run e.g. --certs CIS-CSM CIS-VR")
    args, _ = parser.parse_known_args()
    run(args.certs)
