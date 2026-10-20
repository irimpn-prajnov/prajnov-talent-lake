"""
Pipeline: ServiceNow Cert Discovery via Credly Directory
Source:   https://www.credly.com/api/v1/directory
Output:   s3://prajnov-talent-lake/bronze/discovery/credly/certs/{cert}/YYYY/MM/DD/{uuid}.json

Fetches all earners of a given ServiceNow cert directly from Credly's public directory.
Each record includes: name, location, vanity_url, skills, badge list.

Usage (local):
  python find_cert.py --cert CIS-CSM
  python find_cert.py --cert CIS-VR --dry-run
  python find_cert.py --cert CIS-CSM --limit 50

Glue: invoked via --cert job argument
"""
import argparse
import sys
import time
from datetime import datetime, timezone

import requests

from common import bronze_path, hash_record, make_meta, s3_put, write_manifest, SeenSet

CREDLY_ORG_ID  = "687e2b74-213e-42f1-9f9c-71bd4fe8bea4"  # ServiceNow on Credly
CREDLY_DIR_URL = "https://www.credly.com/api/v1/directory"

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.credly.com/organizations/servicenow-new/directory",
}

# Maps cert code → exact badge name as it appears in Credly directory
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


def fetch_page(badge_name: str, page: int, page_size: int = 20) -> dict:
    params = {
        "organization_id": CREDLY_ORG_ID,
        "filter[badge_name]": badge_name,
        "page": page,
        "page_size": page_size,
    }
    resp = requests.get(CREDLY_DIR_URL, params=params, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    return resp.json()


def build_record(earner: dict, cert: str) -> dict:
    vanity = earner.get("vanity_url", "").lstrip("/")  # users/john-smith-abc
    username = vanity.replace("users/", "") if vanity else ""
    skills = [s.get("name", "") for s in (earner.get("skills") or [])]
    badges = [
        {
            "name": b.get("name"),
            "date": b.get("date"),
            "url": f"https://www.credly.com{b.get('url', '')}",
        }
        for b in (earner.get("highlighted_badges") or [])
    ]
    data = {
        "credly_id":    earner.get("id", ""),
        "first_name":   earner.get("first_name", ""),
        "last_name":    earner.get("last_name", ""),
        "name":         f"{earner.get('first_name', '')} {earner.get('last_name', '')}".strip(),
        "location":     earner.get("location", ""),
        "credly_url":   f"https://www.credly.com/{vanity}",
        "credly_username": username,
        "badge_count":  earner.get("total_badge_count", 0),
        "skills":       skills,
        "badges":       badges,
        "cert":         cert,
        "discovered_at": datetime.now(timezone.utc).isoformat(),
    }
    meta = make_meta(
        source="credly",
        source_url=data["credly_url"],
        extra={"raw_hash": hash_record(data), "cert": cert, "record_id": data["credly_id"]},
    )
    return {"_meta": meta, **data}


def run(cert: str, dry_run: bool, limit: int):
    badge_name = CERT_CONFIG.get(cert)
    if not badge_name:
        print(f"ERROR: unknown cert '{cert}'. Add it to CERT_CONFIG.")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  Credly Directory: {cert}")
    print(f"  Badge: {badge_name}")
    print(f"{'='*60}\n")

    # Get total count first
    first = fetch_page(badge_name, page=1, page_size=20)
    meta = first.get("metadata", {})
    total_count = meta.get("total_count", 0)
    total_pages = meta.get("total_pages", 1)
    print(f"Total earners: {total_count} across {total_pages} pages\n")

    seen = SeenSet()
    stats = {"written": 0, "skipped_dup": 0, "errors": 0}
    total = 0
    page = 1
    batch = []
    BATCH_SIZE = 10_000
    cert_key = cert.lower().replace('-', '_')

    def flush(b):
        key = bronze_path("discovery", f"credly/certs/{cert_key}")
        s3_put(key, b, dry_run=dry_run)
        stats["written"] += len(b)
        print(f"  [FLUSH] {len(b)} records → {key}")

    while page <= total_pages:
        if limit and total >= limit:
            break
        try:
            result = first if page == 1 else fetch_page(badge_name, page=page, page_size=20)
            earners = result.get("data", [])
            if not earners:
                break
            time.sleep(0.3)
        except Exception as e:
            print(f"[ERROR] page {page}: {e}")
            stats["errors"] += 1
            page += 1
            continue

        for earner in earners:
            if limit and total >= limit:
                break
            uid = earner.get("id", "")
            if uid in seen:
                stats["skipped_dup"] += 1
                continue
            seen.add(uid)
            record = build_record(earner, cert)
            batch.append(record)
            total += 1
            print(f"  ✓ {record['name']} | {record['location'] or 'N/A'} | badges={record['badge_count']}")
            if len(batch) >= BATCH_SIZE:
                flush(batch)
                batch = []

        print(f"  page {page}/{total_pages} — total so far: {total}\n")
        page += 1

    if batch:
        flush(batch)

    write_manifest(f"credly_cert_{cert_key}", stats, dry_run=dry_run)
    print(f"{'='*60}")
    print(f"  DONE {cert} — {total} earners | stats={stats}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cert", required=True, choices=list(CERT_CONFIG.keys()))
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    args, _ = parser.parse_known_args()
    run(args.cert, args.dry_run, args.limit)
