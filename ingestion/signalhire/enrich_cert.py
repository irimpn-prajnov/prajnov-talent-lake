"""
Pipeline: Cert Enrichment
Reads bronze/discovery/credly/certs/{cert}/ records (name + credly_url from Credly directory),
searches SignalHire by name to get linkedin_url, title, company, full location.
Writes merged record to bronze/enrichment/signalhire/certs/{cert}/

Flow:
  Credly directory → name + credly_url + location (partial)
       ↓
  SignalHire search by name → linkedin_url + title + company + full location

Usage (local):
  python enrich_cert.py --cert CIS-VR --api-key $SIGNALHIRE_API_KEY
  python enrich_cert.py --cert CIS-CSM --dry-run --limit 20

Glue: invoked via --cert and --SIGNALHIRE_API_KEY job arguments
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

import boto3
import requests

from common import bronze_path, hash_record, make_meta, s3_put, write_manifest, SeenSet

BUCKET        = "prajnov-talent-lake"
SIGNALHIRE_API = "https://www.signalhire.com/api/v1"


def _s3():
    return boto3.client("s3", region_name="ap-south-1")


def list_discovery_records(cert: str) -> list:
    prefix = f"bronze/discovery/credly/certs/{cert.lower().replace('-', '_')}/"
    paginator = _s3().get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json") and "_manifests" not in key:
                keys.append(key)
    return keys


def read_record(key: str) -> dict:
    resp = _s3().get_object(Bucket=BUCKET, Key=key)
    return json.loads(resp["Body"].read())


def sh_search_by_name(api_key: str, name: str, cert: str) -> dict:
    """Search SignalHire by full name + ServiceNow keyword to narrow results."""
    payload = {"keywords": f"{name} ServiceNow {cert}", "size": 1}
    resp = requests.post(
        f"{SIGNALHIRE_API}/candidate/searchByQuery",
        json=payload,
        headers={"apikey": api_key, "Content-Type": "application/json"},
        timeout=30,
    )
    if resp.status_code == 402:
        raise Exception("SignalHire daily quota exhausted")
    resp.raise_for_status()
    return resp.json()


def extract_candidate(resp: dict) -> dict:
    for key in ("profiles", "candidates", "results", "data"):
        val = resp.get(key)
        if isinstance(val, list) and val:
            return val[0]
    return {}


def build_record(discovery: dict, sh: dict, cert: str) -> dict:
    # Extract from SignalHire result
    skills_raw = sh.get("skills") or sh.get("keywords") or []
    skills_sh  = [s.get("name", s) if isinstance(s, dict) else s for s in skills_raw]

    exp     = sh.get("experience") or []
    title   = sh.get("title") or sh.get("currentTitle") or ""
    company = sh.get("company") or sh.get("currentCompany") or ""
    if not title and exp:
        title = exp[0].get("position", "") if isinstance(exp[0], dict) else ""
    if not company and exp:
        company = exp[0].get("company", "") if isinstance(exp[0], dict) else ""

    linkedin_url = (
        sh.get("linkedInUrl") or sh.get("linkedinUrl") or sh.get("linkedin_url") or ""
    )
    location = sh.get("location") or sh.get("city") or discovery.get("location") or ""

    data = {
        # From Credly discovery
        "credly_id":       discovery.get("credly_id", ""),
        "credly_url":      discovery.get("credly_url", ""),
        "credly_username": discovery.get("credly_username", ""),
        "name":            discovery.get("name", ""),
        "first_name":      discovery.get("first_name", ""),
        "last_name":       discovery.get("last_name", ""),
        "credly_skills":   discovery.get("skills", []),
        "badges":          discovery.get("badges", []),
        "badge_count":     discovery.get("badge_count", 0),
        "cert":            cert,
        # From SignalHire
        "signalhire_uid":  sh.get("uid") or sh.get("id") or sh.get("candidateId") or "",
        "linkedin_url":    linkedin_url,
        "title":           title,
        "company":         company,
        "location":        location,
        "skills":          skills_sh,
        "experience_years": sh.get("experienceYears"),
        "open_to_work":    sh.get("openToWork"),
        "experience":      exp,
        "education":       sh.get("education", []),
        # Meta
        "enrichment_source": "signalhire_name_search",
        "enriched_at":     datetime.now(timezone.utc).isoformat(),
    }
    meta = make_meta(
        source="signalhire",
        retention_class="pii",
        source_url=linkedin_url or discovery.get("credly_url", ""),
        extra={"raw_hash": hash_record(data), "cert": cert},
    )
    return {"_meta": meta, **data}


def run(cert: str, api_key: str, dry_run: bool, limit: int):
    print(f"\n{'='*60}")
    print(f"  Cert Enrichment: {cert} | dry_run={dry_run}")
    print(f"  Flow: Credly name → SignalHire search → linkedin_url + location")
    print(f"{'='*60}\n")

    keys = list_discovery_records(cert)
    print(f"Found {len(keys)} discovery records for {cert}")
    if not keys:
        print("Nothing to enrich — run cert discovery job first.")
        return

    seen  = SeenSet()
    stats = {"written": 0, "skipped_dup": 0, "errors": 0, "no_match": 0, "quota_hit": False}
    total = 0

    for key in keys:
        if limit and total >= limit:
            break
        if stats["quota_hit"]:
            print("SignalHire quota hit — stopping.")
            break

        try:
            discovery = read_record(key)
        except Exception as e:
            print(f"[ERROR] read {key}: {e}")
            stats["errors"] += 1
            continue

        name = discovery.get("name", "")
        uid  = discovery.get("credly_id", "") or name
        if not name:
            stats["no_match"] += 1
            continue
        if uid in seen:
            stats["skipped_dup"] += 1
            continue
        seen.add(uid)

        print(f"  Searching: {name}")
        sh = {}
        if not dry_run:
            try:
                resp = sh_search_by_name(api_key, name, cert)
                sh   = extract_candidate(resp)
                time.sleep(1)
            except Exception as e:
                if "quota" in str(e).lower():
                    stats["quota_hit"] = True
                    print(f"  QUOTA HIT after {total} records")
                    break
                print(f"  [ERROR] SignalHire: {e}")
                stats["errors"] += 1
                continue

        if not sh and not dry_run:
            stats["no_match"] += 1
            print(f"  ✗ No SignalHire match")
            continue

        record  = build_record(discovery, sh, cert)
        out_key = bronze_path("enrichment", f"signalhire/certs/{cert.lower().replace('-', '_')}")
        s3_put(out_key, record, dry_run=dry_run)
        stats["written"] += 1
        total += 1

        li  = "✓" if record.get("linkedin_url") else "✗"
        loc = record.get("location") or "—"
        ttl = (record.get("title") or "N/A")[:45]
        print(f"  {li} LinkedIn | {loc:<25} | {ttl}")

    write_manifest(f"enrich_cert_{cert.lower().replace('-', '_')}", stats, dry_run=dry_run)
    print(f"\n{'='*60}")
    print(f"  DONE {cert} | stats={stats}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cert", required=True, help="e.g. CIS-VR")
    parser.add_argument("--api-key", default=os.getenv("SIGNALHIRE_API_KEY", ""))
    parser.add_argument("--SIGNALHIRE_API_KEY", default="")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    args, _ = parser.parse_known_args()

    api_key = args.api_key or args.SIGNALHIRE_API_KEY
    if not api_key and not args.dry_run:
        print("ERROR: SIGNALHIRE_API_KEY required")
        sys.exit(1)

    run(args.cert, api_key, args.dry_run, args.limit)
