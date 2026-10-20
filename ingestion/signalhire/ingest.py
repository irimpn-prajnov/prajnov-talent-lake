"""
Pipeline: SignalHire Enrichment
Source:   SignalHire API (search + reveal)
Output:   s3://prajnov-talent-lake/bronze/enrichment/signalhire/YYYY/MM/DD/{uuid}.json

Two modes:
  1. search  — search by title/skills/location → new candidate profiles
  2. enrich  — take linkedin URLs from a file and reveal contact info

Usage:
  python ingest.py --mode search --title "ServiceNow Architect" --location "United States"
  python ingest.py --mode enrich --input-file linkedin_urls.txt
  python ingest.py --mode search --dry-run --limit 50
"""
import argparse
import os
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import bronze_path, hash_record, make_meta, s3_put, write_manifest, SeenSet

SIGNALHIRE_API = "https://www.signalhire.com/api/v1"

# US states for location sharding — gets past the 100-result cap per query
US_STATES = [
    "California", "Texas", "New York", "Illinois", "Florida", "Georgia",
    "Washington", "Virginia", "New Jersey", "Pennsylvania", "Massachusetts",
    "North Carolina", "Colorado", "Arizona", "Michigan", "Ohio", "Maryland",
    "Minnesota", "Tennessee", "Missouri", "Indiana", "Wisconsin", "Connecticut",
    "Oregon", "Nevada", "Utah", "South Carolina", "Alabama", "Louisiana",
    "Kentucky", "Oklahoma", "Iowa", "Kansas", "Arkansas", "Mississippi",
    "Nebraska", "New Mexico", "Idaho", "Hawaii", "New Hampshire", "Maine",
    "Rhode Island", "Montana", "Delaware", "South Dakota", "North Dakota",
    "Alaska", "Vermont", "Wyoming", "West Virginia",
]

# CIS-CSM first, then other ServiceNow, then broader IT
# Each config runs across all US_STATES to get past the 100/query cap
SEARCH_CONFIGS = [
    {"keywords": "CIS-CSM ServiceNow",                          "size": 100},
    {"keywords": "ServiceNow CSM certified",                    "size": 100},
    {"keywords": "ServiceNow CIS-ITSM",                         "size": 100},
    {"keywords": "ServiceNow CIS-HR",                           "size": 100},
    {"keywords": "ServiceNow Developer",                        "size": 100},
    {"keywords": "ServiceNow Architect",                        "size": 100},
    {"keywords": "ServiceNow Administrator",                    "size": 100},
    {"keywords": "AWS Solutions Architect",                     "size": 100},
    {"keywords": "Azure DevOps Engineer",                       "size": 100},
    {"keywords": "Data Engineer Databricks Snowflake",          "size": 100},
    {"keywords": "Salesforce Developer",                        "size": 100},
    {"keywords": "DevOps Engineer Kubernetes Terraform",        "size": 100},
]


def sh_headers(api_key: str) -> dict:
    return {"apikey": api_key, "Content-Type": "application/json"}


def sh_search(api_key: str, keywords: str, location: str, size: int = 100) -> dict:
    payload = {"keywords": keywords, "location": location, "size": size}
    resp = requests.post(
        f"{SIGNALHIRE_API}/candidate/searchByQuery",
        json=payload, headers=sh_headers(api_key), timeout=30
    )
    if resp.status_code == 402:
        raise Exception("Daily search quota exhausted — resets tomorrow")
    resp.raise_for_status()
    return resp.json()


def sh_reveal(api_key: str, linkedin_url: str, callback_url: str = "") -> dict:
    payload = {"items": [linkedin_url]}
    if callback_url:
        payload["callbackUrl"] = callback_url
    resp = requests.post(
        f"{SIGNALHIRE_API}/candidate/search",
        json=payload, headers=sh_headers(api_key), timeout=20
    )
    resp.raise_for_status()
    return resp.json()


def extract_candidates(resp: dict) -> list:
    for key in ("profiles", "candidates", "results", "data"):
        val = resp.get(key)
        if isinstance(val, list):
            return val
    return []


def build_record(candidate: dict, mode: str, query_meta: dict) -> tuple:
    uid = candidate.get("uid") or candidate.get("id") or candidate.get("candidateId", "")
    name = (candidate.get("name") or candidate.get("fullName") or
            f"{candidate.get('firstName','')} {candidate.get('lastName','')}".strip())
    linkedin_url = candidate.get("linkedInUrl") or candidate.get("linkedinUrl") or candidate.get("linkedin_url", "")

    skills_raw = candidate.get("skills") or candidate.get("keywords") or []
    skills = [s.get("name", s) if isinstance(s, dict) else s for s in skills_raw] if isinstance(skills_raw, list) else []

    # title from most recent experience
    exp = candidate.get("experience") or []
    title = candidate.get("title") or candidate.get("currentTitle") or ""
    company = candidate.get("company") or candidate.get("currentCompany") or ""
    if not title and exp:
        title = exp[0].get("position", "") if isinstance(exp[0], dict) else ""
    if not company and exp:
        company = exp[0].get("company", "") if isinstance(exp[0], dict) else ""

    data = {
        "signalhire_uid": uid,
        "linkedin_url": linkedin_url,
        "name": name,
        "title": title,
        "company": company,
        "location": candidate.get("location") or candidate.get("city", ""),
        "skills": skills,
        "experience_years": candidate.get("experienceYears"),
        "open_to_work": candidate.get("openToWork"),
        "experience": candidate.get("experience", []),
        "education": candidate.get("education", []),
        "mode": mode,
        **query_meta,
    }
    meta = make_meta(
        source="signalhire",
        retention_class="pii",
        source_url=linkedin_url,
        extra={"raw_hash": hash_record(data), "record_id": uid},
    )
    return {"_meta": meta, **data}, uid or linkedin_url or name


def run_search(api_key: str, configs: list, states: list, dry_run: bool, limit: int):
    seen = SeenSet()
    stats = {"written": 0, "skipped_dup": 0, "errors": 0, "quota_hit": False}
    total = 0

    for cfg in configs:
        keywords = cfg["keywords"]
        size = cfg["size"]
        print(f"\n{'='*60}")
        print(f"Keywords: {keywords}")
        print(f"Sharding across {len(states)} states")
        print(f"{'='*60}")

        for location in states:
            if stats["quota_hit"]:
                print("  Quota exhausted — stopping. Run again tomorrow.")
                write_manifest("signalhire", stats, dry_run=dry_run)
                return

            try:
                result = sh_search(api_key, keywords, location, size)
                candidates = extract_candidates(result)
                if dry_run:
                    candidates = candidates[:10]
                if not candidates:
                    continue
                print(f"  {location}: {len(candidates)} candidates")
                time.sleep(2)
            except Exception as e:
                if "quota" in str(e).lower():
                    stats["quota_hit"] = True
                    print(f"  QUOTA HIT at {location} — written={stats['written']} so far")
                    break
                print(f"  [ERROR] {location}: {e}")
                stats["errors"] += 1
                continue

            for c in candidates:
                try:
                    record, uid = build_record(c, "search", {"search_keywords": keywords, "search_location": location})
                    if uid in seen:
                        stats["skipped_dup"] += 1
                        continue
                    seen.add(uid)
                    key = bronze_path("enrichment", "signalhire")
                    s3_put(key, record, dry_run=dry_run)
                    stats["written"] += 1
                    total += 1
                    if stats["written"] % 100 == 0:
                        print(f"  written={stats['written']} skipped={stats['skipped_dup']}")
                except Exception as e:
                    stats["errors"] += 1
                    print(f"  [ERROR] record: {e}")

                if limit and total >= limit:
                    write_manifest("signalhire", stats, dry_run=dry_run)
                    print(f"Done (limit reached): {stats}")
                    return

            if dry_run:
                print(f"  [DRY-RUN] stopping after first state — {len(candidates)} real results, S3 writes skipped")
                write_manifest("signalhire", stats, dry_run=dry_run)
                return

    write_manifest("signalhire", stats, dry_run=dry_run)
    print(f"\nDone: {stats}")


def run_enrich(api_key: str, input_file: str, dry_run: bool, limit: int):
    urls = [l.strip() for l in open(input_file) if l.strip()]
    seen = SeenSet()
    stats = {"written": 0, "skipped_dup": 0, "errors": 0}

    for i, url in enumerate(urls):
        if limit and i >= limit:
            break
        if url in seen:
            stats["skipped_dup"] += 1
            continue
        seen.add(url)

        try:
            resp = sh_reveal(api_key, url)
            candidates = extract_candidates(resp)
            if not candidates:
                candidates = [resp]  # sometimes the response itself is the candidate
            for c in candidates:
                record, uid = build_record(c, "enrich", {"source_linkedin_url": url})
                key = bronze_path("enrichment", "signalhire")
                s3_put(key, record, dry_run=dry_run)
                stats["written"] += 1
            time.sleep(0.5)
        except Exception as e:
            stats["errors"] += 1
            print(f"[ERROR] reveal url={url}: {e}")

    write_manifest("signalhire_enrich", stats, dry_run=dry_run)
    print(f"Done: {stats}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-key", default=os.getenv("SIGNALHIRE_API_KEY"))
    parser.add_argument("--mode", choices=["search", "enrich"], default="search")
    parser.add_argument("--input-file", help="File with LinkedIn URLs (enrich mode)")
    parser.add_argument("--keywords", help="Single keywords override for search mode")
    parser.add_argument("--states", default=None, help="Comma-separated states to shard across")
    parser.add_argument("--location", default="United States", help="Single location (overrides state sharding)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    if not args.api_key:
        print("ERROR: SIGNALHIRE_API_KEY required")
        sys.exit(1)

    if args.mode == "enrich":
        if not args.input_file:
            print("ERROR: --input-file required for enrich mode")
            sys.exit(1)
        run_enrich(args.api_key, args.input_file, args.dry_run, args.limit)
    else:
        configs = [{"keywords": args.keywords, "size": 100}] if args.keywords else SEARCH_CONFIGS
        states = args.states.split(",") if args.states else US_STATES
        run_search(args.api_key, configs, states, args.dry_run, args.limit)
