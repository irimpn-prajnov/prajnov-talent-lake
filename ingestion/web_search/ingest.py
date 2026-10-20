"""
Pipeline: Web Search (Google CSE → LinkedIn URL discovery)
Source:   Google Custom Search API
Output:   s3://prajnov-talent-lake/bronze/discovery/web_search/YYYY/MM/DD/{uuid}.json

Extracts: linkedin_url, name, headline, company, location from Google snippet only.
No LinkedIn requests made.

Usage:
  python ingest.py
  python ingest.py --dry-run --limit 50
  python ingest.py --groups aws,azure --location "New York"
"""
import argparse
import re
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import bronze_path, hash_record, make_meta, s3_put, write_manifest, SeenSet

# ── Config ────────────────────────────────────────────────────────────────────

GOOGLE_CSE_URL = "https://www.googleapis.com/customsearch/v1"

# Titles to query per group. Add/remove as needed.
TITLE_GROUPS = {
    "servicenow": [
        "ServiceNow Developer", "ServiceNow Architect", "ServiceNow Admin",
        "ServiceNow CSM", "ServiceNow ITSM", "ServiceNow HRSD", "ServiceNow SPM",
    ],
    "aws": [
        "AWS Solutions Architect", "AWS DevOps Engineer", "AWS Cloud Engineer",
        "AWS Data Engineer", "AWS ML Engineer",
    ],
    "azure": [
        "Azure Solutions Architect", "Azure DevOps Engineer", "Azure Data Engineer",
        "Azure Cloud Engineer",
    ],
    "data": [
        "Data Engineer", "Data Scientist", "ML Engineer", "AI Engineer",
        "Databricks Engineer", "Snowflake Engineer",
    ],
    "fullstack": [
        "Full Stack Developer", "React Developer", "Node.js Developer",
        "Python Developer", "Java Developer",
    ],
    "devops": [
        "DevOps Engineer", "Platform Engineer", "Site Reliability Engineer",
        "Kubernetes Engineer", "Terraform Engineer",
    ],
    "salesforce": [
        "Salesforce Developer", "Salesforce Architect", "Salesforce Admin",
    ],
    "sap": [
        "SAP Consultant", "SAP FICO Consultant", "SAP SD Consultant", "SAP ABAP Developer",
    ],
}

US_LOCATIONS = ["United States", "New York", "Texas", "California", "Illinois"]

# Pattern: "Name - Title - Company | LinkedIn" or "Name - Title | LinkedIn"
_TITLE_RE = re.compile(r"^(.+?)\s*[-–]\s*(.+?)(?:\s*[-–]\s*(.+?))?\s*\|\s*LinkedIn", re.IGNORECASE)


# ── Snippet parser ────────────────────────────────────────────────────────────

def parse_snippet(title_text: str, snippet: str, url: str) -> dict:
    name, headline, company = "", "", ""
    m = _TITLE_RE.match(title_text or "")
    if m:
        name = m.group(1).strip()
        headline = m.group(2).strip()
        company = (m.group(3) or "").strip()

    # Location often appears in snippet as "City, State, United States"
    location = ""
    loc_m = re.search(r"([A-Z][a-zA-Z\s]+,\s*[A-Z][a-zA-Z\s]+,\s*United States)", snippet or "")
    if loc_m:
        location = loc_m.group(1).strip()

    return {
        "linkedin_url": url,
        "name": name,
        "headline": headline,
        "company": company,
        "location_snippet": location,
        "raw_snippet": snippet,
    }


# ── Google CSE call ───────────────────────────────────────────────────────────

def search(query: str, api_key: str, cx: str, start: int = 1) -> list:
    params = {
        "key": api_key,
        "cx": cx,
        "q": query,
        "start": start,
        "num": 10,
    }
    resp = requests.get(GOOGLE_CSE_URL, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json().get("items", [])


# ── Main ──────────────────────────────────────────────────────────────────────

def run(api_key: str, cx: str, groups: list, location: str, dry_run: bool, limit: int):
    seen = SeenSet()
    stats = {"written": 0, "skipped_dup": 0, "errors": 0, "queries": 0}
    total = 0

    title_list = []
    for g in groups:
        title_list.extend(TITLE_GROUPS.get(g, []))

    for title in title_list:
        query = f'site:linkedin.com/in "{title}" "{location}"'

        try:
            items = search(query, api_key, cx)
            stats["queries"] += 1
            if not dry_run:
                time.sleep(0.5)
        except Exception as e:
            print(f"[ERROR] query '{query}': {e}")
            stats["errors"] += 1
            continue

        for item in items[:10] if dry_run else items:
            url = item.get("link", "")
            if not url.startswith("https://www.linkedin.com/in/"):
                continue
            if url in seen:
                stats["skipped_dup"] += 1
                continue
            seen.add(url)

            parsed = parse_snippet(item.get("title", ""), item.get("snippet", ""), url)
            parsed["title_group"] = next(
                (g for g, titles in TITLE_GROUPS.items() if title in titles), ""
            )
            parsed["search_query"] = query
            parsed["search_title"] = title

            meta = make_meta(
                source="web_search",
                source_url=f"https://www.googleapis.com/customsearch/v1?q={query}",
                extra={"raw_hash": hash_record(parsed)},
            )
            record = {"_meta": meta, **parsed}

            key = bronze_path("discovery", "web_search")
            s3_put(key, record, dry_run=dry_run)
            stats["written"] += 1
            total += 1

            if limit and total >= limit:
                write_manifest("web_search", stats, dry_run=dry_run)
                print(f"Done (limit reached): {stats}")
                return

    write_manifest("web_search", stats, dry_run=dry_run)
    print(f"Done: {stats}")


if __name__ == "__main__":
    import os

    parser = argparse.ArgumentParser()
    parser.add_argument("--api-key", default=os.getenv("GOOGLE_SEARCH_API_KEY"))
    parser.add_argument("--cx", default=os.getenv("GOOGLE_SEARCH_CX"))
    parser.add_argument("--groups", default=",".join(TITLE_GROUPS.keys()),
                        help="Comma-separated title groups, e.g. aws,azure,servicenow")
    parser.add_argument("--location", default="United States")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    if not args.api_key or not args.cx:
        print("ERROR: GOOGLE_SEARCH_API_KEY and GOOGLE_SEARCH_CX required")
        sys.exit(1)

    run(
        api_key=args.api_key,
        cx=args.cx,
        groups=args.groups.split(","),
        location=args.location,
        dry_run=args.dry_run,
        limit=args.limit,
    )
