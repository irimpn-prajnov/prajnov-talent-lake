"""
Pipeline: Dice Candidate Discovery
Source:   Google CSE targeting dice.com + dice.com public resume pages
Output:   s3://prajnov-talent-lake/bronze/dice/profiles/YYYY/MM/DD/{uuid}.json

NOTE: dice.com/profiles requires login — candidate profiles are not publicly accessible.
Strategy: Use Google CSE (site:dice.com) to discover public-facing candidate resume
pages and extract name, title, skills, location from snippet + page metadata.

Two modes:
  1. google_cse  — Google CSE site:dice.com search (recommended, no login needed)
  2. resume_page — Fetch individual dice.com/resume/{id} pages (public, no login)

Usage:
  python ingest.py --mode google_cse --api-key xxx --cx yyy
  python ingest.py --mode google_cse --dry-run --limit 50
  python ingest.py --mode resume_page --input-file dice_urls.txt
"""
import argparse
import os
import re
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import bronze_path, hash_record, make_meta, s3_put, write_manifest, SeenSet

GOOGLE_CSE_URL = "https://www.googleapis.com/customsearch/v1"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}

TITLE_GROUPS = {
    "servicenow": ["ServiceNow Developer", "ServiceNow Architect", "ServiceNow Admin"],
    "aws":        ["AWS Solutions Architect", "AWS Cloud Engineer", "AWS DevOps Engineer"],
    "azure":      ["Azure Solutions Architect", "Azure DevOps Engineer"],
    "data":       ["Data Engineer", "Data Scientist", "ML Engineer"],
    "fullstack":  ["Full Stack Developer", "React Developer", "Python Developer"],
    "devops":     ["DevOps Engineer", "Platform Engineer", "SRE"],
    "salesforce": ["Salesforce Developer", "Salesforce Architect"],
    "sap":        ["SAP Consultant", "SAP ABAP Developer"],
}

# Dice resume/profile URL patterns that are publicly accessible
_DICE_PROFILE_RE = re.compile(
    r"https?://(?:www\.)?dice\.com/(?:resume|profile|candidate)/[\w\-]+"
)
# "Name - Title - Location | Dice" or "Name | Title | Dice"
_TITLE_RE = re.compile(
    r"^(.+?)\s*[-–|]\s*(.+?)(?:\s*[-–|]\s*(.+?))?\s*\|\s*Dice", re.IGNORECASE
)


def parse_google_item(item: dict, title_group: str, search_title: str) -> dict:
    url = item.get("link", "")
    title_text = item.get("title", "")
    snippet = item.get("snippet", "")

    name, headline, location = "", "", ""
    m = _TITLE_RE.match(title_text)
    if m:
        name = m.group(1).strip()
        headline = m.group(2).strip()
        location = (m.group(3) or "").strip()

    # Fallback: location from snippet
    if not location:
        loc_m = re.search(
            r"([A-Z][a-zA-Z\s]+,\s*[A-Z]{2}(?:,\s*United States)?)", snippet
        )
        if loc_m:
            location = loc_m.group(1).strip()

    # Skills often listed in snippet as comma-separated after a dash
    skills = []
    skill_m = re.search(r"(?:skills?|expertise)[:\s]+([^.]+)", snippet, re.IGNORECASE)
    if skill_m:
        skills = [s.strip() for s in skill_m.group(1).split(",") if s.strip()]

    return {
        "dice_url": url,
        "name": name,
        "headline": headline,
        "location": location,
        "skills_snippet": skills,
        "raw_snippet": snippet,
        "title_group": title_group,
        "search_title": search_title,
        "discovery_method": "google_cse",
    }


def google_search(query: str, api_key: str, cx: str, start: int = 1) -> list:
    params = {"key": api_key, "cx": cx, "q": query, "start": start, "num": 10}
    resp = requests.get(GOOGLE_CSE_URL, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json().get("items", [])


def fetch_resume_page(url: str) -> dict:
    """Fetch a public dice.com/resume/{id} page and extract structured data."""
    resp = requests.get(url, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    html = resp.text

    # Extract JSON-LD if present
    jsonld_m = re.search(r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>',
                         html, re.DOTALL)
    structured = {}
    if jsonld_m:
        try:
            import json
            structured = json.loads(jsonld_m.group(1))
        except Exception:
            pass

    # Fallback meta tags
    name = ""
    name_m = re.search(r'<meta[^>]+property="og:title"[^>]+content="([^"]+)"', html)
    if name_m:
        name = name_m.group(1).strip()

    description = ""
    desc_m = re.search(r'<meta[^>]+name="description"[^>]+content="([^"]+)"', html)
    if desc_m:
        description = desc_m.group(1).strip()

    return {
        "dice_url": url,
        "name": structured.get("name", name),
        "description": description,
        "structured_data": structured,
        "discovery_method": "resume_page",
    }


def run_google_cse(api_key: str, cx: str, groups: list, dry_run: bool, limit: int):
    seen = SeenSet()
    stats = {"written": 0, "skipped_dup": 0, "errors": 0, "queries": 0}
    total = 0

    title_list = [(g, t) for g in groups for t in TITLE_GROUPS.get(g, [])]

    for title_group, title in title_list:
        # Target dice.com resume/profile pages specifically
        query = f'site:dice.com/resume OR site:dice.com/profile "{title}" "United States"'

        try:
            items = google_search(query, api_key, cx)
            stats["queries"] += 1
            if not dry_run:
                time.sleep(0.5)
        except Exception as e:
            print(f"[ERROR] query '{query}': {e}")
            stats["errors"] += 1
            continue

        for item in items[:10] if dry_run else items:
            url = item.get("link", "")
            if not _DICE_PROFILE_RE.match(url):
                continue
            if url in seen:
                stats["skipped_dup"] += 1
                continue
            seen.add(url)

            data = parse_google_item(item, title_group, title)
            meta = make_meta(
                source="dice",
                retention_class="pii",
                source_url=url,
                extra={"raw_hash": hash_record(data)},
            )
            record = {"_meta": meta, **data}
            key = bronze_path("dice", "profiles")
            s3_put(key, record, dry_run=dry_run)
            stats["written"] += 1
            total += 1

            if limit and total >= limit:
                write_manifest("dice", stats, dry_run=dry_run)
                print(f"Done (limit reached): {stats}")
                return

    write_manifest("dice", stats, dry_run=dry_run)
    print(f"Done: {stats}")


def run_resume_page(input_file: str, dry_run: bool, limit: int):
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
            data = fetch_resume_page(url)
            meta = make_meta(
                source="dice",
                retention_class="pii",
                source_url=url,
                extra={"raw_hash": hash_record(data)},
            )
            record = {"_meta": meta, **data}
            key = bronze_path("dice", "profiles")
            s3_put(key, record, dry_run=dry_run)
            stats["written"] += 1
            time.sleep(1)
        except Exception as e:
            stats["errors"] += 1
            print(f"[ERROR] {url}: {e}")

    write_manifest("dice", stats, dry_run=dry_run)
    print(f"Done: {stats}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["google_cse", "resume_page"], default="google_cse")
    parser.add_argument("--api-key", default=os.getenv("GOOGLE_SEARCH_API_KEY"))
    parser.add_argument("--cx", default=os.getenv("GOOGLE_SEARCH_CX"))
    parser.add_argument("--input-file", help="File with Dice profile URLs (resume_page mode)")
    parser.add_argument("--groups", default=",".join(TITLE_GROUPS.keys()))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    if args.mode == "google_cse":
        if not args.api_key or not args.cx:
            print("ERROR: GOOGLE_SEARCH_API_KEY and GOOGLE_SEARCH_CX required")
            sys.exit(1)
        run_google_cse(args.api_key, args.cx, args.groups.split(","), args.dry_run, args.limit)
    else:
        if not args.input_file:
            print("ERROR: --input-file required for resume_page mode")
            sys.exit(1)
        run_resume_page(args.input_file, args.dry_run, args.limit)
