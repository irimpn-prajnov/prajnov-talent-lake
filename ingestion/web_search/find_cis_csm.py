"""
CIS-CSM LinkedIn Discovery
Finds LinkedIn profiles of ServiceNow CIS-CSM certified professionals in the US.

Two modes:
  1. cse    — Google Custom Search API (needs GOOGLE_SEARCH_API_KEY + GOOGLE_SEARCH_CX enabled)
  2. scrape — Direct Google search HTML scraping (no key needed, rate limited by Google)

Output: prints results to console + writes to cis_csm_results.json

Usage:
  python find_cis_csm.py --mode scrape
  python find_cis_csm.py --mode cse --api-key xxx --cx yyy
  python find_cis_csm.py --mode scrape --limit 50
"""
import argparse
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

QUERIES = [
    'site:linkedin.com/in "CIS-CSM" "ServiceNow" "United States"',
    'site:linkedin.com/in "CIS-CSM" "ServiceNow" certified',
    'site:linkedin.com/in "Certified Implementation Specialist" "Customer Service Management" "United States"',
    'site:linkedin.com/in "ServiceNow CIS-CSM" certified professional',
    'site:linkedin.com/in "CIS-CSM" "ServiceNow" developer',
    'site:linkedin.com/in "CIS-CSM" "ServiceNow" architect',
    'site:linkedin.com/in "CIS-CSM" "ServiceNow" consultant',
    'site:linkedin.com/in "CIS-CSM" "ServiceNow" administrator',
]

GOOGLE_CSE_URL = "https://www.googleapis.com/customsearch/v1"
GOOGLE_SEARCH_URL = "https://www.google.com/search"

_TITLE_RE = re.compile(
    r"^(.+?)\s*[-–]\s*(.+?)(?:\s*[-–]\s*(.+?))?\s*\|\s*LinkedIn",
    re.IGNORECASE
)
_LOC_RE = re.compile(
    r"([A-Z][a-zA-Z\s]+,\s*[A-Z][a-zA-Z\s]+(?:,\s*United States)?)"
)


def parse_result(url: str, title_text: str, snippet: str, query: str) -> dict:
    name, headline, company, location = "", "", "", ""

    m = _TITLE_RE.match(title_text or "")
    if m:
        name = m.group(1).strip()
        headline = m.group(2).strip()
        company = (m.group(3) or "").strip()

    loc_m = _LOC_RE.search(snippet or "")
    if loc_m:
        location = loc_m.group(1).strip()

    return {
        "linkedin_url": url,
        "name": name,
        "headline": headline,
        "company": company,
        "location": location,
        "raw_snippet": snippet,
        "matched_query": query,
        "cert": "CIS-CSM",
        "discovered_at": datetime.now(timezone.utc).isoformat(),
    }


def search_cse(query: str, api_key: str, cx: str) -> list:
    params = {"key": api_key, "cx": cx, "q": query, "num": 10}
    resp = requests.get(GOOGLE_CSE_URL, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise Exception(data["error"].get("message", "CSE error"))
    return data.get("items", [])


def search_scrape(query: str) -> list:
    """Scrape Google search results HTML directly."""
    params = {"q": query, "num": 10, "hl": "en", "gl": "us"}
    resp = requests.get(GOOGLE_SEARCH_URL, params=params, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    html = resp.text

    results = []
    # Extract result blocks — each organic result has a specific pattern
    blocks = re.findall(
        r'<div[^>]+class="[^"]*tF2Cxc[^"]*"[^>]*>(.*?)</div>\s*</div>\s*</div>',
        html, re.DOTALL
    )
    if not blocks:
        # Fallback: extract all linkedin.com/in URLs + surrounding text
        matches = re.finditer(
            r'href="(https://(?:www\.)?linkedin\.com/in/[^"&]+)"[^>]*>.*?'
            r'<h3[^>]*>(.*?)</h3>.*?'
            r'<span[^>]*>(.*?)</span>',
            html, re.DOTALL
        )
        for m in matches:
            results.append({
                "link": m.group(1),
                "title": re.sub(r'<[^>]+>', '', m.group(2)).strip(),
                "snippet": re.sub(r'<[^>]+>', '', m.group(3)).strip(),
            })
    else:
        for block in blocks:
            url_m = re.search(r'href="(https://(?:www\.)?linkedin\.com/in/[^"&]+)"', block)
            title_m = re.search(r'<h3[^>]*>(.*?)</h3>', block, re.DOTALL)
            snippet_m = re.search(r'<span[^>]*class="[^"]*aCOpRe[^"]*"[^>]*>(.*?)</span>', block, re.DOTALL)
            if url_m:
                results.append({
                    "link": url_m.group(1),
                    "title": re.sub(r'<[^>]+>', '', title_m.group(1)).strip() if title_m else "",
                    "snippet": re.sub(r'<[^>]+>', '', snippet_m.group(1)).strip() if snippet_m else "",
                })

    return results


def run(mode: str, api_key: str, cx: str, limit: int, output_file: str):
    seen = set()
    results = []

    print(f"\n{'='*60}")
    print(f"  CIS-CSM LinkedIn Discovery")
    print(f"  Mode: {mode} | Queries: {len(QUERIES)}")
    print(f"{'='*60}\n")

    for i, query in enumerate(QUERIES):
        if limit and len(results) >= limit:
            break

        print(f"[{i+1}/{len(QUERIES)}] {query}")

        try:
            if mode == "cse":
                items = search_cse(query, api_key, cx)
            else:
                items = search_scrape(query)
                time.sleep(3)  # be polite to Google
        except Exception as e:
            print(f"  ERROR: {e}")
            continue

        new_this_query = 0
        for item in items:
            url = item.get("link", "")
            if not url or "linkedin.com/in/" not in url:
                continue
            # Normalize URL — strip query params
            url = url.split("?")[0].rstrip("/")
            if url in seen:
                continue
            seen.add(url)

            record = parse_result(
                url,
                item.get("title", ""),
                item.get("snippet", ""),
                query
            )
            results.append(record)
            new_this_query += 1

            print(f"  ✓ {record['name'] or 'Unknown'} | {record['headline'][:60] if record['headline'] else 'N/A'}")
            if record["company"]:
                print(f"    Company: {record['company']}")
            if record["location"]:
                print(f"    Location: {record['location']}")
            print(f"    URL: {url}")
            print()

        print(f"  → {new_this_query} new profiles this query | total so far: {len(results)}\n")

        if mode == "cse":
            time.sleep(0.5)

    # Write output
    output = {
        "cert": "CIS-CSM",
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "mode": mode,
        "total_found": len(results),
        "profiles": results,
    }
    with open(output_file, "w") as f:
        json.dump(output, f, indent=2)

    print(f"{'='*60}")
    print(f"  DONE — {len(results)} unique CIS-CSM profiles found")
    print(f"  Saved to: {output_file}")
    print(f"{'='*60}\n")

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["cse", "scrape"], default="scrape")
    parser.add_argument("--api-key", default=os.getenv("GOOGLE_SEARCH_API_KEY", ""))
    parser.add_argument("--cx", default=os.getenv("GOOGLE_SEARCH_CX", ""))
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--output", default="cis_csm_results.json")
    args = parser.parse_args()

    if args.mode == "cse" and (not args.api_key or not args.cx):
        print("ERROR: --api-key and --cx required for cse mode")
        print("Tip: run with --mode scrape to use direct Google search instead")
        raise SystemExit(1)

    run(args.mode, args.api_key, args.cx, args.limit, args.output)
