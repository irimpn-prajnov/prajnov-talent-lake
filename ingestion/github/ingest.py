"""
Pipeline: GitHub
Source:   GitHub REST API
Output:   s3://prajnov-talent-lake/bronze/github/users/YYYY/MM/DD/{uuid}.json

Extracts: profile (name, bio, location, company) + repos (languages, topics, stars)

Usage:
  python ingest.py --token ghp_xxx
  python ingest.py --dry-run --limit 50 --query "ServiceNow developer"
"""
import argparse
import os
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import bronze_path, hash_record, make_meta, s3_put, write_manifest, SeenSet

GITHUB_API = "https://api.github.com"

SEARCH_QUERIES = [
    "ServiceNow developer location:US",
    "AWS architect location:US",
    "Azure engineer location:US",
    "Salesforce developer location:US",
    "SAP consultant location:US",
    "data engineer location:US",
    "DevOps engineer location:US",
    "machine learning engineer location:US",
]


def gh_get(path: str, token: str, params: dict = None) -> dict:
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    resp = requests.get(f"{GITHUB_API}{path}", headers=headers, params=params, timeout=15)
    # Respect rate limit
    if resp.status_code == 403 and "rate limit" in resp.text.lower():
        reset = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
        wait = max(reset - time.time(), 1)
        print(f"  Rate limited — sleeping {wait:.0f}s")
        time.sleep(wait)
        return gh_get(path, token, params)
    resp.raise_for_status()
    return resp.json()


def fetch_user_with_repos(username: str, token: str) -> dict:
    profile = gh_get(f"/users/{username}", token)
    repos_raw = gh_get(f"/users/{username}/repos", token, params={"per_page": 30, "sort": "pushed"})

    repos = []
    for r in repos_raw[:10]:  # top 10 most recently pushed
        langs = {}
        try:
            langs = gh_get(f"/repos/{username}/{r['name']}/languages", token)
        except Exception:
            pass
        repos.append({
            "name": r["name"],
            "description": r.get("description", ""),
            "languages": langs,
            "topics": r.get("topics", []),
            "stars": r.get("stargazers_count", 0),
            "forks": r.get("forks_count", 0),
            "last_pushed": r.get("pushed_at", ""),
        })
        time.sleep(0.2)

    return {
        "login": profile.get("login"),
        "name": profile.get("name", ""),
        "bio": profile.get("bio", ""),
        "location": profile.get("location", ""),
        "company": profile.get("company", ""),
        "blog": profile.get("blog", ""),
        "public_repos": profile.get("public_repos", 0),
        "followers": profile.get("followers", 0),
        "profile_url": profile.get("html_url", ""),
        "repos": repos,
    }


def run(token: str, queries: list, dry_run: bool, limit: int):
    seen = SeenSet()
    stats = {"written": 0, "skipped_dup": 0, "errors": 0}
    total = 0

    for query in queries:
        page = 1
        while True:
            try:
                result = gh_get("/search/users", token, params={
                    "q": query, "per_page": 30, "page": page
                })
                users = result.get("items", [])
                if not users:
                    break
                time.sleep(1)
            except Exception as e:
                print(f"[ERROR] query='{query}' page={page}: {e}")
                stats["errors"] += 1
                break

            for user in (users[:10] if dry_run else users):
                username = user.get("login", "")
                if username in seen:
                    stats["skipped_dup"] += 1
                    continue
                seen.add(username)

                try:
                    data = fetch_user_with_repos(username, token)
                    meta = make_meta(
                        source="github",
                        source_url=data["profile_url"],
                        extra={"raw_hash": hash_record(data), "record_id": username, "search_query": query},
                    )
                    record = {"_meta": meta, **data}
                    key = bronze_path("github", "users")
                    s3_put(key, record, dry_run=dry_run)
                    stats["written"] += 1
                    total += 1
                    if not dry_run:
                        time.sleep(0.5)
                except Exception as e:
                    stats["errors"] += 1
                    print(f"[ERROR] user={username}: {e}")

                if limit and total >= limit:
                    write_manifest("github", stats, dry_run=dry_run)
                    print(f"Done (limit reached): {stats}")
                    return

            if dry_run:
                print(f"  [DRY-RUN] stopping after first query page — {len(users[:10])} real results, S3 writes skipped")
                write_manifest("github", stats, dry_run=dry_run)
                return

            # GitHub search caps at 1000 results (100 pages × 10, or ~33 pages × 30)
            page += 1
            if page > 33 or len(users) < 30:
                break

    write_manifest("github", stats, dry_run=dry_run)
    print(f"Done: {stats}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--token", default=os.getenv("GITHUB_TOKEN"))
    parser.add_argument("--queries", default=None,
                        help="Comma-separated search queries (overrides defaults)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    if not args.token:
        print("ERROR: GITHUB_TOKEN required")
        sys.exit(1)

    queries = args.queries.split(",") if args.queries else SEARCH_QUERIES
    run(token=args.token, queries=queries, dry_run=args.dry_run, limit=args.limit)
