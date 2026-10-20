"""
Pipeline: Credly Certifications
Source:   Credly public API + partner API
Output:   s3://prajnov-talent-lake/bronze/certifications/credly/YYYY/MM/DD/{uuid}.json

Three modes:

  1. catalog  — fetch all badge templates per org (AWS, Microsoft, ServiceNow...)
                REQUIRES Credly partner API key (apply at https://info.credly.com/contact-us)

  2. verify   — fetch a single public badge by URL (no key needed)
                Use when a candidate shares their badge URL

  3. profile  — fetch all public badges for a Credly username (no key needed)
                Use when a candidate shares their Credly profile URL

Usage:
  python ingest.py --mode catalog --api-key $CREDLY_API_KEY
  python ingest.py --mode verify --badge-url https://www.credly.com/badges/abc-123
  python ingest.py --mode profile --username john-smith-abc123
  python ingest.py --mode profile --input-file credly_usernames.txt
  python ingest.py --mode catalog --dry-run --limit 50
"""
import argparse
import os
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import bronze_path, hash_record, make_meta, s3_put, write_manifest, SeenSet

CREDLY_API = "https://www.credly.com/api/v1"
CREDLY_PUBLIC = "https://www.credly.com"

CERT_ORGS = {
    "aws":          "amazon-web-services",
    "microsoft":    "microsoft-certification",
    "servicenow":   "servicenow",
    "google_cloud": "google-cloud",
    "databricks":   "databricks",
    "snowflake":    "snowflake",
    "hashicorp":    "hashicorp",
    "cncf":         "the-linux-foundation",
    "salesforce":   "salesforce",
}


def _get(url: str, api_key: str = None, params: dict = None) -> dict:
    headers = {"Accept": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    resp = requests.get(url, headers=headers, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def _extract_skills(raw_skills: list) -> list:
    return [s.get("name", s) if isinstance(s, dict) else s for s in (raw_skills or [])]


# ── Mode: catalog ─────────────────────────────────────────────────────────────

def run_catalog(api_key: str, orgs: list, dry_run: bool, limit: int):
    if not api_key:
        print("ERROR: --api-key required for catalog mode.")
        print("Apply for Credly partner API access at https://info.credly.com/contact-us")
        sys.exit(1)

    seen = SeenSet()
    stats = {"written": 0, "skipped_dup": 0, "errors": 0}
    total = 0

    for org_key in orgs:
        org_slug = CERT_ORGS.get(org_key)
        if not org_slug:
            print(f"[WARN] unknown org key: {org_key}")
            continue

        page = 1
        while True:
            try:
                result = _get(
                    f"{CREDLY_API}/organizations/{org_slug}/badge_templates",
                    api_key=api_key,
                    params={"page": page, "page_size": 50, "filter": "state::active"},
                )
                badges = result.get("data") or []
                if not badges:
                    break
                time.sleep(0.5)
            except Exception as e:
                print(f"[ERROR] org={org_key} page={page}: {e}")
                stats["errors"] += 1
                break

            for badge in badges:
                bid = badge.get("id", "")
                if bid in seen:
                    stats["skipped_dup"] += 1
                    continue
                seen.add(bid)

                data = {
                    "badge_template_id": bid,
                    "badge_name": badge.get("name", ""),
                    "org_key": org_key,
                    "issuer": badge.get("issuer", {}).get("name", org_key)
                              if isinstance(badge.get("issuer"), dict) else org_key,
                    "description": badge.get("description", ""),
                    "skills": _extract_skills(badge.get("skills", [])),
                    "level": badge.get("level", ""),
                    "template_url": f"{CREDLY_PUBLIC}/org/{org_slug}/badge/{bid}",
                    "mode": "catalog",
                }
                meta = make_meta(
                    source="credly",
                    source_url=data["template_url"],
                    extra={"raw_hash": hash_record(data), "record_id": bid},
                )
                s3_put(bronze_path("certifications", "credly"), {"_meta": meta, **data}, dry_run)
                stats["written"] += 1
                total += 1

                if limit and total >= limit:
                    write_manifest("credly", stats, dry_run)
                    print(f"Done (limit reached): {stats}")
                    return

            total_pages = (result.get("metadata") or {}).get("total_pages", 1)
            page += 1
            if page > total_pages:
                break

    write_manifest("credly", stats, dry_run)
    print(f"Done: {stats}")


# ── Mode: verify (single badge URL, no key) ───────────────────────────────────

def _build_badge_record(badge: dict, badge_url: str, mode: str) -> dict:
    """Build a bronze record from a badge object returned by the profile endpoint."""
    badge_id = badge.get("id", badge_url.rstrip("/").split("/")[-1])
    template = badge.get("badge_template") or {}
    issuer = badge.get("issuer") or template.get("issuer") or {}
    data = {
        "badge_id": badge_id,
        "badge_url": badge_url,
        "badge_name": template.get("name", ""),
        "issuer": issuer.get("name", "") if isinstance(issuer, dict) else str(issuer),
        "earner_name": badge.get("issued_to", ""),
        "issued_at": badge.get("issued_at", ""),
        "expires_at": badge.get("expires_at", ""),
        "is_expired": badge.get("state") == "expired",
        "skills": _extract_skills(template.get("skills", [])),
        "verification_url": badge_url,
        "mode": mode,
    }
    meta = make_meta(
        source="credly",
        retention_class="pii",
        source_url=badge_url,
        extra={"raw_hash": hash_record(data), "record_id": badge_id},
    )
    return {"_meta": meta, **data}


def fetch_and_store_badge(badge_url: str, dry_run: bool, username: str = "") -> bool:
    """Fetch a single public badge by badge URL + earner username.
    Requires username because Credly's badge page doesn't expose a slug."""
    badge_id = badge_url.rstrip("/").split("/")[-1]
    if not username:
        print(f"[ERROR] badge {badge_id}: --username required for verify mode (Credly removed the public badge JSON endpoint)")
        return False
    try:
        result = _get(f"{CREDLY_PUBLIC}/users/{username}/badges.json",
                      params={"page": 1, "page_size": 48})
        badges = result.get("data") or []
        for badge in badges:
            if badge.get("id") == badge_id:
                record = _build_badge_record(badge, badge_url, "verify")
                s3_put(bronze_path("certifications", "credly"), record, dry_run)
                print(f"  verified badge {badge_id}: {record['badge_name']}")
                return True
        print(f"[WARN] badge {badge_id} not found in {username}'s profile")
        return False
    except Exception as e:
        print(f"[ERROR] badge {badge_id}: {e}")
        return False


def run_verify(badge_url: str, username: str, dry_run: bool):
    ok = fetch_and_store_badge(badge_url, dry_run, username=username)
    write_manifest("credly_verify", {"written": int(ok), "errors": int(not ok)}, dry_run)


# ── Mode: profile (all badges for a username, no key) ─────────────────────────

def run_profile(usernames: list, dry_run: bool, limit: int):
    stats = {"written": 0, "errors": 0}
    total = 0

    for username in usernames:
        page = 1
        while True:
            try:
                # Public user badges endpoint — no auth required
                result = _get(
                    f"{CREDLY_PUBLIC}/users/{username}/badges.json",
                    params={"page": page, "page_size": 48, "sort": "-state_updated_at"},
                )
                badges = result.get("data") or []
                if not badges:
                    break
                time.sleep(0.3)
            except Exception as e:
                print(f"[ERROR] profile username={username} page={page}: {e}")
                stats["errors"] += 1
                break

            for badge in badges:
                badge_id = badge.get("id", "")
                badge_url = f"{CREDLY_PUBLIC}/badges/{badge_id}"
                try:
                    record = _build_badge_record(badge, badge_url, "profile")
                    s3_put(bronze_path("certifications", "credly"), record, dry_run)
                    stats["written"] += 1
                    total += 1
                except Exception as e:
                    print(f"[ERROR] badge {badge_id}: {e}")
                    stats["errors"] += 1

                if limit and total >= limit:
                    write_manifest("credly_profile", stats, dry_run)
                    print(f"Done (limit reached): {stats}")
                    return

            total_pages = (result.get("metadata") or {}).get("total_pages", 1)
            page += 1
            if page > total_pages:
                break

    write_manifest("credly_profile", stats, dry_run)
    print(f"Done: {stats}")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["catalog", "verify", "profile"], default="verify")
    parser.add_argument("--api-key", default=os.getenv("CREDLY_API_KEY", ""),
                        help="Partner API key (catalog mode only)")
    parser.add_argument("--badge-url", help="Badge URL for verify mode")
    parser.add_argument("--username", help="Credly username for profile mode")
    parser.add_argument("--input-file", help="File with usernames or badge URLs (one per line)")
    parser.add_argument("--orgs", default=",".join(CERT_ORGS.keys()))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    if args.mode == "catalog":
        run_catalog(args.api_key, args.orgs.split(","), args.dry_run, args.limit)

    elif args.mode == "verify":
        if not args.badge_url:
            print("ERROR: --badge-url required for verify mode")
            sys.exit(1)
        if not args.username:
            print("ERROR: --username required for verify mode (Credly removed public badge JSON endpoint)")
            sys.exit(1)
        run_verify(args.badge_url, args.username, args.dry_run)

    elif args.mode == "profile":
        if not args.username and not args.input_file:
            print("ERROR: --username or --input-file required for profile mode")
            sys.exit(1)
        usernames = [args.username] if args.username else \
            [l.strip() for l in open(args.input_file) if l.strip()]
        run_profile(usernames, args.dry_run, args.limit)
