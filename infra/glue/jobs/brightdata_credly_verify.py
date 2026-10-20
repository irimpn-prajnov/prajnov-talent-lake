"""
Job: brightdata_credly_verify
For each BrightData candidate with self-reported SN certs,
search Credly directory by name → if found, upgrade to credly_verified
and update verified_certifications in candidates table.

Output:
  - bronze/discovery/credly/brightdata_verify/YYYY/MM/DD/batch_NNNN.jsonl
  - candidates.verified_certifications updated in DB

Usage:
  python3 -u brightdata_credly_verify.py
  python3 -u brightdata_credly_verify.py --limit 500 --us-only
"""
import argparse, json, os, sys, time, uuid
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "ingestion"))
from common import parquet_read, get_db, get_s3, BRONZE_BUCKET, now_iso, date_path

CREDLY_BASE   = "https://www.credly.com"
CREDLY_ORG_ID = "687e2b74-213e-42f1-9f9c-71bd4fe8bea4"
CREDLY_DIR    = "https://www.credly.com/api/v1/directory"
HEADERS       = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "https://www.credly.com/organizations/servicenow-new/directory",
}
SN_CERTS = {
    "csa","cad","cta","cma","cis-itsm","cis-hrsd","cis-csm","cis-fsm",
    "cis-sam","cis-ham","cis-itam","cis-spm","cis-ppm","cis-rc","cis-sir",
    "cis-vr","cis-disco","cis-apm","cis-em","cis-sm",
}
BATCH_SIZE = 500


def norm_name(name: str) -> str:
    return " ".join(name.lower().strip().split()) if name else ""


def credly_search_by_name(name: str) -> list:
    """Search Credly directory by user_name, return list of earner dicts."""
    try:
        resp = requests.get(
            CREDLY_DIR,
            params={"organization_id": CREDLY_ORG_ID, "filter[user_name]": name, "page_size": 5},
            headers=HEADERS, timeout=15,
        )
        if resp.status_code == 429:
            time.sleep(5)
            return []
        resp.raise_for_status()
        return resp.json().get("data", [])
    except Exception:
        return []


def best_match(name: str, earners: list) -> dict:
    """Return best matching earner by normalized name."""
    target = norm_name(name)
    for e in earners:
        full = norm_name(f"{e.get('first_name','')} {e.get('last_name','')}")
        if full == target:
            return e
    return {}


def fetch_all_badges(username: str) -> list:
    """Fetch full badge list from user profile."""
    try:
        resp = requests.get(
            f"{CREDLY_BASE}/users/{username}/badges.json",
            headers=HEADERS, timeout=15,
        )
        resp.raise_for_status()
        return resp.json().get("data", [])
    except Exception:
        return []


def extract_badges(earner: dict) -> list:
    return [
        {
            "name": b.get("name", ""),
            "date": b.get("date", ""),
            "url":  f"{CREDLY_BASE}/badges/{b.get('id','')}",
        }
        for b in (earner.get("highlighted_badges") or [])
    ]


def load_already_verified() -> set:
    """Return set of linkedin_ids already credly_verified in candidates table."""
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT linkedin_id FROM candidates
                WHERE cert_verification_sources @> '"credly_verified"'
                AND linkedin_id IS NOT NULL AND linkedin_id != ''
            """)
            result = {r["linkedin_id"] for r in cur.fetchall()}
        conn.close()
        return result
    except Exception as e:
        print(f"  [WARN] load_already_verified: {e}")
        return set()


def flush_batch(batch: list, batch_num: int, dp: str):
    if not batch:
        return
    key  = f"bronze/discovery/credly/brightdata_verify/{dp}/batch_{batch_num:04d}.jsonl"
    body = "\n".join(json.dumps(r, default=str) for r in batch).encode()
    get_s3().put_object(Bucket=BRONZE_BUCKET, Key=key, Body=body, ContentType="application/x-ndjson")


def run(limit=None, us_only=False):
    dp       = date_path()
    run_id   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    print("Loading BrightData silver...")
    bd_rows = parquet_read("silver/candidate/source=brightdata/")
    print(f"  Total: {len(bd_rows)}")

    # Filter to US only if requested — no cert pre-filter
    targets = []
    for r in bd_rows:
        if us_only and (r.get("location_country") or "").upper() != "US":
            continue
        targets.append(r)

    print("Loading already verified set...")
    already_done = load_already_verified()
    targets = [r for r in targets if r.get("linkedin_id", "") not in already_done]
    print(f"  Candidates to search: {len(targets)}{' (US only)' if us_only else ' (all)'}")

    if limit:
        targets = targets[:limit]
    print(f"  Running: {len(targets)}\n")

    conn    = get_db()
    stats   = {"found": 0, "not_found": 0, "updated": 0, "errors": 0}
    batch   = []
    batch_n = 0
    db_buf  = []  # buffer (linkedin_id, cert_codes) for batch DB update

    for i, r in enumerate(targets):
        name        = r.get("canonical_name", "")
        linkedin_id = r.get("linkedin_id", "")
        if not name:
            continue

        earners = credly_search_by_name(name)
        match   = best_match(name, earners)
        time.sleep(0.4)  # be polite to Credly

        if not match:
            stats["not_found"] += 1
            if (i + 1) % 100 == 0:
                print(f"  [{i+1}/{len(targets)}] not found: {name}")
            continue

        stats["found"] += 1
        username = match.get("vanity_url", "").replace("users/", "").lstrip("/")

        # Fetch full badge list from profile
        all_badge_data = fetch_all_badges(username)
        time.sleep(0.3)
        badges = [
            {
                "name":  b.get("badge_template", {}).get("name", ""),
                "date":  (b.get("issued_at") or "")[:10],
                "url":   f"{CREDLY_BASE}/badges/{b.get('id','')}",
            }
            for b in all_badge_data
        ]
        badge_names = [b["name"] for b in badges if b["name"]]

        # Normalize cert codes from badge names
        from common import normalize_cert
        cert_codes = list({normalize_cert(b) for b in badge_names if normalize_cert(b)})

        print(f"  [{i+1}/{len(targets)}] ✓ {name} | {username} | {cert_codes}")

        # Bronze record
        batch.append({
            "credly_username":  username,
            "credly_url":       f"{CREDLY_BASE}/users/{username}",
            "name":             name,
            "linkedin_id":      linkedin_id,
            "badges":           badges,
            "cert_codes":       cert_codes,
            "source":           "brightdata_credly_verify",
            "verified_at":      now_iso(),
        })

        if linkedin_id:
            db_buf.append((cert_codes, linkedin_id))

        # Flush bronze batch
        if len(batch) >= BATCH_SIZE:
            flush_batch(batch, batch_n, dp)
            batch_n += 1
            batch = []

        # Flush DB batch
        if len(db_buf) >= 100:
            try:
                with conn.cursor() as cur:
                    for cert_codes_db, lid in db_buf:
                        cur.execute("""
                            UPDATE candidates SET
                                verified_certifications  = %s,
                                cert_verification_status = 'credly_verified',
                                cert_verification_sources = '["credly_verified"]'::jsonb,
                                cert_verified_at         = now(),
                                updated_at               = now()
                            WHERE linkedin_id = %s
                        """, (cert_codes_db, lid))
                        stats["updated"] += cur.rowcount
                conn.commit()
                db_buf = []
            except Exception as e:
                conn.rollback()
                db_buf = []
                print(f"  [ERROR] DB batch: {e}")
                stats["errors"] += 1

    # Final flushes
    flush_batch(batch, batch_n, dp)

    if db_buf:
        try:
            with conn.cursor() as cur:
                for cert_codes_db, lid in db_buf:
                    cur.execute("""
                        UPDATE candidates SET
                            verified_certifications  = %s,
                            cert_verification_status = 'credly_verified',
                            cert_verification_sources = '["credly_verified"]'::jsonb,
                            cert_verified_at         = now(),
                            updated_at               = now()
                        WHERE linkedin_id = %s
                    """, (cert_codes_db, lid))
                    stats["updated"] += cur.rowcount
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"  [ERROR] final DB flush: {e}")

    conn.close()
    print(f"\nDone.")
    print(f"  Found on Credly : {stats['found']}")
    print(f"  Not found       : {stats['not_found']}")
    print(f"  DB rows updated : {stats['updated']}")
    print(f"  Errors          : {stats['errors']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit",   type=int, default=0)
    parser.add_argument("--us-only", action="store_true")
    args, _ = parser.parse_known_args()
    run(args.limit or None, args.us_only)
