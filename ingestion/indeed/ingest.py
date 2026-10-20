"""
Pipeline: Indeed (Partner API)
Source:   Indeed Candidate Retrieval API
Output:   s3://prajnov-talent-lake/bronze/indeed/applications/YYYY/MM/DD/{uuid}.json

STATUS: Stub — requires Indeed Partner API approval.

To get access:
  1. Register at https://developer.indeed.com
  2. Apply for "Candidate Retrieval API" (employer/partner agreement required)
  3. Set env vars: INDEED_CLIENT_ID, INDEED_CLIENT_SECRET, INDEED_EMPLOYER_ID
  4. Uncomment and implement the API calls below

Usage (once approved):
  python ingest.py --dry-run --limit 50
"""
import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import bronze_path, hash_record, make_meta, s3_put, write_manifest

INDEED_TOKEN_URL = "https://apis.indeed.com/oauth/v2/tokens"
INDEED_CANDIDATES_URL = "https://apis.indeed.com/v1/employers/{employer_id}/candidates"


def get_access_token(client_id: str, client_secret: str) -> str:
    """OAuth2 client credentials flow."""
    import requests
    resp = requests.post(INDEED_TOKEN_URL, data={
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "employer_access",
    }, timeout=15)
    resp.raise_for_status()
    return resp.json()["access_token"]


def fetch_candidates(token: str, employer_id: str, page_token: str = None) -> dict:
    """Fetch candidates from Indeed Candidate Retrieval API."""
    import requests
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    params = {"limit": 100}
    if page_token:
        params["pageToken"] = page_token
    url = INDEED_CANDIDATES_URL.format(employer_id=employer_id)
    resp = requests.get(url, headers=headers, params=params, timeout=20)
    resp.raise_for_status()
    return resp.json()


def build_record(candidate: dict) -> tuple:
    cid = candidate.get("candidateId", "")
    data = {
        "indeed_candidate_id": cid,
        "name": candidate.get("name", ""),
        "email": candidate.get("email", ""),
        "phone": candidate.get("phone", ""),
        "location": candidate.get("location", ""),
        "resume_text": candidate.get("resumeText", ""),
        "skills": candidate.get("skills", []),
        "experience_years": candidate.get("experienceYears"),
        "applied_jobs": candidate.get("appliedJobs", []),
        "last_active": candidate.get("lastActive", ""),
    }
    meta = make_meta(
        source="indeed",
        retention_class="pii",
        extra={"raw_hash": hash_record(data), "record_id": cid},
    )
    return {"_meta": meta, **data}, cid


def run(client_id: str, client_secret: str, employer_id: str, dry_run: bool, limit: int):
    token = get_access_token(client_id, client_secret)
    stats = {"written": 0, "errors": 0}
    total = 0
    page_token = None

    while True:
        try:
            result = fetch_candidates(token, employer_id, page_token)
            candidates = result.get("candidates") or []
            if not candidates:
                break
        except Exception as e:
            print(f"[ERROR] fetch candidates: {e}")
            stats["errors"] += 1
            break

        for c in candidates:
            try:
                record, cid = build_record(c)
                key = bronze_path("indeed", "applications")
                s3_put(key, record, dry_run=dry_run)
                stats["written"] += 1
                total += 1
            except Exception as e:
                stats["errors"] += 1
                print(f"[ERROR] candidate: {e}")

            if limit and total >= limit:
                write_manifest("indeed", stats, dry_run=dry_run)
                print(f"Done (limit reached): {stats}")
                return

        page_token = result.get("nextPageToken")
        if not page_token:
            break

    write_manifest("indeed", stats, dry_run=dry_run)
    print(f"Done: {stats}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--client-id", default=os.getenv("INDEED_CLIENT_ID"))
    parser.add_argument("--client-secret", default=os.getenv("INDEED_CLIENT_SECRET"))
    parser.add_argument("--employer-id", default=os.getenv("INDEED_EMPLOYER_ID"))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    if not all([args.client_id, args.client_secret, args.employer_id]):
        print("ERROR: INDEED_CLIENT_ID, INDEED_CLIENT_SECRET, INDEED_EMPLOYER_ID required")
        print("Apply for access at https://developer.indeed.com")
        sys.exit(1)

    run(args.client_id, args.client_secret, args.employer_id, args.dry_run, args.limit)
