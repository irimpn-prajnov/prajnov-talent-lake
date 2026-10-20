"""
Pipeline: Credly Full Consolidation + Candidate Enrichment

Step 1: For each cert — read ALL S3 files, dedup by credly_id, write one
        clean consolidated file back to S3 (overwrites previous consolidated)
Step 2: Load all consolidated records into credly_staging temp table
Step 3: Match against candidates (BrightData) via 3-tier entity resolution:
          Tier 1 — LinkedIn URL (exact)
          Tier 2 — normalized name + state
          Tier 3 — normalized name only
Step 4: Matched → union Credly cert codes into verified_certifications[], mark credly_verified
Step 5: Unmatched → insert as new candidate rows
Step 6: Ensure GIN index on verified_certifications for multi-cert queries

Usage:
  python credly_enrich_candidates.py
  python credly_enrich_candidates.py --dry-run
  python credly_enrich_candidates.py --skip-consolidate   # if already consolidated
"""
import argparse, json, re, sys, uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion"))
from common import get_db, get_s3, s3_list, BRONZE_BUCKET, now_iso, parse_location, normalize_cert

CREDLY_PREFIX = "bronze/discovery/credly/certs/"
CERTS = ["cad", "cis_ham", "cis_sam", "csa", "cis_csm", "cis_spm"]

SLUG_TO_CODE = {
    "cad":     "cad",
    "cis_ham": "cis-ham",
    "cis_sam": "cis-sam",
    "csa":     "csa",
    "cis_csm": "cis-csm",
    "cis_spm": "cis-spm",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def norm_name(name: str) -> str:
    return re.sub(r"\s+", " ", (name or "").lower().strip())

def linkedin_id_from_url(url: str) -> str:
    m = re.search(r"linkedin\.com/in/([^/?#\s]+)", url or "")
    return m.group(1).rstrip("/").lower() if m else ""


# ── Step 1: Consolidate all S3 files per cert ─────────────────────────────────

def consolidate_cert(s3, slug: str, cert_code: str, dry_run: bool) -> list:
    prefix = f"{CREDLY_PREFIX}{slug}/"
    keys   = s3_list(prefix)
    print(f"  {cert_code}: reading {len(keys)} files...")

    seen    = {}   # credly_id → record (last write wins for same id)
    no_id   = []   # records with no credly_id — keep all, dedup by name

    for key in keys:
        try:
            body = s3.get_object(Bucket=BRONZE_BUCKET, Key=key)["Body"].read()
            data = json.loads(body)
            batch = data if isinstance(data, list) else [data]
            for rec in batch:
                cid = rec.get("credly_id", "").strip()
                if cid:
                    seen[cid] = rec
                else:
                    no_id.append(rec)
        except Exception as e:
            print(f"    [WARN] {key}: {e}")

    # Dedup no_id records by normalized name
    name_seen = set()
    for rec in no_id:
        n = norm_name(rec.get("name", ""))
        if n and n not in name_seen:
            name_seen.add(n)
            seen[f"nocredly_{n}"] = rec

    records = list(seen.values())
    print(f"  {cert_code}: {len(records)} unique records after dedup")

    if dry_run:
        return records

    # Write one clean consolidated file — delete old consolidated if exists, write fresh
    consolidated_key = f"{CREDLY_PREFIX}{slug}/consolidated/all.json"
    body = json.dumps(records, ensure_ascii=False, default=str).encode()
    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=consolidated_key,
        Body=body,
        ContentType="application/json",
    )
    print(f"  {cert_code}: wrote → s3://{BRONZE_BUCKET}/{consolidated_key}")
    return records


def consolidate_all(dry_run: bool) -> dict:
    """Returns {slug: [records]} for all certs."""
    s3      = get_s3()
    result  = {}
    for slug in CERTS:
        cert_code = SLUG_TO_CODE[slug]
        records   = consolidate_cert(s3, slug, cert_code, dry_run)
        result[slug] = records
    return result


# ── Step 2: Build enriched record list ───────────────────────────────────────

def build_staging_records(all_cert_records: dict) -> list:
    """
    Merge records across certs — same credly_id appearing in multiple certs
    gets all cert codes merged into one staging row.
    """
    merged = {}  # credly_id → staging record

    for slug, records in all_cert_records.items():
        cert_code = SLUG_TO_CODE[slug]
        for rec in records:
            cid = rec.get("credly_id", "").strip() or f"nocredly_{norm_name(rec.get('name',''))}"
            if not cid:
                continue

            loc = rec.get("location", "")
            city, state, country = parse_location(loc)

            # Extract all cert codes from badges on this record
            badge_codes = set()
            badge_codes.add(cert_code)
            for badge in (rec.get("badges") or []):
                code = normalize_cert(badge.get("name", ""))
                if code:
                    badge_codes.add(code)

            if cid not in merged:
                merged[cid] = {
                    "credly_id":       rec.get("credly_id", ""),
                    "credly_username": rec.get("credly_username", ""),
                    "credly_url":      rec.get("credly_url", ""),
                    "name":            rec.get("name", ""),
                    "name_norm":       norm_name(rec.get("name", "")),
                    "location_raw":    loc,
                    "city":            city,
                    "state":           state,
                    "country":         country or "US",
                    "linkedin_url":    rec.get("linkedin_url", ""),
                    "linkedin_id":     linkedin_id_from_url(rec.get("linkedin_url", "")),
                    "badge_count":     rec.get("badge_count", 0),
                    "cert_codes":      badge_codes,
                    "discovered_at":   rec.get("discovered_at", now_iso()),
                }
            else:
                # Same person appeared in multiple cert scrapes — union cert codes
                merged[cid]["cert_codes"] |= badge_codes
                # Take higher badge count
                if rec.get("badge_count", 0) > merged[cid]["badge_count"]:
                    merged[cid]["badge_count"] = rec["badge_count"]

    # Finalise cert_codes as sorted list
    staging = []
    for r in merged.values():
        r["cert_codes"] = sorted(r["cert_codes"])
        staging.append(r)

    return staging


# ── Step 3: Stage into temp table ─────────────────────────────────────────────

def create_staging(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TEMP TABLE credly_staging (
                credly_id       TEXT,
                credly_username TEXT,
                credly_url      TEXT,
                name            TEXT,
                name_norm       TEXT,
                location_raw    TEXT,
                city            TEXT,
                state           TEXT,
                country         TEXT,
                linkedin_url    TEXT,
                linkedin_id     TEXT,
                badge_count     INT,
                cert_codes      TEXT[],
                discovered_at   TEXT
            ) ON COMMIT PRESERVE ROWS
        """)
    conn.commit()


def insert_staging(conn, records: list):
    with conn.cursor() as cur:
        for r in records:
            cur.execute("""
                INSERT INTO credly_staging VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                )
            """, (
                r["credly_id"], r["credly_username"], r["credly_url"],
                r["name"], r["name_norm"], r["location_raw"],
                r["city"], r["state"], r["country"],
                r["linkedin_url"], r["linkedin_id"],
                r["badge_count"], r["cert_codes"], r["discovered_at"],
            ))
    conn.commit()
    print(f"  Staged {len(records)} records")


# ── Step 4: Match + Enrich ────────────────────────────────────────────────────

def enrich_matched(conn, query: str, tier: str, dry_run: bool) -> set:
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()

    if not rows:
        print(f"  [{tier}] matched=0")
        return set()

    matched_usernames = set()
    updated = 0

    with conn.cursor() as cur:
        for row in rows:
            username   = row["credly_username"]
            cert_codes = row["cert_codes"]
            uid        = row["uid"]
            matched_usernames.add(username)

            if dry_run:
                updated += 1
                continue

            cur.execute("""
                UPDATE candidates SET
                    verified_certifications = (
                        SELECT array_agg(DISTINCT v ORDER BY v)
                        FROM unnest(
                            COALESCE(verified_certifications, ARRAY[]::text[]) || %s::text[]
                        ) v
                    ),
                    cert_verification_status  = 'credly_verified',
                    cert_verification_sources = COALESCE(cert_verification_sources, '{}'::jsonb)
                                                || %s::jsonb,
                    updated_at = now()
                WHERE uid = %s
            """, (
                cert_codes,
                json.dumps({"credly_verified": True, "credly_username": username}),
                uid,
            ))
            updated += cur.rowcount

    if not dry_run:
        conn.commit()

    print(f"  [{tier}] matched={len(rows)} | db_updated={updated}")
    return matched_usernames


# ── Step 5: Insert unmatched ──────────────────────────────────────────────────

def insert_unmatched(conn, records: list, matched_usernames: set, dry_run: bool) -> int:
    unmatched = [r for r in records if r["credly_username"] not in matched_usernames]
    print(f"  Unmatched (new candidates): {len(unmatched)}")
    if dry_run or not unmatched:
        return len(unmatched)

    inserted = 0
    with conn.cursor() as cur:
        for r in unmatched:
            uid = f"credly_{r['credly_username']}" if r["credly_username"] \
                  else f"credly_{uuid.uuid4().hex[:16]}"
            cur.execute("""
                INSERT INTO candidates (
                    uid, name, linkedin_url, city, state, country, location_raw,
                    certifications, verified_certifications,
                    cert_verification_status, cert_verification_sources,
                    source, added_at, updated_at
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,
                    %s,%s,
                    'credly_verified', %s,
                    'credly', now(), now()
                )
                ON CONFLICT (uid) DO UPDATE SET
                    verified_certifications = (
                        SELECT array_agg(DISTINCT v ORDER BY v)
                        FROM unnest(
                            COALESCE(candidates.verified_certifications, ARRAY[]::text[])
                            || EXCLUDED.verified_certifications
                        ) v
                    ),
                    cert_verification_status = 'credly_verified',
                    updated_at = now()
            """, (
                uid, r["name"], r["linkedin_url"],
                r["city"], r["state"], r["country"], r["location_raw"],
                r["cert_codes"], r["cert_codes"],
                json.dumps({"credly_verified": True, "credly_username": r["credly_username"]}),
            ))
            inserted += cur.rowcount
    conn.commit()
    return inserted


# ── Step 6: GIN index ─────────────────────────────────────────────────────────

def ensure_indexes(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_candidates_verified_certs_gin
            ON candidates USING GIN (verified_certifications)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_candidates_credly_verified
            ON candidates (cert_verification_status)
            WHERE cert_verification_status = 'credly_verified'
        """)
    conn.commit()
    print("  Indexes ensured")


# ── Main ──────────────────────────────────────────────────────────────────────

def run(dry_run: bool, skip_consolidate: bool):
    print(f"\n{'='*60}")
    print(f"  Credly Full Consolidation + Enrich  {'[DRY RUN]' if dry_run else ''}")
    print(f"{'='*60}\n")

    if skip_consolidate:
        print("Step 1: Skipped (--skip-consolidate)\n")
        # Still need to read consolidated files to build staging records
        s3 = get_s3()
        all_cert_records = {}
        for slug in CERTS:
            cert_code = SLUG_TO_CODE[slug]
            key = f"{CREDLY_PREFIX}{slug}/consolidated/all.json"
            body = s3.get_object(Bucket=BRONZE_BUCKET, Key=key)["Body"].read()
            all_cert_records[slug] = json.loads(body)
            print(f"  {cert_code}: {len(all_cert_records[slug])} records from consolidated file")
    else:
        print("Step 1: Consolidating all S3 files per cert (dedup by credly_id)...")
        all_cert_records = consolidate_all(dry_run)

    print(f"\nStep 2: Building unified staging records (cross-cert dedup)...")
    staging_records = build_staging_records(all_cert_records)
    cert_dist = {}
    for r in staging_records:
        for c in r["cert_codes"]:
            cert_dist[c] = cert_dist.get(c, 0) + 1
    print(f"  Total unique candidates: {len(staging_records)}")
    for cert, count in sorted(cert_dist.items()):
        print(f"    {cert}: {count}")

    conn = get_db()

    print(f"\nStep 3: Staging into temp table...")
    create_staging(conn)
    insert_staging(conn, staging_records)

    print(f"\nStep 4: Matching & enriching BrightData candidates...")
    matched = set()

    # Tier 1: LinkedIn URL
    matched |= enrich_matched(conn, """
        SELECT s.credly_username, s.cert_codes, c.uid
        FROM credly_staging s
        JOIN candidates c
          ON c.linkedin_url IS NOT NULL AND c.linkedin_url != ''
         AND lower(regexp_replace(c.linkedin_url, '.*/in/([^/?#]+).*', '\\1')) =
             lower(regexp_replace(s.linkedin_url,  '.*/in/([^/?#]+).*', '\\1'))
        WHERE s.linkedin_url != ''
    """, "linkedin_url", dry_run)

    # Tier 2: Name + State
    matched |= enrich_matched(conn, """
        SELECT s.credly_username, s.cert_codes, c.uid
        FROM credly_staging s
        JOIN candidates c
          ON lower(trim(c.name)) = s.name_norm
         AND lower(trim(c.state)) = lower(trim(s.state))
        WHERE s.state != ''
          AND s.credly_username NOT IN (SELECT unnest(%s::text[]))
    """ if matched else """
        SELECT s.credly_username, s.cert_codes, c.uid
        FROM credly_staging s
        JOIN candidates c
          ON lower(trim(c.name)) = s.name_norm
         AND lower(trim(c.state)) = lower(trim(s.state))
        WHERE s.state != ''
    """, "name+state", dry_run)

    # Tier 3: DISABLED — name-only matching produces too many false positives
    # for common names (Andrew Bennett, Ajay Kumar etc.) even with cert overlap check
    # because BrightData certifications[] contains raw free-text strings that can
    # accidentally overlap. Only Tier 1 (LinkedIn URL) and Tier 2 (name+state) are safe.
    # matched |= enrich_matched(conn, "...", "name_only+sn_cert_overlap", dry_run)

    print(f"  Total matched+enriched: {len(matched)}")

    print(f"\nStep 5: Inserting unmatched as new candidates...")
    inserted = insert_unmatched(conn, staging_records, matched, dry_run)
    print(f"  Inserted: {inserted}")

    print(f"\nStep 6: Ensuring GIN indexes...")
    if not dry_run:
        ensure_indexes(conn)

    conn.close()

    print(f"\n{'='*60}")
    print(f"  DONE {'[DRY RUN - no writes]' if dry_run else ''}")
    print(f"  Unique Credly candidates : {len(staging_records)}")
    print(f"  Matched+enriched         : {len(matched)}")
    print(f"  New from Credly          : {inserted}")
    print(f"{'='*60}")
    print("""
Multi-cert query examples:
  -- ALL of CSA + CAD + CIS-HAM + CIS-CSM:
  SELECT uid, name, state, verified_certifications
  FROM candidates
  WHERE verified_certifications @> ARRAY['csa','cad','cis-ham','cis-csm']
    AND cert_verification_status = 'credly_verified';

  -- ANY of CSA or CAD:
  SELECT uid, name, state, verified_certifications
  FROM candidates
  WHERE verified_certifications && ARRAY['csa','cad']
    AND cert_verification_status = 'credly_verified';

  -- Count per cert:
  SELECT cert, count(*) FROM (
    SELECT unnest(verified_certifications) AS cert FROM candidates
    WHERE cert_verification_status = 'credly_verified'
  ) t GROUP BY cert ORDER BY count DESC;
""")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run",       action="store_true")
    parser.add_argument("--consolidate",   action="store_true",
                        help="Re-consolidate all raw S3 files (slow). Default: read consolidated/all.json")
    args, _ = parser.parse_known_args()
    run(args.dry_run, skip_consolidate=not args.consolidate)
