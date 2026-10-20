"""
Glue Job: cert-verify
Verifies cert claims for all candidates in pgvector.

Tier 1 — Credly bronze match (free, instant, confidence=1.0)
  Match by linkedin_id → Credly bronze S3 index.
  Match by name if no linkedin_id match.

Tier 2 — SN Partner Portal (Playwright, confidence=1.0)
  Only for candidates who:
    a) claim a cert AND
    b) were NOT verified by Tier 1 AND
    c) have not been portal-checked in last 30 days

Result: candidates.verified_certifications[] contains ONLY proven certs.
AIR MUST use verified_certifications[], never certifications[].

Schedule: Weekly (Sunday 03:00 UTC)
"""
import json, logging, os, sys, time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import psycopg2, psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "processing"))
from cert_verification_engine import (
    verify_tier1_credly, verify_tier2_portal,
    write_verification_result, log_not_found,
    normalize_cert, db_conn,
)

PORTAL_THROTTLE_S  = 4.0
PORTAL_CONCURRENCY = int(os.getenv("PORTAL_CONCURRENCY", "2"))
PORTAL_BATCH_LIMIT = int(os.getenv("PORTAL_BATCH_LIMIT", "500"))  # max portal checks per run


def fetch_candidates_needing_verification(conn, force: bool = False) -> list:
    """
    Returns candidates that:
    1. Have cert claims (certifications[] not empty) but no verified_certifications yet, OR
    2. Have credly source (always re-verify against bronze), OR
    3. force=True (re-verify all)
    """
    recency_filter = "" if force else "AND (cert_verified_at IS NULL OR cert_verified_at < now() - interval '30 days')"

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT uid, name, linkedin_id, linkedin_url, email,
                   certifications, source, cert_verification_status,
                   sn_portal_verified, sn_portal_checked_at
            FROM candidates
            WHERE (
                array_length(certifications, 1) > 0
                OR source = 'credly'
            )
            {recency_filter}
            ORDER BY
                CASE source
                    WHEN 'credly'        THEN 1
                    WHEN 'signalhire'    THEN 2
                    WHEN 'brightdata'    THEN 3
                    WHEN 'resume_upload' THEN 4
                    ELSE 5
                END,
                name
        """)
        return cur.fetchall()


def run(force: bool = False):
    logger.info("=== cert-verify job started | force=%s ===", force)
    conn = db_conn()

    candidates = fetch_candidates_needing_verification(conn, force=force)
    logger.info("Candidates to verify: %d", len(candidates))

    stats = {
        "tier1_verified": 0,
        "tier2_verified": 0,
        "not_found":      0,
        "errors":         0,
        "portal_checks":  0,
    }

    portal_queue = []  # candidates for Tier 2

    # ── Tier 1: Credly bronze match (all candidates) ──────────────────────────
    logger.info("--- Tier 1: Credly bronze match ---")
    for cand in candidates:
        uid          = cand["uid"]
        name         = cand["name"] or ""
        linkedin_id  = cand["linkedin_id"] or ""
        claimed_raw  = cand["certifications"] or []
        claimed_norm = [c for c in (normalize_cert(r) for r in claimed_raw) if c]

        # Credly-sourced candidates: their cert IS the claim, verify against bronze
        if cand["source"] == "credly":
            claimed_norm = claimed_norm or []  # will match on name/linkedin

        try:
            verified = verify_tier1_credly(linkedin_id, name, claimed_norm)
            if verified:
                write_verification_result(conn, uid, verified, "credly_bronze")
                stats["tier1_verified"] += 1
                logger.info("  ✅ T1 %s — %s", name, list(verified.keys()))
            else:
                # Queue for Tier 2 if they claim a cert
                if claimed_norm:
                    portal_queue.append(cand)
                else:
                    log_not_found(conn, uid, "any", "credly_bronze", "no cert claims")
                    stats["not_found"] += 1
        except Exception as e:
            logger.error("  T1 error %s: %s", name, e)
            stats["errors"] += 1

    logger.info("Tier 1 done. verified=%d, queued_for_portal=%d",
                stats["tier1_verified"], len(portal_queue))

    # ── Tier 2: SN Partner Portal (unverified cert claimants only) ───────────
    logger.info("--- Tier 2: SN Portal (max %d checks) ---", PORTAL_BATCH_LIMIT)
    portal_candidates = portal_queue[:PORTAL_BATCH_LIMIT]

    for i, cand in enumerate(portal_candidates, 1):
        uid   = cand["uid"]
        name  = cand["name"] or ""
        email = cand["email"] or ""

        # Skip if portal-checked recently (within 30 days)
        if cand["sn_portal_checked_at"]:
            age = datetime.now(timezone.utc) - cand["sn_portal_checked_at"].replace(tzinfo=timezone.utc)
            if age < timedelta(days=30) and not force:
                stats["not_found"] += 1
                continue

        logger.info("  [%d/%d] Portal: %s", i, len(portal_candidates), name)
        try:
            verified = verify_tier2_portal(name, email)
            stats["portal_checks"] += 1

            if verified:
                write_verification_result(conn, uid, verified, "sn_portal")
                stats["tier2_verified"] += 1
                logger.info("    ✅ T2 %s — %s", name, list(verified.keys()))
            else:
                claimed_raw = cand["certifications"] or []
                for raw in claimed_raw:
                    code = normalize_cert(raw)
                    if code:
                        log_not_found(conn, uid, code, "sn_portal", "not found on portal")
                stats["not_found"] += 1
                logger.info("    — No portal match: %s", name)

            time.sleep(PORTAL_THROTTLE_S)

        except Exception as e:
            logger.error("    T2 error %s: %s", name, e)
            stats["errors"] += 1

    conn.close()

    logger.info("=== cert-verify done ===")
    logger.info("  Tier 1 verified : %d", stats["tier1_verified"])
    logger.info("  Tier 2 verified : %d", stats["tier2_verified"])
    logger.info("  Not found       : %d", stats["not_found"])
    logger.info("  Portal checks   : %d", stats["portal_checks"])
    logger.info("  Errors          : %d", stats["errors"])
    logger.info("  Total trusted   : %d", stats["tier1_verified"] + stats["tier2_verified"])


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--portal-limit", type=int, default=500)
    args, _ = parser.parse_known_args()
    os.environ["PORTAL_BATCH_LIMIT"] = str(args.portal_limit)
    run(force=args.force)
