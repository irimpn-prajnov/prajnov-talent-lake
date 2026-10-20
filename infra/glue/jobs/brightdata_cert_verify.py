"""
Job: brightdata_cert_verify
For every BrightData candidate:
  Tier 1 — Search Credly by name → get all verified badges (free)
  Tier 2 — SN Partner Portal via Playwright (for those not found on Credly)

Updates candidates.verified_certifications, cert_verification_status,
cert_verification_sources, cert_verified_at, embedding (re-embed with certs).

Usage:
  python3 -u brightdata_cert_verify.py                    # all 92K
  python3 -u brightdata_cert_verify.py --us-only          # US only
  python3 -u brightdata_cert_verify.py --limit 1000       # test run
  python3 -u brightdata_cert_verify.py --tier1-only       # skip portal
  python3 -u brightdata_cert_verify.py --workers 5        # parallel Credly searches
  python3 -u brightdata_cert_verify.py --sn-only          # only candidates claiming SN certs
"""
import argparse, json, os, sys, time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "ingestion"))
from common import get_db, get_s3, BRONZE_BUCKET, now_iso, date_path, normalize_cert

# ── Config ────────────────────────────────────────────────────────────────────
CREDLY_BASE   = "https://www.credly.com"
CREDLY_ORG_ID = "687e2b74-213e-42f1-9f9c-71bd4fe8bea4"
CREDLY_DIR    = "https://www.credly.com/api/v1/directory"
HEADERS       = {
    "Accept":     "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer":    "https://www.credly.com/organizations/servicenow-new/directory",
}
BATCH_SIZE    = 200
PORTAL_LIMIT  = int(os.getenv("PORTAL_LIMIT", "500"))
EMBED_MODEL   = "all-MiniLM-L6-v2"
_embed_model  = None

SN_CERT_KEYWORDS = {"csa","cad","cta","cma","cis","cis-csm","cis-itsm","cis-hrsd",
                    "cis-fsm","cis-sam","cis-ham","cis-vr","cis-rc","cis-sir",
                    "cis-disco","cis-spm","cis-itom","servicenow"}


def get_embed_model():
    global _embed_model
    if _embed_model is None:
        from sentence_transformers import SentenceTransformer
        _embed_model = SentenceTransformer(EMBED_MODEL)
    return _embed_model


def build_embed_text(r: dict, cert_codes: list) -> str:
    cert_text  = " ".join(cert_codes)
    skill_text = ", ".join((r.get("skills") or [])[:30])
    return " | ".join(filter(None, [
        r.get("name", ""),
        r.get("current_title", ""),
        r.get("current_company", ""),
        f"Location: {r.get('city','')} {r.get('state','')}".strip(),
        f"Certifications: {cert_text}" if cert_text else "",
        f"Skills: {skill_text}" if skill_text else "",
    ]))


# ── Credly helpers ────────────────────────────────────────────────────────────

def norm_name(name: str) -> str:
    return " ".join(name.lower().strip().split()) if name else ""


def credly_search(name: str) -> list:
    try:
        resp = requests.get(
            CREDLY_DIR,
            params={"organization_id": CREDLY_ORG_ID, "filter[user_name]": name, "page_size": 5},
            headers=HEADERS, timeout=15,
        )
        if resp.status_code == 429:
            time.sleep(10)
            return []
        resp.raise_for_status()
        return resp.json().get("data", [])
    except Exception:
        return []


def best_match(name: str, earners: list) -> dict:
    target = norm_name(name)
    for e in earners:
        full = norm_name(f"{e.get('first_name','')} {e.get('last_name','')}")
        if full == target:
            return e
    return {}


def fetch_all_badges(username: str) -> list:
    try:
        resp = requests.get(
            f"{CREDLY_BASE}/users/{username}/badges.json",
            headers=HEADERS, timeout=15,
        )
        resp.raise_for_status()
        return resp.json().get("data", [])
    except Exception:
        return []


def credly_verify(name: str) -> tuple:
    """
    Returns (username, badges_list) or ("", []) if not found.
    badges_list = [{"name": str, "date": str, "url": str}]
    """
    earners = credly_search(name)
    match   = best_match(name, earners)
    if not match:
        return "", []
    username   = match.get("vanity_url", "").replace("/users/", "").lstrip("/")
    badge_data = fetch_all_badges(username)
    time.sleep(0.3)
    badges = [
        {
            "name": b.get("badge_template", {}).get("name", ""),
            "date": (b.get("issued_at") or "")[:10],
            "url":  f"{CREDLY_BASE}/badges/{b.get('id','')}",
        }
        for b in badge_data
        if b.get("badge_template", {}).get("name")
    ]
    return username, badges


# ── SN Portal helpers ─────────────────────────────────────────────────────────

def portal_verify(name: str, email: str = "") -> list:
    """
    Returns list of cert codes verified on SN portal.
    Uses existing sn_cert_verify.py Playwright script.
    """
    try:
        sn_script = Path(__file__).resolve().parents[4] / "prajnov-recruiter" / "scripts" / "sn_cert_verify.py"
        sys.path.insert(0, str(sn_script.parent.parent))
        from scripts.sn_cert_verify import verify_candidate
        result = verify_candidate(name=name, email=email, headless=True, timeout_ms=25000)
        return result.get("verified_certs", [])
    except Exception as e:
        print(f"  [PORTAL ERROR] {name}: {e}")
        return []


# ── DB helpers ────────────────────────────────────────────────────────────────

def load_targets(conn, us_only: bool, sn_only: bool, fix_empty: bool = False) -> list:
    """Load BrightData candidates from DB directly — faster than parquet."""
    if fix_empty:
        # Re-run Credly for credly_verified rows where verified_certifications was never written
        filters = ["source = 'brightdata'",
                   "cert_verification_status = 'credly_verified'",
                   "verified_certifications = '{}'"]
    else:
        filters = ["source = 'brightdata'",
                   "cert_verification_status NOT IN ('credly_verified','portal_verified','verified')"]
    if us_only:
        filters.append("country = 'US'")
    if sn_only:
        # candidates who claim at least one SN cert keyword
        filters.append("""
            EXISTS (
                SELECT 1 FROM unnest(certifications) c
                WHERE c ILIKE ANY(ARRAY['%csa%','%cad%','%cta%','%cis%','%servicenow%'])
            )
        """)
    where = " AND ".join(filters)
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT uid, name, linkedin_id, linkedin_url, current_title, current_company,
                   city, state, country, skills, certifications, email
            FROM candidates
            WHERE {where}
            ORDER BY
                -- SN cert claimants first, then full names (no dots = not abbreviated)
                CASE WHEN certifications && ARRAY['csa','cad','cis-csm','cis-itsm','cis-hrsd',
                     'cis-fsm','cis-sam','cis-vr','cis-rc','cis-sir','cis-disco'] THEN 0 ELSE 1 END,
                CASE WHEN name NOT LIKE '%. %' THEN 0 ELSE 1 END,
                uid
        """)
        return cur.fetchall()


def update_candidate(cur, uid: str, cert_codes: list, source: str, embed_vec):
    cur.execute("""
        UPDATE candidates SET
            verified_certifications   = %s,
            cert_verification_status  = %s,
            cert_verification_sources = %s::jsonb,
            cert_verified_at          = now(),
            embedding                 = %s,
            updated_at                = now()
        WHERE uid = %s
    """, (
        cert_codes, source,
        json.dumps({c: source for c in cert_codes}),
        embed_vec,
        uid,
    ))
    return cur.rowcount


def flush_db(conn, db_buf: list, stats: dict) -> tuple:
    """Flush db_buf with reconnect on stale connection. Returns (conn, db_buf)."""
    if not db_buf:
        return conn, db_buf
    # reconnect if connection is closed
    try:
        conn.isolation_level  # cheap check
    except Exception:
        conn = get_db()
    try:
        with conn.cursor() as cur:
            for uid_, codes, src, vec_ in db_buf:
                stats["updated"] += update_candidate(cur, uid_, codes, src, vec_)
        conn.commit()
        return conn, []
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        conn = get_db()  # fresh connection
        print(f"  [ERROR] DB flush, reconnected: {e}")
        stats["errors"] += 1
        return conn, []


def flush_bronze(batch: list, batch_num: int, dp: str):
    if not batch:
        return
    key  = f"bronze/discovery/credly/brightdata_verify/{dp}/batch_{batch_num:04d}.jsonl"
    body = "\n".join(json.dumps(r, default=str) for r in batch).encode()
    get_s3().put_object(Bucket=BRONZE_BUCKET, Key=key, Body=body, ContentType="application/x-ndjson")


# ── Main ──────────────────────────────────────────────────────────────────────

def run(limit=None, us_only=False, tier1_only=False, workers=3, sn_only=False, fix_empty=False):
    dp   = date_path()
    conn = get_db()

    print("Loading BrightData targets from DB...")
    targets = load_targets(conn, us_only, sn_only, fix_empty)
    print(f"  Targets: {len(targets)}{' (US only)' if us_only else ''}{' (SN cert claimants)' if sn_only else ''}")

    if limit:
        targets = list(targets)[:limit]
    print(f"  Running: {len(targets)}\n")

    model = get_embed_model()

    stats      = {"t1_found": 0, "t2_found": 0, "not_found": 0, "updated": 0, "errors": 0}
    bronze_buf = []
    bronze_num = 0
    db_buf     = []   # (uid, cert_codes, source)
    portal_q   = []   # candidates for Tier 2
    total      = len(targets)

    # ── Tier 1: Credly (parallel) ─────────────────────────────────────────────
    print("=== Tier 1: Credly search ===")

    def process_one(r):
        name = r["name"] or ""
        if not name:
            return None
        username, badges = credly_verify(name)
        return (r, username, badges)

    # Chunked sliding window — never more than CHUNK_SIZE futures in-flight
    CHUNK_SIZE = workers * 50
    i = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for chunk_start in range(0, total, CHUNK_SIZE):
            chunk = targets[chunk_start:chunk_start + CHUNK_SIZE]
            futures = {ex.submit(process_one, r): r for r in chunk}
            for fut in as_completed(futures):
                i += 1
                try:
                    result = fut.result()
                    if result is None:
                        continue
                    r, username, badges = result
                    name = r["name"] or ""
                    uid  = r["uid"]

                    if badges:
                        cert_codes = sorted({c for b in badges if (c := normalize_cert(b["name"]))})
                        # Fall back to raw badge names if no canonical codes matched
                        if not cert_codes:
                            cert_codes = sorted({b["name"] for b in badges if b.get("name")})
                        stats["t1_found"] += 1
                        print(f"  [{i}/{total}] ✓ T1 {name} | {cert_codes}")

                        embed_text = build_embed_text(dict(r), cert_codes)
                        vec = model.encode([embed_text], normalize_embeddings=True)[0].tolist()

                        bronze_buf.append({
                            "credly_username": username,
                            "credly_url":      f"{CREDLY_BASE}/users/{username}",
                            "name":            name,
                            "uid":             uid,
                            "badges":          badges,
                            "cert_codes":      cert_codes,
                            "source":          "brightdata_credly_verify",
                            "verified_at":     now_iso(),
                        })
                        db_buf.append((uid, cert_codes, "credly_verified", vec))
                    else:
                        stats["not_found"] += 1
                        if not tier1_only:
                            portal_q.append(r)
                        if i % 1000 == 0:
                            print(f"  [{i}/{total}] checked | found={stats['t1_found']} not_found={stats['not_found']}")

                    if len(bronze_buf) >= BATCH_SIZE:
                        flush_bronze(bronze_buf, bronze_num, dp)
                        bronze_num += 1
                        bronze_buf = []

                    if len(db_buf) >= BATCH_SIZE:
                        conn, db_buf = flush_db(conn, db_buf, stats)

                except Exception as e:
                    print(f"  [ERROR] {e}")
                    stats["errors"] += 1

    # ── Tier 2: SN Portal ─────────────────────────────────────────────────────
    if not tier1_only and portal_q:
        portal_run = portal_q[:PORTAL_LIMIT]
        print(f"\n=== Tier 2: SN Portal ({len(portal_run)} candidates, limit={PORTAL_LIMIT}) ===")

        for i, r in enumerate(portal_run, 1):
            name  = r["name"] or ""
            uid   = r["uid"]
            email = r["email"] or ""
            print(f"  [{i}/{len(portal_run)}] Portal: {name}")

            cert_codes = portal_verify(name, email)
            time.sleep(4)

            if cert_codes:
                stats["t2_found"] += 1
                print(f"    ✓ T2 {name} | {cert_codes}")
                embed_text = build_embed_text(dict(r), cert_codes)
                vec = model.encode([embed_text], normalize_embeddings=True)[0].tolist()
                db_buf.append((uid, cert_codes, "portal_verified", vec))
            else:
                stats["not_found"] += 1

            if len(db_buf) >= BATCH_SIZE:
                conn, db_buf = flush_db(conn, db_buf, stats)

    # ── Final flush ───────────────────────────────────────────────────────────
    flush_bronze(bronze_buf, bronze_num, dp)
    conn, db_buf = flush_db(conn, db_buf, stats)
    conn.close()
    print(f"\nDone.")
    print(f"  Tier 1 (Credly) found : {stats['t1_found']}")
    print(f"  Tier 2 (Portal) found : {stats['t2_found']}")
    print(f"  Not found             : {stats['not_found']}")
    print(f"  DB rows updated       : {stats['updated']}")
    print(f"  Errors                : {stats['errors']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit",      type=int,  default=0)
    parser.add_argument("--us-only",    action="store_true")
    parser.add_argument("--tier1-only", action="store_true")
    parser.add_argument("--sn-only",    action="store_true")
    parser.add_argument("--fix-empty",  action="store_true", help="Re-run Credly for credly_verified rows with empty verified_certifications")
    parser.add_argument("--workers",    type=int,  default=3)
    args, _ = parser.parse_known_args()
    run(args.limit or None, args.us_only, args.tier1_only, args.workers, args.sn_only, args.fix_empty)
