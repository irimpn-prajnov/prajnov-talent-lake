"""
Cert Verification Audit
=======================
Tests the Credly lookup against a stratified 150-candidate sample and measures
miss breakdown by root cause. Produces a JSON report + printed summary.

Audit groups:
  g1_credly_earners    — 50 known badge holders (ground truth, should all pass)
  g2_bd_fullname       — 50 BrightData SN cert claimants with full names
  g3_bd_problem_name   — 25 BrightData claimants with problematic names
  g4_bd_already_verified — 25 BrightData already credly_verified (control)

For each candidate we test:
  1. Direct badge URL (if credly_url known)
  2. Exact name search via filter[user_name]
  3. Name variants (lowercase, no punctuation, first+last only, unicode normalised)
  4. LinkedIn ID cross-check on any match found

Outcomes recorded per candidate:
  verified              — badge confirmed, identity corroborated
  ambiguous_identity    — badge found but identity signals don't match
  no_public_match       — all variants tried, nothing found
  private_or_opted_out  — direct URL works but badge is private/hidden
  lookup_failed         — HTTP error / timeout (not "not found")

Usage:
  python3 -u cert_verification_audit.py
  python3 -u cert_verification_audit.py --out /tmp/audit_report.json
"""
import argparse, json, os, re, sys, time, unicodedata
from datetime import datetime, timezone
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion"))
from common import get_db, normalize_cert

# ── Config ────────────────────────────────────────────────────────────────────
CREDLY_BASE   = "https://www.credly.com"
CREDLY_ORG_ID = "687e2b74-213e-42f1-9f9c-71bd4fe8bea4"
CREDLY_DIR    = f"{CREDLY_BASE}/api/v1/directory"
HEADERS       = {
    "Accept":     "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer":    f"{CREDLY_BASE}/organizations/servicenow-new/directory",
}
REQUEST_DELAY = 0.4   # seconds between API calls
TIMEOUT       = 15


# ── Name variant generator ────────────────────────────────────────────────────

def name_variants(name: str) -> list:
    """Generate search variants for a name, deduped, ordered by confidence."""
    if not name:
        return []
    variants = []

    # Strip trailing period from the whole name first
    name = name.rstrip(". ")

    # Normalise unicode (é → e, ñ → n etc.)
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_name = "".join(c for c in nfkd if not unicodedata.combining(c))

    def add(v):
        v = v.strip()
        if v and v not in variants:
            variants.append(v)

    add(name)                                          # original
    add(name.lower())                                  # lowercase
    add(name.title())                                  # Title Case
    add(ascii_name)                                    # unicode stripped
    add(ascii_name.lower())

    # Remove punctuation variants (apostrophes, dots, hyphens)
    clean = re.sub(r"['\u2019\u0060\.\-]", " ", name)
    clean = re.sub(r"\s+", " ", clean).strip()
    add(clean)
    add(clean.lower())

    # First + last only (drop middle names/initials)
    parts = [p.rstrip(".") for p in name.split()]  # strip periods from each part
    parts = [p for p in parts if p]                 # drop empty
    if len(parts) > 2:
        add(f"{parts[0]} {parts[-1]}")
        add(f"{parts[0].lower()} {parts[-1].lower()}")

    # Drop trailing single initial (e.g. "Alex K" → "Alex")
    if len(parts) >= 2 and len(parts[-1]) == 1:
        add(parts[0])
        add(parts[0].lower())

    return variants


# ── Credly API helpers ────────────────────────────────────────────────────────

def credly_search_name(name: str) -> tuple:
    """
    Returns (results, outcome) where outcome is:
      'ok' | 'rate_limited' | 'error' | 'empty'
    """
    try:
        resp = requests.get(
            CREDLY_DIR,
            params={"organization_id": CREDLY_ORG_ID, "filter[user_name]": name, "page_size": 10},
            headers=HEADERS, timeout=TIMEOUT,
        )
        if resp.status_code == 429:
            return [], "rate_limited"
        if resp.status_code != 200:
            return [], f"http_{resp.status_code}"
        data = resp.json().get("data", [])
        return data, "ok" if data else "empty"
    except requests.Timeout:
        return [], "timeout"
    except Exception as e:
        return [], f"error:{e}"


def credly_fetch_badges(username: str) -> tuple:
    """Returns (badges, outcome)."""
    try:
        resp = requests.get(
            f"{CREDLY_BASE}/users/{username}/badges.json",
            headers=HEADERS, timeout=TIMEOUT,
        )
        if resp.status_code == 404:
            return [], "not_found"
        if resp.status_code == 403:
            return [], "private"
        if resp.status_code != 200:
            return [], f"http_{resp.status_code}"
        return resp.json().get("data", []), "ok"
    except requests.Timeout:
        return [], "timeout"
    except Exception as e:
        return [], f"error:{e}"


def credly_verify_direct_url(credly_url: str) -> tuple:
    """
    Verify a known credly profile URL directly.
    Returns (badges, outcome).
    """
    username = credly_url.rstrip("/").split("/users/")[-1] if "/users/" in credly_url else ""
    if not username:
        return [], "no_username"
    badges, outcome = credly_fetch_badges(username)
    time.sleep(REQUEST_DELAY)
    return badges, outcome


def identity_score(earner: dict, candidate: dict, badges: list) -> tuple:
    """
    Score identity confidence between a Credly earner and a candidate.
    Returns (corroborated: bool|None, reason: str)

    Signals (Credly never exposes LinkedIn IDs directly):
      1. linkedin_id name-part appears in Credly vanity_url     — strong
      2. Claimed SN certs ⊆ found badge certs                   — strong
      3. Partial cert overlap + rare name (>3 tokens or >18 chars) — moderate
      4. Location match (city/state in earner location string)   — moderate
      5. Name match only, common name, no other signal           — weak → ambiguous
    """
    name        = candidate.get("name") or ""
    linkedin_id = (candidate.get("linkedin_id") or "").lower().rstrip("/")
    claimed     = set(candidate.get("certifications") or [])
    vanity      = (earner.get("vanity_url") or "").lower()
    location    = (earner.get("location") or "").lower()

    found_certs = set()
    for b in badges:
        code = normalize_cert(b.get("badge_template", {}).get("name", ""))
        if code:
            found_certs.add(code)

    signals = []

    # Signal 1: linkedin_id name-part in Credly vanity_url
    # Strip trailing numeric IDs (e.g. kevin-tran-3b3ba3209 -> kevin-tran)
    # then check if that prefix appears in the vanity URL
    if linkedin_id and len(linkedin_id) > 4:
        name_part = re.sub(r'[-][0-9a-f]{6,}$', '', linkedin_id)  # strip hex suffix
        name_part = re.sub(r'-\d+$', '', name_part)                # strip numeric suffix
        if name_part and len(name_part) > 4 and name_part in vanity:
            signals.append("vanity_url_match")

    # Signal 2: claimed SN certs are subset of found certs
    sn_claimed = {c for c in claimed if re.match(r'^(csa|cad|cta|cma|cis-)', c)}
    if sn_claimed and sn_claimed.issubset(found_certs):
        signals.append("cert_overlap")
    elif sn_claimed and found_certs and sn_claimed & found_certs:
        signals.append("cert_partial_overlap")

    # Signal 3: name rarity
    parts      = name.strip().split()
    name_len   = sum(len(p) for p in parts)
    if len(parts) >= 3 or name_len > 18:
        signals.append("rare_name")

    # Signal 4: location match
    if location:
        city  = (candidate.get("city") or "").lower()
        state = (candidate.get("state") or "").lower()
        if (city and len(city) > 3 and city in location) or \
           (state and len(state) > 3 and state in location):
            signals.append("location_match")

    # Decision
    strong = {"vanity_url_match", "cert_overlap"}
    if strong & set(signals):
        return True, "+".join(signals)
    if "cert_partial_overlap" in signals and ("rare_name" in signals or "location_match" in signals):
        return True, "+".join(signals)
    if signals:
        return None, "weak:" + "+".join(signals)
    return False, "name_only_no_corroboration"


def norm_name(name: str) -> str:
    return " ".join(name.lower().strip().split()) if name else ""


def best_match(target_name: str, earners: list) -> dict:
    target = norm_name(target_name)
    for e in earners:
        full = norm_name(f"{e.get('first_name','')} {e.get('last_name','')}")
        if full == target:
            return e
    return {}


# ── Per-candidate audit ───────────────────────────────────────────────────────

def audit_candidate(row: dict) -> dict:
    """
    Run full verification audit for one candidate.
    Returns a result dict with outcome + evidence.
    """
    name         = row["name"] or ""
    linkedin_id  = (row["linkedin_id"] or "").strip().rstrip("/")
    credly_url   = row.get("credly_url") or ""
    certifications = row.get("certifications") or []

    result = {
        "name":            name,
        "linkedin_id":     linkedin_id,
        "audit_group":     row["audit_group"],
        "certifications":  certifications,
        "credly_url_known": bool(credly_url),
        "variants_tried":  [],
        "match_found":     False,
        "match_variant":   None,
        "identity_corroborated": None,
        "badges_found":    [],
        "cert_codes_found": [],
        "outcome":         None,
        "miss_reason":     None,
        "api_outcomes":    [],
    }

    # ── Step 1: Direct URL if known ───────────────────────────────────────────
    if credly_url:
        badges, outcome = credly_verify_direct_url(credly_url)
        result["api_outcomes"].append({"method": "direct_url", "outcome": outcome})
        if outcome == "ok" and badges:
            cert_codes = sorted({c for b in badges if (c := normalize_cert(
                b.get("badge_template", {}).get("name", "")))})
            corroborated, reason = identity_score({}, row, badges)
            result.update({
                "match_found":           True,
                "match_variant":         "direct_url",
                "badges_found":          [b.get("badge_template", {}).get("name") for b in badges],
                "cert_codes_found":      cert_codes,
                "identity_corroborated": True,   # direct URL = identity confirmed
                "identity_reason":       "direct_url",
                "outcome":               "verified",
            })
            return result
        elif outcome == "private":
            result["outcome"]    = "private_or_opted_out"
            result["miss_reason"] = "badge_private"
            return result

    # ── Step 2: Name variant search ───────────────────────────────────────────
    variants = name_variants(name)
    result["variants_tried"] = variants

    for variant in variants:
        earners, api_outcome = credly_search_name(variant)
        result["api_outcomes"].append({"method": f"name_search:{variant}", "outcome": api_outcome})
        time.sleep(REQUEST_DELAY)

        if api_outcome == "rate_limited":
            time.sleep(30)
            earners, api_outcome = credly_search_name(variant)
            result["api_outcomes"].append({"method": f"name_search_retry:{variant}", "outcome": api_outcome})
            time.sleep(REQUEST_DELAY)

        if api_outcome not in ("ok", "empty"):
            result["outcome"]    = "lookup_failed"
            result["miss_reason"] = f"api_{api_outcome}"
            return result

        if not earners:
            continue

        # Try exact match first, then any match
        match = best_match(name, earners) or earners[0]
        username = match.get("vanity_url", "").replace("/users/", "").lstrip("/")
        if not username:
            continue

        badges, badge_outcome = credly_fetch_badges(username)
        result["api_outcomes"].append({"method": f"fetch_badges:{username}", "outcome": badge_outcome})
        time.sleep(REQUEST_DELAY)

        if badge_outcome == "private":
            result["outcome"]    = "private_or_opted_out"
            result["miss_reason"] = "badge_private"
            result["match_found"] = True
            result["match_variant"] = variant
            return result

        if badge_outcome != "ok" or not badges:
            continue

        corroborated, reason = identity_score(match, row, badges)
        cert_codes = sorted({c for b in badges if (c := normalize_cert(
            b.get("badge_template", {}).get("name", "")))})

        result.update({
            "match_found":           True,
            "match_variant":         variant,
            "badges_found":          [b.get("badge_template", {}).get("name") for b in badges],
            "cert_codes_found":      cert_codes,
            "identity_corroborated": corroborated,
            "identity_reason":       reason,
        })

        if corroborated is False:
            result["outcome"]     = "ambiguous_identity"
            result["miss_reason"] = "name_only_no_corroboration"
        elif corroborated is None:
            result["outcome"]     = "ambiguous_identity"
            result["miss_reason"] = f"weak_signal:{reason}"
        else:
            result["outcome"] = "verified"
        return result

    # ── No match found across all variants ───────────────────────────────────
    result["outcome"] = "no_public_match"

    # Classify miss reason
    parts = name.split()
    if len(parts) < 2:
        result["miss_reason"] = "single_token_name"
    elif any(len(p.rstrip(".")) == 1 for p in parts):
        result["miss_reason"] = "abbreviated_name"
    elif re.search(r"['\u2019\.\-]{1}", name) and len(parts) == 2:
        result["miss_reason"] = "punctuation_in_name"
    elif len(parts) > 3:
        result["miss_reason"] = "long_name_possible_mismatch"
    else:
        result["miss_reason"] = "not_on_credly_or_private"

    return result


# ── Load sample from DB ───────────────────────────────────────────────────────

def load_sample(conn) -> list:
    rows = []
    queries = [
        ("g1_credly_earners", """
            SELECT 'g1_credly_earners' as audit_group,
                   name, linkedin_id, linkedin_url,
                   cert_codes as certifications, credly_url,
                   city as city, state as state
            FROM credly_earners ORDER BY random() LIMIT 200
        """),
        ("g2_bd_fullname", """
            SELECT 'g2_bd_fullname' as audit_group,
                   name, linkedin_id, linkedin_url,
                   certifications, '' as credly_url,
                   city, state
            FROM candidates
            WHERE source='brightdata' AND cert_verification_status='unverified'
              AND certifications && ARRAY['csa','cad','cis-csm','cis-itsm']
              AND name NOT LIKE '%%. %%' AND length(name) > 6
            ORDER BY random() LIMIT 150
        """),
        ("g3_bd_problem_name", """
            SELECT 'g3_bd_problem_name' as audit_group,
                   name, linkedin_id, linkedin_url,
                   certifications, '' as credly_url,
                   city, state
            FROM candidates
            WHERE source='brightdata' AND cert_verification_status='unverified'
              AND certifications && ARRAY['csa','cad','cis-csm','cis-itsm']
              AND (name LIKE '%%. %%' OR name LIKE '%%..%%' OR length(name) < 6)
            ORDER BY random() LIMIT 75
        """),
        ("g4_bd_verified_control", """
            SELECT 'g4_bd_verified_control' as audit_group,
                   name, linkedin_id, linkedin_url,
                   verified_certifications as certifications, '' as credly_url,
                   city, state
            FROM candidates
            WHERE source='brightdata' AND cert_verification_status='credly_verified'
            ORDER BY random() LIMIT 75
        """),
    ]
    for label, sql in queries:
        for attempt in range(3):
            try:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    rows.extend([dict(r) for r in cur.fetchall()])
                break
            except Exception as e:
                print(f"  [WARN] {label} attempt {attempt+1}: {e} — reconnecting")
                try:
                    conn.close()
                except Exception:
                    pass
                conn = get_db()
                time.sleep(2)
    return rows


# ── Report ────────────────────────────────────────────────────────────────────

def print_report(results: list):
    from collections import defaultdict, Counter

    by_group = defaultdict(list)
    for r in results:
        by_group[r["audit_group"]].append(r)

    print("\n" + "="*70)
    print("CERT VERIFICATION AUDIT REPORT")
    print(f"Run at: {datetime.now(timezone.utc).isoformat()}")
    print("="*70)

    overall_outcomes = Counter()
    overall_miss_reasons = Counter()

    for group, rows in sorted(by_group.items()):
        outcomes    = Counter(r["outcome"] for r in rows)
        miss_reasons = Counter(r["miss_reason"] for r in rows if r["miss_reason"])
        verified    = outcomes.get("verified", 0)
        total       = len(rows)

        print(f"\n── {group} ({total} candidates) ──")
        print(f"  verified              : {verified} ({verified/total*100:.0f}%)")
        for outcome, count in outcomes.most_common():
            if outcome != "verified":
                print(f"  {outcome:<28}: {count}")
        if miss_reasons:
            print("  Miss reasons:")
            for reason, count in miss_reasons.most_common():
                print(f"    {reason:<30}: {count}")

        overall_outcomes.update(outcomes)
        overall_miss_reasons.update(miss_reasons)

    total = len(results)
    verified = overall_outcomes.get("verified", 0)
    print(f"\n── OVERALL ({total} candidates) ──")
    print(f"  verified              : {verified} ({verified/total*100:.0f}%)")
    for outcome, count in overall_outcomes.most_common():
        if outcome != "verified":
            print(f"  {outcome:<28}: {count}")
    print("  Miss reasons:")
    for reason, count in overall_miss_reasons.most_common():
        print(f"    {reason:<30}: {count}")

    # Control group check
    control = by_group.get("g4_bd_verified_control", [])
    if control:
        control_pass = sum(1 for r in control if r["outcome"] == "verified")
        print(f"\n  Control group (g4) pass rate: {control_pass}/{len(control)}")
        if control_pass < len(control) * 0.8:
            print("  ⚠ WARNING: control pass rate < 80% — connector may have issues")

    print("="*70)


# ── Main ──────────────────────────────────────────────────────────────────────

def run(out_path: str = None):
    conn    = get_db()
    print("Loading audit sample from DB...")
    sample  = load_sample(conn)
    conn.close()
    print(f"  Sample size: {len(sample)}\n")

    results = []
    total   = len(sample)

    for i, row in enumerate(sample, 1):
        name = row["name"] or "(no name)"
        print(f"[{i}/{total}] {row['audit_group']} | {name}", end=" ... ", flush=True)
        result = audit_candidate(row)
        results.append(result)
        print(f"{result['outcome']}" + (f" ({result['miss_reason']})" if result["miss_reason"] else ""))

    print_report(results)

    out = out_path or f"/tmp/cert_audit_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
    with open(out, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nFull results saved to: {out}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=None, help="Output JSON path")
    args, _ = parser.parse_known_args()
    run(args.out)
