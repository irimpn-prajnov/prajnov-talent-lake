"""
Glue Job: credly_gold_assemble
Source:   silver/candidate/source=credly/
          silver/candidate_certification/source=credly/
          silver/candidate_skill/source=credly/
          silver/candidate_experience/source=credly/
          silver/candidate_education/source=credly/
Output:   credly_earners table + candidates table (pgvector DB)

Enrichment priority per candidate:
  Tier 1 — BrightData silver name match (free, ~30-40% hit rate)
  Tier 2 — SignalHire credly_csm_candidates enrichment (existing DB table)
  Tier 3 — OpenAI silver fields (empty until credits topped up)

Schedule: Weekly (Sunday 03:00 UTC) — after credly_silver_normalize
"""
import json, os, sys, uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "ingestion"))
from common import (
    parquet_read, now_iso, get_watermark, set_watermark, get_db, compute_dq_score,
)

JOB_NAME    = "credly_gold_assemble"
BATCH_SIZE  = 500


# ── Helpers ───────────────────────────────────────────────────────────────────

def norm_name(name: str) -> str:
    return " ".join(name.lower().strip().split()) if name else ""


def load_silver(prefix: str) -> list:
    rows = parquet_read(prefix)
    print(f"  {prefix}: {len(rows)} rows")
    return rows


def build_brightdata_index() -> dict:
    """name → record (or list on collision)"""
    index = {}
    for r in parquet_read("silver/candidate/source=brightdata/"):
        key = norm_name(r.get("canonical_name", ""))
        if not key:
            continue
        if key not in index:
            index[key] = r
        else:
            existing = index[key]
            index[key] = existing if isinstance(existing, list) else [existing]
            index[key].append(r)
    print(f"  BrightData index: {len(index)} unique names")
    return index


def match_brightdata(name: str, country: str, state: str, index: dict) -> dict:
    hit = index.get(norm_name(name))
    if not hit:
        return {}
    if isinstance(hit, dict):
        return hit
    # disambiguate by location
    if country:
        by_country = [r for r in hit if (r.get("location_country") or "").upper() == country.upper()]
        if len(by_country) == 1:
            return by_country[0]
        if state:
            by_state = [r for r in by_country if (r.get("location_state") or "").lower() == state.lower()]
            if by_state:
                return by_state[0]
        if by_country:
            return by_country[0]
    return hit[0]  # ambiguous — take first


def load_sh_enrichment(conn) -> dict:
    """Load existing SignalHire enrichment from credly_csm_candidates if table exists."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT credly_username, sh_linkedin_url, sh_title, sh_company,
                       sh_email, sh_phone, sh_signalhire_url
                FROM credly_csm_candidates WHERE sh_searched = true
            """)
            rows = {r["credly_username"]: r for r in cur.fetchall()}
        print(f"  SignalHire enrichment: {len(rows)} records")
        return rows
    except Exception as e:
        print(f"  SignalHire enrichment skipped: {e}")
        return {}


# ── Main ──────────────────────────────────────────────────────────────────────

def run(cert_filter=None):
    run_start = now_iso()
    print(f"credly_gold_assemble | cert={cert_filter or 'all'}")

    # ── Load silver ──────────────────────────────────────────────────────────
    cand_prefix = f"silver/candidate/source=credly/cert={cert_filter}/" if cert_filter \
                  else "silver/candidate/source=credly/"

    def cert_prefix(table):
        return f"silver/{table}/source=credly/cert={cert_filter}/" if cert_filter \
               else f"silver/{table}/source=credly/"

    candidates = load_silver(cand_prefix)
    if not candidates:
        print("Nothing to assemble.")
        return

    # dedup silver by credly_username (keep latest created_at)
    seen_usernames = {}
    for c in candidates:
        u = c.get("credly_username", "")
        if not u:
            continue
        if u not in seen_usernames or c.get("created_at", "") > seen_usernames[u].get("created_at", ""):
            seen_usernames[u] = c
    candidates = list(seen_usernames.values())
    print(f"  After dedup: {len(candidates)} unique candidates")

    certs_by  = defaultdict(list)
    skills_by = defaultdict(list)
    exp_by    = defaultdict(list)
    edu_by    = defaultdict(list)

    for r in load_silver(cert_prefix("candidate_certification")):
        certs_by[r["candidate_id"]].append(r)
    for r in load_silver(cert_prefix("candidate_skill")):
        skills_by[r["candidate_id"]].append(r)
    for r in load_silver(cert_prefix("candidate_experience")):
        exp_by[r["candidate_id"]].append(r)
    for r in load_silver(cert_prefix("candidate_education")):
        edu_by[r["candidate_id"]].append(r)

    # ── Enrichment sources ───────────────────────────────────────────────────
    print("\nLoading enrichment sources...")
    bd_index = build_brightdata_index()
    conn     = get_db()
    sh_map   = load_sh_enrichment(conn)

    # ── Process ──────────────────────────────────────────────────────────────
    stats   = {"upserted": 0, "errors": 0, "bd_match": 0, "sh_match": 0, "no_match": 0}
    batch_n = 0

    for i, c in enumerate(candidates):
        cid      = c["candidate_id"]
        username = c.get("credly_username", "")
        sh       = sh_map.get(username, {})

        # Tier 1: BrightData
        bd = match_brightdata(
            c.get("canonical_name", ""),
            c.get("location_country", ""),
            c.get("location_state", ""),
            bd_index,
        )

        if bd:
            stats["bd_match"] += 1
            title        = bd.get("current_title") or sh.get("sh_title", "") or c.get("current_title", "") or ""
            company      = bd.get("current_company") or sh.get("sh_company", "") or c.get("current_company", "") or ""
            linkedin_url = bd.get("linkedin_url") or sh.get("sh_linkedin_url", "") or c.get("linkedin_url", "") or ""
            location_city    = bd.get("location_city") or c.get("location_city", "") or ""
            location_state   = bd.get("location_state") or c.get("location_state", "") or ""
            location_country = bd.get("location_country") or c.get("location_country", "") or ""
            years_exp        = bd.get("years_experience") or c.get("years_experience")
            enrichment_src   = "brightdata"
        elif sh:
            stats["sh_match"] += 1
            title        = sh.get("sh_title", "") or c.get("current_title", "") or ""
            company      = sh.get("sh_company", "") or c.get("current_company", "") or ""
            linkedin_url = sh.get("sh_linkedin_url", "") or c.get("linkedin_url", "") or ""
            location_city    = c.get("location_city", "") or ""
            location_state   = c.get("location_state", "") or ""
            location_country = c.get("location_country", "") or ""
            years_exp        = c.get("years_experience")
            enrichment_src   = "signalhire"
        else:
            stats["no_match"] += 1
            title        = c.get("current_title", "") or ""
            company      = c.get("current_company", "") or ""
            linkedin_url = c.get("linkedin_url", "") or ""
            location_city    = c.get("location_city", "") or ""
            location_state   = c.get("location_state", "") or ""
            location_country = c.get("location_country", "") or ""
            years_exp        = c.get("years_experience")
            enrichment_src   = "credly_only"

        cert_codes   = sorted({r["cert_code"] for r in certs_by.get(cid, [])})
        cert_details = [
            {"code": r["cert_code"], "name": r["cert_name"],
             "issued_date": r.get("issued_date", ""), "badge_url": r.get("verification_url", "")}
            for r in certs_by.get(cid, [])
        ]
        skills = sorted({r["skill_name"] for r in skills_by.get(cid, [])})
        experience = [
            {"company": e["company"], "title": e["title"],
             "start_year": e.get("start_year"), "end_year": e.get("end_year"),
             "is_current": e.get("is_current", False)}
            for e in exp_by.get(cid, [])
        ]
        education = [
            {"institution": e["institution"], "degree": e.get("degree", ""),
             "field": e.get("field", ""), "end_year": e.get("end_year")}
            for e in edu_by.get(cid, [])
        ]

        linkedin_id = linkedin_url.split("/in/")[-1].rstrip("/") if "/in/" in linkedin_url else ""

        dq = compute_dq_score({
            "canonical_name": c.get("canonical_name"), "current_title": title,
            "current_company": company, "location_state": location_state,
            "location_city": location_city, "linkedin_url": linkedin_url,
            "verified_certs": cert_codes, "experience": experience,
            "skills": skills, "email": sh.get("sh_email", "") if sh else "",
        })

        try:
            with conn.cursor() as cur:
                # ── credly_earners ──────────────────────────────────────────
                cur.execute("""
                    INSERT INTO credly_earners (
                        credly_username, credly_url, credly_id,
                        name, first_name, last_name,
                        location_raw, city, state, country,
                        badge_count, skills, cert_codes, cert_details,
                        linkedin_url, linkedin_id,
                        sh_title, sh_company, sh_email, sh_phone, sh_signalhire_url,
                        openai_title, openai_company, openai_location, openai_summary,
                        openai_confidence, openai_searched,
                        candidate_uid, bronze_sources,
                        enrichment_source,
                        created_at, updated_at
                    ) VALUES (
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,now(),now()
                    )
                    ON CONFLICT (credly_username) DO UPDATE SET
                        cert_codes       = EXCLUDED.cert_codes,
                        cert_details     = EXCLUDED.cert_details,
                        skills           = CASE WHEN cardinality(EXCLUDED.skills) > cardinality(credly_earners.skills)
                                                THEN EXCLUDED.skills ELSE credly_earners.skills END,
                        linkedin_url     = CASE WHEN EXCLUDED.linkedin_url != '' THEN EXCLUDED.linkedin_url
                                                ELSE credly_earners.linkedin_url END,
                        linkedin_id      = CASE WHEN EXCLUDED.linkedin_id != '' THEN EXCLUDED.linkedin_id
                                                ELSE credly_earners.linkedin_id END,
                        city             = CASE WHEN EXCLUDED.city != '' THEN EXCLUDED.city ELSE credly_earners.city END,
                        state            = CASE WHEN EXCLUDED.state != '' THEN EXCLUDED.state ELSE credly_earners.state END,
                        country          = CASE WHEN EXCLUDED.country != '' THEN EXCLUDED.country ELSE credly_earners.country END,
                        sh_title         = CASE WHEN EXCLUDED.sh_title != '' THEN EXCLUDED.sh_title ELSE credly_earners.sh_title END,
                        sh_company       = CASE WHEN EXCLUDED.sh_company != '' THEN EXCLUDED.sh_company ELSE credly_earners.sh_company END,
                        sh_email         = CASE WHEN EXCLUDED.sh_email != '' THEN EXCLUDED.sh_email ELSE credly_earners.sh_email END,
                        openai_title     = CASE WHEN EXCLUDED.openai_title != '' THEN EXCLUDED.openai_title ELSE credly_earners.openai_title END,
                        openai_company   = CASE WHEN EXCLUDED.openai_company != '' THEN EXCLUDED.openai_company ELSE credly_earners.openai_company END,
                        enrichment_source= EXCLUDED.enrichment_source,
                        updated_at       = now()
                """, (
                    username, c.get("credly_url", ""), c.get("credly_id", ""),
                    c.get("canonical_name", ""), c.get("first_name", ""), c.get("last_name", ""),
                    c.get("location_raw", ""), location_city, location_state, location_country,
                    c.get("badge_count", 0), skills, cert_codes, json.dumps(cert_details),
                    linkedin_url, linkedin_id,
                    sh.get("sh_title", "") if sh else "",
                    sh.get("sh_company", "") if sh else "",
                    sh.get("sh_email", "") if sh else "",
                    sh.get("sh_phone", "") if sh else "",
                    sh.get("sh_signalhire_url", "") if sh else "",
                    c.get("current_title", ""), c.get("current_company", ""),
                    c.get("location_raw", ""), c.get("summary", ""),
                    c.get("openai_confidence", ""), False,
                    cid, [c.get("credly_url", "")],
                    enrichment_src,
                ))

                # ── candidates (main table) ─────────────────────────────────
                cur.execute("""
                    INSERT INTO candidates (
                        uid, name, first_name, last_name,
                        current_title, current_company, experience_years,
                        city, state, country, location_raw,
                        linkedin_url, linkedin_id,
                        verified_certifications, skills,
                        source, added_at, updated_at
                    ) VALUES (
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),now()
                    )
                    ON CONFLICT (uid) DO UPDATE SET
                        current_title           = CASE WHEN EXCLUDED.current_title != '' THEN EXCLUDED.current_title ELSE candidates.current_title END,
                        current_company         = CASE WHEN EXCLUDED.current_company != '' THEN EXCLUDED.current_company ELSE candidates.current_company END,
                        linkedin_url            = CASE WHEN EXCLUDED.linkedin_url != '' THEN EXCLUDED.linkedin_url ELSE candidates.linkedin_url END,
                        linkedin_id             = CASE WHEN EXCLUDED.linkedin_id != '' THEN EXCLUDED.linkedin_id ELSE candidates.linkedin_id END,
                        verified_certifications = EXCLUDED.verified_certifications,
                        skills                  = CASE WHEN cardinality(EXCLUDED.skills) > cardinality(candidates.skills)
                                                      THEN EXCLUDED.skills ELSE candidates.skills END,
                        updated_at              = now()
                """, (
                    cid, c.get("canonical_name", ""), c.get("first_name", ""), c.get("last_name", ""),
                    title, company, years_exp,
                    location_city, location_state, location_country, c.get("location_raw", ""),
                    linkedin_url, linkedin_id,
                    cert_codes, skills, "credly",
                ))

            batch_n += 1
            if batch_n >= BATCH_SIZE:
                conn.commit()
                print(f"  Committed batch | upserted so far: {stats['upserted'] + batch_n}")
                batch_n = 0

            stats["upserted"] += 1

        except Exception as e:
            conn.rollback()
            batch_n = 0
            print(f"  [ERROR] {c.get('canonical_name', cid)}: {e}")
            stats["errors"] += 1

    # final commit
    try:
        conn.commit()
    except Exception as e:
        print(f"  [ERROR] final commit: {e}")

    conn.close()
    set_watermark(JOB_NAME, run_start)
    print(f"\nDone. upserted={stats['upserted']} errors={stats['errors']}")
    print(f"  BrightData matched : {stats['bd_match']}")
    print(f"  SignalHire matched : {stats['sh_match']}")
    print(f"  No match (credly only): {stats['no_match']}")
    print("credly_earners + candidates tables updated")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--cert", default=None)
    args, _ = parser.parse_known_args()
    run(args.cert)
