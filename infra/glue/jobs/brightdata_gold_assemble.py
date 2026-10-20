"""
Glue Job: brightdata_gold_assemble
Source:   silver/candidate/source=brightdata/
          silver/candidate_certification/source=brightdata/
          silver/candidate_skill/source=brightdata/
          silver/candidate_experience/source=brightdata/
          silver/candidate_education/source=brightdata/
Output:   candidates table (pgvector DB) — upsert with dedup against Credly

Entity resolution rules:
  1. Match by linkedin_id against existing candidates — if match found, MERGE
  2. Credly wins on verified_certifications (never overwrite with self-reported)
  3. BrightData wins on: experience, education, about (richer profile data)
  4. self_reported certs go into certifications[] only — NOT verified_certifications[]
  5. verified_certifications[] only updated if portal-verified certs exist

Schedule: Weekly (Sunday 03:30 UTC) — after brightdata_silver_normalize
"""
import json, os, sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "ingestion"))
from common import (
    parquet_read, date_path, now_iso,
    get_watermark, set_watermark, get_db,
    compute_dq_score, linkedin_id_from_url,
    normalize_certs, CERT_DISPLAY_NAMES,
)
import hashlib

JOB_NAME    = "brightdata_gold_assemble"
EMBED_MODEL = "all-MiniLM-L6-v2"


def load_silver(prefix):
    rows = parquet_read(prefix)
    print(f"  {prefix}: {len(rows)} records")
    return rows


def make_uid(linkedin_id, original_uid):
    if linkedin_id:
        return f"bd_{linkedin_id}"
    return f"bd_{hashlib.md5(original_uid.encode()).hexdigest()[:16]}"


def build_embed_text(c, skills, experience):
    cert_text = " ".join(jlist(c.get("certifications_clean")))
    skill_text = ", ".join(skills[:30])
    exp_parts  = [f"{e['title']} at {e['company']}" for e in experience[:5]
                  if e.get("title")]
    return " | ".join(filter(None, [
        c.get("canonical_name", ""),
        c.get("current_title", ""),
        c.get("current_company", ""),
        f"Location: {c.get('location_city','')} {c.get('location_state','')}".strip(),
        f"Certifications: {cert_text}" if cert_text else "",
        f"Skills: {skill_text}" if skill_text else "",
        " | ".join(exp_parts),
        (c.get("summary") or "")[:300],
    ]))


def jlist(val):
    """Parquet stores list fields as JSON strings — parse back to list."""
    if isinstance(val, list): return val
    if isinstance(val, str):
        try: return json.loads(val)
        except Exception: return []
    return []


def run(limit=None):
    from sentence_transformers import SentenceTransformer
    model     = SentenceTransformer(EMBED_MODEL)
    watermark = get_watermark(JOB_NAME)
    run_start = now_iso()
    print(f"brightdata_gold_assemble | watermark={watermark}")

    candidates = load_silver("silver/candidate/source=brightdata/")
    print(f"Silver candidates: {len(candidates)}")
    if not candidates:
        print("Nothing to assemble.")
        return

    certs_by  = defaultdict(list)
    skills_by = defaultdict(list)
    exp_by    = defaultdict(list)
    edu_by    = defaultdict(list)

    for r in load_silver("silver/candidate_certification/source=brightdata/"):
        certs_by[r["candidate_id"]].append(r)
    for r in load_silver("silver/candidate_skill/source=brightdata/"):
        skills_by[r["candidate_id"]].append(r)
    for r in load_silver("silver/candidate_experience/source=brightdata/"):
        exp_by[r["candidate_id"]].append(r)
    for r in load_silver("silver/candidate_education/source=brightdata/"):
        edu_by[r["candidate_id"]].append(r)

    conn  = get_db()
    stats = {"upserted": 0, "errors": 0}
    COMMIT_BATCH = 500

    for i, cand in enumerate(candidates):
        if limit and i >= limit: break
        cid = cand["candidate_id"]
        lid = cand.get("linkedin_id", "")
        uid = make_uid(lid, cand.get("original_uid", cid))

        skills     = sorted({r["skill_name"] for r in skills_by.get(cid, [])})
        experience = [{"company": e["company"], "title": e["title"],
                       "start_year": e.get("start_year"), "end_year": e.get("end_year"),
                       "is_current": e.get("is_current", False), "location": e.get("location", "")}
                      for e in exp_by.get(cid, [])]
        education  = [{"institution": e["institution"], "degree": e.get("degree", ""),
                       "field": e.get("field", ""), "end_year": e.get("end_year")}
                      for e in edu_by.get(cid, [])]

        raw_certs       = jlist(cand.get("certifications_clean"))
        portal_verified = [r["cert_code"] for r in certs_by.get(cid, [])
                           if r.get("verification_status") == "portal_verified"]

        exp_text   = " | ".join(filter(None, [f"{e['title']} at {e['company']}" for e in experience[:10]]))
        embed_text = build_embed_text(cand, skills, experience)
        vec        = model.encode([embed_text], normalize_embeddings=True)[0].tolist()

        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO candidates (
                        uid, name, first_name, last_name,
                        linkedin_id, linkedin_url,
                        current_title, current_company,
                        city, state, country, location_raw,
                        about, certifications, verified_certifications,
                        cert_verification_status, cert_verification_sources,
                        skills, companies, industries,
                        experience, experience_text, experience_years,
                        education, email, phone, visa_status,
                        source, is_internal, embedding,
                        sn_portal_verified, sn_portal_certs,
                        added_at, updated_at
                    ) VALUES (
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        'brightdata',false,%s,%s,%s,now(),now()
                    )
                    ON CONFLICT (uid) DO UPDATE SET
                        current_title   = CASE WHEN EXCLUDED.current_title != ''
                                               THEN EXCLUDED.current_title
                                               ELSE candidates.current_title END,
                        current_company = CASE WHEN EXCLUDED.current_company != ''
                                               THEN EXCLUDED.current_company
                                               ELSE candidates.current_company END,
                        city  = CASE WHEN EXCLUDED.city != '' AND candidates.city = ''
                                     THEN EXCLUDED.city ELSE candidates.city END,
                        state = CASE WHEN EXCLUDED.state != '' AND candidates.state = ''
                                     THEN EXCLUDED.state ELSE candidates.state END,
                        about = CASE WHEN length(EXCLUDED.about) > length(COALESCE(candidates.about,''))
                                     THEN EXCLUDED.about ELSE candidates.about END,
                        experience      = EXCLUDED.experience,
                        experience_text = EXCLUDED.experience_text,
                        experience_years= CASE WHEN EXCLUDED.experience_years > 0
                                               THEN EXCLUDED.experience_years
                                               ELSE candidates.experience_years END,
                        education       = EXCLUDED.education,
                        skills = CASE WHEN cardinality(EXCLUDED.skills) > cardinality(COALESCE(candidates.skills, ARRAY[]::text[])) THEN EXCLUDED.skills ELSE candidates.skills END,
                        certifications  = EXCLUDED.certifications,
                        verified_certifications = CASE
                            WHEN cardinality(EXCLUDED.verified_certifications) > 0
                            THEN (
                                SELECT array_agg(DISTINCT v)
                                FROM unnest(
                                    COALESCE(candidates.verified_certifications, ARRAY[]::text[])
                                    || EXCLUDED.verified_certifications
                                ) v
                            )
                            ELSE candidates.verified_certifications
                        END,
                        cert_verification_status = CASE
                            WHEN cardinality(EXCLUDED.verified_certifications) > 0
                                 AND candidates.cert_verification_status NOT IN ('credly_verified','portal_verified')
                            THEN 'portal_verified'
                            ELSE candidates.cert_verification_status
                        END,
                        embedding  = EXCLUDED.embedding,
                        source     = CASE WHEN candidates.source LIKE '%%brightdata%%'
                                          THEN candidates.source
                                          ELSE candidates.source || '+brightdata' END,
                        updated_at = now()
                """, (
                    uid, cand.get("canonical_name", ""),
                    cand.get("first_name", ""), cand.get("last_name", ""),
                    lid, cand.get("linkedin_url", ""),
                    cand.get("current_title", ""), cand.get("current_company", ""),
                    cand.get("location_city", ""), cand.get("location_state", ""),
                    cand.get("location_country", "US"), cand.get("location_raw", ""),
                    cand.get("summary", ""),
                    raw_certs, portal_verified,
                    "portal_verified" if portal_verified else "unverified",
                    json.dumps({code: "sn_portal" for code in portal_verified}),
                    skills, jlist(cand.get("companies")), jlist(cand.get("industries")),
                    json.dumps(experience), exp_text,
                    cand.get("years_experience") or -1,
                    json.dumps(education),
                    cand.get("email", ""), cand.get("phone", ""), cand.get("visa_status", ""),
                    str(vec),
                    cand.get("sn_portal_verified"),
                    json.dumps(jlist(cand.get("sn_portal_certs"))),
                ))
            conn.commit()
            stats["upserted"] += 1
            if stats["upserted"] % COMMIT_BATCH == 0:
                conn.commit()
                print(f"  {stats['upserted']} upserted...")
        except Exception as e:
            print(f"  [ERROR] {cand.get('canonical_name', uid)}: {e}")
            conn.rollback()
            stats["errors"] += 1

    conn.commit()  # final batch
    conn.close()
    set_watermark(JOB_NAME, run_start)
    print(f"\nDone. upserted={stats['upserted']} errors={stats['errors']}")
    print("Note: verified_certifications[] only populated for portal-verified candidates.")
    print("Run shared_cert_verify to verify remaining cert claims via Credly/portal.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0)
    args, _ = parser.parse_known_args()
    run(args.limit or None)
