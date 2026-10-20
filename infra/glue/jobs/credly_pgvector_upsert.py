"""
Glue Job: credly_pgvector_upsert
Source:   credly_earners table (pgvector DB)
Output:   candidates table (pgvector DB)

Upserts Credly-verified candidates into the main candidates table.
- verified_certifications[] = cert_codes from credly_earners (ground truth)
- cert_verification_status  = 'credly_verified'
- Dedup key: linkedin_id (primary), credly_username (fallback)
- Credly wins on certifications; existing profile data preserved if richer

Schedule: Weekly (Sunday 04:00 UTC) — after credly_gold_assemble
"""
import hashlib, json, os, sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "ingestion"))
from common import get_db, get_watermark, set_watermark, now_iso, linkedin_id_from_url

JOB_NAME       = "credly_pgvector_upsert"
EMBED_MODEL    = "all-MiniLM-L6-v2"
BATCH_SIZE     = 100


def make_uid(linkedin_id, credly_username):
    if linkedin_id:
        return f"credly_{linkedin_id}"
    return f"credly_{hashlib.md5(credly_username.encode()).hexdigest()[:16]}"


def build_embed_text(earner):
    cert_text  = " ".join(earner.get("cert_codes") or [])
    skill_text = ", ".join((earner.get("skills") or [])[:30])
    title      = earner.get("openai_title") or earner.get("sh_title") or ""
    company    = earner.get("openai_company") or earner.get("sh_company") or ""
    return " | ".join(filter(None, [
        earner.get("name", ""),
        title, company,
        f"Location: {earner.get('city','')} {earner.get('state','')}".strip(),
        f"Certifications: {cert_text}" if cert_text else "",
        f"Skills: {skill_text}" if skill_text else "",
    ]))


def fix_credly_status(conn):
    """Set cert_verification_status=credly_verified for credly candidates
    that have verified_certifications populated but status is still unverified."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE candidates
            SET cert_verification_status = 'credly_verified',
                cert_verified_at         = now(),
                updated_at               = now()
            WHERE source = 'credly'
              AND cert_verification_status = 'unverified'
              AND verified_certifications  != '{}'
        """)
        n = cur.rowcount
    conn.commit()
    print(f"  Fixed {n} credly candidates: unverified → credly_verified")
    return n


def run(full=False):
    from sentence_transformers import SentenceTransformer
    model     = SentenceTransformer(EMBED_MODEL)
    run_start = now_iso()

    conn  = get_db()
    stats = {"upserted": 0, "errors": 0}

    # Step 1: fix status inconsistency
    print("Step 1: fixing credly cert_verification_status...")
    fix_credly_status(conn)

    # Step 2: load earners — full run ignores watermark
    watermark = "1970-01-01T00:00:00+00:00" if full else get_watermark(JOB_NAME)
    print(f"Step 2: loading credly_earners | watermark={'epoch (full)' if full else watermark}")

    with conn.cursor() as cur:
        cur.execute("""
            SELECT credly_username, credly_url, name, first_name, last_name,
                   location_raw, city, state, country, badge_count, skills,
                   cert_codes, cert_details, linkedin_url, linkedin_id,
                   sh_title, sh_company, sh_email, sh_phone, sh_signalhire_url,
                   openai_title, openai_company, openai_location, openai_summary,
                   openai_confidence, sn_portal_verified, sn_portal_certs,
                   candidate_uid
            FROM credly_earners
            WHERE updated_at > %s
            ORDER BY credly_username
        """, (watermark,))
        earners = cur.fetchall()

    print(f"  Earners to upsert: {len(earners)}")

    for earner in earners:
        username = earner["credly_username"]
        name     = earner["name"] or ""
        lid      = earner["linkedin_id"] or linkedin_id_from_url(earner["linkedin_url"] or "")
        uid      = make_uid(lid, username)

        title   = earner["openai_title"] or earner["sh_title"] or ""
        company = earner["openai_company"] or earner["sh_company"] or ""
        cert_codes = earner["cert_codes"] or []

        # Portal-verified certs take highest trust
        portal_certs = []
        for pc in (earner["sn_portal_certs"] or []):
            if isinstance(pc, dict) and pc.get("abbr"):
                portal_certs.append(pc["abbr"].lower())

        verified_certs = sorted(set(cert_codes + portal_certs))
        cert_status    = "portal_verified" if portal_certs else "credly_verified"
        cert_sources   = json.dumps({c: cert_status for c in verified_certs})

        embed_text = build_embed_text(dict(earner))
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
                        cert_verification_status, cert_verified_at,
                        cert_verification_sources,
                        skills, companies, industries,
                        experience, experience_text, experience_years,
                        education, email, phone,
                        source, is_internal, embedding,
                        sn_portal_verified, sn_portal_certs,
                        added_at, updated_at
                    ) VALUES (
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,now(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        'credly',false,%s::vector,%s,%s,now(),now()
                    )
                    ON CONFLICT (uid) DO UPDATE SET
                        certifications           = EXCLUDED.certifications,
                        verified_certifications  = EXCLUDED.verified_certifications,
                        cert_verification_status = EXCLUDED.cert_verification_status,
                        cert_verified_at         = now(),
                        cert_verification_sources= EXCLUDED.cert_verification_sources,
                        current_title   = CASE WHEN EXCLUDED.current_title != ''
                                               AND candidates.current_title = ''
                                               THEN EXCLUDED.current_title
                                               ELSE candidates.current_title END,
                        current_company = CASE WHEN EXCLUDED.current_company != ''
                                               AND candidates.current_company = ''
                                               THEN EXCLUDED.current_company
                                               ELSE candidates.current_company END,
                        city  = CASE WHEN EXCLUDED.city != '' AND candidates.city = ''
                                     THEN EXCLUDED.city ELSE candidates.city END,
                        state = CASE WHEN EXCLUDED.state != '' AND candidates.state = ''
                                     THEN EXCLUDED.state ELSE candidates.state END,
                        skills = CASE WHEN cardinality(EXCLUDED.skills) > cardinality(COALESCE(candidates.skills,'{}'))
                                      THEN EXCLUDED.skills ELSE candidates.skills END,
                        embedding  = EXCLUDED.embedding,
                        source     = CASE WHEN candidates.source = 'credly' THEN 'credly'
                                          ELSE candidates.source || '+credly' END,
                        updated_at = now()
                """, (
                    uid, name,
                    earner["first_name"] or "", earner["last_name"] or "",
                    lid, earner["linkedin_url"] or "",
                    title, company,
                    earner["city"] or "", earner["state"] or "",
                    earner["country"] or "", earner["location_raw"] or "",
                    earner["openai_summary"] or "",
                    cert_codes, verified_certs,
                    cert_status, cert_sources,
                    earner["skills"] or [], [], [],
                    "[]", "", -1, "[]",
                    earner["sh_email"] or "", earner["sh_phone"] or "",
                    str(vec),                          # cast to vector in SQL
                    earner["sn_portal_verified"],
                    json.dumps(earner["sn_portal_certs"] or []),
                ))

                cur.execute("""
                    UPDATE credly_earners SET candidate_uid = %s, updated_at = now()
                    WHERE credly_username = %s
                """, (uid, username))

            stats["upserted"] += 1
            if stats["upserted"] % BATCH_SIZE == 0:
                conn.commit()
                print(f"  Committed {stats['upserted']} / {len(earners)}")

        except Exception as e:
            conn.rollback()
            print(f"  [ERROR] {name}: {e}")
            stats["errors"] += 1

    try:
        conn.commit()
    except Exception:
        pass
    conn.close()
    set_watermark(JOB_NAME, run_start)
    print(f"\nDone. upserted={stats['upserted']} errors={stats['errors']}")
    print("Verify: SELECT source, cert_verification_status, COUNT(*) FROM candidates WHERE source LIKE '%credly%' GROUP BY 1,2;")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--full", action="store_true", help="Ignore watermark, process all earners")
    args, _ = parser.parse_known_args()
    run(full=args.full)
