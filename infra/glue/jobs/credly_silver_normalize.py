"""
Glue Job: credly_silver_normalize
Source:   bronze/discovery/credly/certs/{cert}/*.json  (one JSON per earner)
Output:   silver/candidate/source=credly/cert={CERT}/
          silver/candidate_certification/source=credly/cert={CERT}/
          silver/candidate_skill/source=credly/cert={CERT}/
          silver/candidate_experience/source=credly/cert={CERT}/
          silver/candidate_education/source=credly/cert={CERT}/

Each output is a single Parquet file per cert per run (snappy compressed).
OpenAI web_search_preview enriches each earner with LinkedIn URL, title,
company, work history, education, skills.

Dedup: checks silver Parquet + pgvector credly_earners before any API call.
Schedule: Weekly (Sunday 02:00 UTC) — after credly_bronze_ingest
"""
import argparse, json, os, sys, time, uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "ingestion"))
from common import (
    BRONZE_BUCKET, s3_list, s3_read, parquet_write, parquet_read,
    date_path, now_iso, get_watermark, set_watermark,
    normalize_cert, parse_location, parse_year, CERT_DISPLAY_NAMES, get_db,
)
from openai import OpenAI

JOB_NAME = "credly_silver_normalize"

OPENAI_PROMPT = """\
Search LinkedIn for "{name}" who holds a ServiceNow {cert_name} ({cert_code}) certification.
Try: site:linkedin.com/in "{name}" ServiceNow

Return ONLY valid JSON (no markdown):
{{
  "linkedin_url": "",
  "location": "",
  "current_title": "",
  "current_company": "",
  "summary": "",
  "years_experience": null,
  "open_to_work": false,
  "languages": [],
  "certifications": [],
  "work_history": [{{"title":"","company":"","start_year":null,"end_year":null,"is_current":false,"location":""}}],
  "education": [{{"institution":"","degree":"","field":"","end_year":null}}],
  "skills": [],
  "confidence": "high|medium|low"
}}
Rules:
- linkedin_url must be a real linkedin.com/in/... URL or empty string
- certifications: list all certifications visible on their profile
- work_history: most recent first, include all roles visible
- skills: include all skills listed on their profile
- open_to_work: true only if explicitly shown on profile
- confidence: high=certain match, medium=likely match, low=uncertain
"""


SN_KEYWORDS = {
    "servicenow", "service-now", "snow", "cis-csm", "cis-itsm", "cis-vr",
    "itsm", "itom", "itam", "csm", "hrsd", "fsm", "spm", "ppm",
}

_openai_dead = False  # circuit breaker: set True on quota exhaustion


def is_servicenow_relevant(ai: dict, bronze: dict) -> bool:
    """Return False if profile was found but has no ServiceNow relevance."""
    # No profile found — keep (we don't know enough to discard)
    if not ai.get("linkedin_url") and not ai.get("current_title"):
        return True
    # Low confidence match — keep, can't be sure it's wrong person
    if ai.get("confidence") == "low":
        return True
    text = " ".join([
        ai.get("current_title", ""),
        ai.get("current_company", ""),
        ai.get("summary", ""),
        " ".join(ai.get("skills") or []),
        " ".join(str(c) for c in (ai.get("certifications") or [])),
        " ".join(j.get("company", "") + " " + j.get("title", "") for j in (ai.get("work_history") or [])),
        " ".join(bronze.get("skills") or []),
    ]).lower()
    return any(kw in text for kw in SN_KEYWORDS)


def call_openai(client, name, cert_code, cert_name):
    global _openai_dead
    if _openai_dead:
        return {}
    try:
        resp = client.responses.create(
            model="gpt-4o-mini",
            tools=[{"type": "web_search_preview"}],
            input=OPENAI_PROMPT.format(name=name, cert_code=cert_code, cert_name=cert_name),
        )
        text  = resp.output_text.strip()
        start = text.find("{")
        end   = text.rfind("}") + 1
        if start >= 0 and end > start:
            raw = text[start:end]
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                # try to salvage partial JSON — extract just the safe scalar fields
                result = {}
                for field in ("linkedin_url", "location", "current_title", "current_company", "summary", "confidence"):
                    m = __import__('re').search(rf'"{field}"\s*:\s*"([^"]*)"', raw)
                    if m:
                        result[field] = m.group(1)
                return result
    except Exception as e:
        print(f"    [OpenAI ERROR] {name}: {e}")
        if "credit_balance_exhausted" in str(e) or "insufficient_quota" in str(e):
            print("    [OpenAI] Quota exhausted — skipping all remaining API calls")
            _openai_dead = True
    return {}


def build_done_set() -> set:
    """
    Global dedup set built BEFORE any API call.
    Layer 1: credly_usernames in silver Parquet (any cert partition).
    Layer 2: credly_usernames already in pgvector credly_earners table.
    """
    seen = set()

    for rec in parquet_read("silver/candidate/source=credly/"):
        u = rec.get("credly_username", "")
        # only skip if already enriched — pending_enrichment means OpenAI failed, retry
        if u and rec.get("discovery_status") not in ("pending_enrichment", "", None):
            seen.add(u)
    print(f"  [dedup] silver parquet:  {len(seen)} usernames (enriched only, pending retried)")

    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT credly_username FROM credly_earners 
                WHERE credly_username IS NOT NULL
                AND enrichment_source != 'credly_only'
            """)
            db_count = 0
            for row in cur.fetchall():
                seen.add(row["credly_username"])
                db_count += 1
        conn.close()
        print(f"  [dedup] pgvector DB:     {db_count} usernames")
    except Exception as e:
        print(f"  [dedup] DB check skipped: {e}")

    print(f"  [dedup] Total unique done: {len(seen)}")
    return seen


def run(certs, limit, openai_key):
    client   = OpenAI(api_key=openai_key)
    dp       = date_path()
    run_id   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    print("Building dedup set...")
    done = build_done_set()
    print(f"  {len(done)} usernames already processed\n")

    # ── Resolve ALL pending records across ALL certs before any API call ────
    all_pending = []       # list of (cert, bronze_rec)
    seen_this_run = set()  # catch cross-cert dupes on first run

    from concurrent.futures import ThreadPoolExecutor, as_completed
    from common import get_s3, BRONZE_BUCKET

    def fetch_key(key):
        try:
            body = get_s3().get_object(Bucket=BRONZE_BUCKET, Key=key)["Body"].read().decode()
            if key.endswith(".jsonl"):
                return [json.loads(l) for l in body.splitlines() if l.strip()]
            return [json.loads(body)]
        except Exception as e:
            print(f"  [WARN] fetch_key {key}: {e}")
            return []

    for cert in certs:
        slug = cert.lower().replace("-", "_")
        keys = s3_list(f"bronze/discovery/credly/certs/{slug}/") + \
               s3_list(f"bronze/discovery/credly/certs/{slug}/", ext=".jsonl")
        print(f"  {cert}: fetching {len(keys)} bronze files in parallel...")
        with ThreadPoolExecutor(max_workers=20) as ex:
            futures = {ex.submit(fetch_key, k): k for k in keys}
            for fut in as_completed(futures):
                for rec in fut.result():
                    username = rec.get("credly_username", "")
                    if not username or not rec.get("name"):
                        continue
                    if username in done or username in seen_this_run:
                        continue
                    all_pending.append((cert, rec))
                    seen_this_run.add(username)

    if limit:
        all_pending = all_pending[:limit]

    print(f"Total pending across all certs: {len(all_pending)}\n")

    # ── Process per cert, buffer records, write one Parquet per cert ────────
    for cert in certs:
        cert_name = CERT_DISPLAY_NAMES.get(cert.lower(), cert)
        pending   = [(k, rec) for (k, rec) in all_pending if k == cert]
        if not pending:
            print(f"=== {cert}: nothing pending ===")
            continue
        print(f"=== {cert}: {len(pending)} pending ===\n")

        buf_candidate    = []
        buf_cert         = []
        buf_skill        = []
        buf_experience   = []
        buf_education    = []

        for i, (_, bronze) in enumerate(pending):
            username = bronze.get("credly_username", "")
            name     = bronze.get("name", "")

            print(f"  [{i+1}/{len(pending)}] {name}")
            ai  = call_openai(client, name, cert, cert_name)
            cid = str(uuid.uuid4())
            time.sleep(0.3)

            if not is_servicenow_relevant(ai, bronze):
                print(f"         ✗ SKIP [not SN relevant]")
                done.add(username)
                continue
            loc_raw = ai.get("location") or bronze.get("location") or ""
            city, state, country = parse_location(loc_raw)

            # ── candidate ──
            buf_candidate.append({
                "candidate_id":      cid,
                "canonical_name":    name,
                "first_name":        bronze.get("first_name", ""),
                "last_name":         bronze.get("last_name", ""),
                "current_title":     ai.get("current_title", ""),
                "current_company":   ai.get("current_company", ""),
                "years_experience":  ai.get("years_experience"),
                "summary":           ai.get("summary", ""),
                "open_to_work":      ai.get("open_to_work", False),
                "languages":         json.dumps(ai.get("languages") or []),
                "ai_certifications": json.dumps(ai.get("certifications") or []),
                "location_city":     city,
                "location_state":    state,
                "location_country":  country,
                "location_raw":      loc_raw,
                "credly_url":        bronze.get("credly_url", ""),
                "credly_username":   username,
                "credly_id":         bronze.get("credly_id", ""),
                "badge_count":       bronze.get("badge_count", 0),
                "linkedin_url":      ai.get("linkedin_url", ""),
                "discovery_status":  "enriched" if ai.get("linkedin_url") else ("pending_enrichment" if _openai_dead else "discovered"),
                "enriched_by":       "openai_web_search",
                "openai_confidence": ai.get("confidence", ""),
                "source":            "credly",
                "source_cert":       cert,
                "created_at":        now_iso(),
            })

            # ── certifications ──
            badges = bronze.get("badges") or [{"name": CERT_DISPLAY_NAMES.get(cert.lower(), cert), "date": "", "url": bronze.get("credly_url", "")}]
            for badge in badges:
                buf_cert.append({
                    "id":                  str(uuid.uuid4()),
                    "candidate_id":        cid,
                    "cert_code":           cert.lower(),
                    "cert_name":           badge.get("name", CERT_DISPLAY_NAMES.get(cert.lower(), cert)),
                    "issuer":              "ServiceNow",
                    "issued_date":         (badge.get("date") or "")[:10],
                    "verification_url":    badge.get("url", bronze.get("credly_url", "")),
                    "verification_status": "credly_verified",
                    "source":              "credly",
                    "credly_username":     username,
                    "last_verified":       now_iso(),
                })
            # additional certs found by OpenAI on LinkedIn profile
            for ai_cert in (ai.get("certifications") or []):
                cert_name_ai = ai_cert if isinstance(ai_cert, str) else ai_cert.get("name", "")
                if not cert_name_ai:
                    continue
                buf_cert.append({
                    "id":                  str(uuid.uuid4()),
                    "candidate_id":        cid,
                    "cert_code":           normalize_cert(cert_name_ai) or cert_name_ai.lower()[:32],
                    "cert_name":           cert_name_ai,
                    "issuer":              ai_cert.get("issuer", "") if isinstance(ai_cert, dict) else "",
                    "issued_date":         "",
                    "verification_url":    "",
                    "verification_status": "linkedin_openai",
                    "source":              "linkedin_openai",
                    "credly_username":     username,
                    "last_verified":       now_iso(),
                })

            # ── skills ──
            skill_map = {}
            def add_skill(sname, src, conf):
                k = sname.lower().strip()
                if not k: return
                if k not in skill_map:
                    skill_map[k] = {"name": sname, "sources": [src], "confidence": conf}
                else:
                    if src not in skill_map[k]["sources"]: skill_map[k]["sources"].append(src)
                    skill_map[k]["confidence"] = max(skill_map[k]["confidence"], conf)
            for s in (bronze.get("skills") or []): add_skill(s, "credly", 0.90)
            for s in (ai.get("skills") or []):     add_skill(s, "linkedin_openai", 0.75)
            for sd in skill_map.values():
                buf_skill.append({
                    "id": str(uuid.uuid4()), "candidate_id": cid,
                    "skill_name": sd["name"], "raw_name": sd["name"],
                    "sources": json.dumps(sd["sources"]), "source": sd["sources"][0],
                    "confidence": round(sd["confidence"], 2), "is_inferred": False,
                    "source_cert": cert, "created_at": now_iso(),
                })

            # ── experience ──
            seen_exp = set()
            for job in (ai.get("work_history") or []):
                company = (job.get("company") or "").strip()
                title   = (job.get("title") or "").strip()
                if not company and not title: continue
                if (company.lower(), title.lower()) in seen_exp: continue
                seen_exp.add((company.lower(), title.lower()))
                buf_experience.append({
                    "id": str(uuid.uuid4()), "candidate_id": cid,
                    "company": company, "title": title,
                    "start_year": job.get("start_year"), "end_year": job.get("end_year"),
                    "is_current": job.get("is_current", False),
                    "location": job.get("location", ""),
                    "source": "linkedin_openai", "confidence": 0.75,
                    "source_cert": cert, "created_at": now_iso(),
                })

            # ── education ──
            seen_edu = set()
            for edu in (ai.get("education") or []):
                inst = (edu.get("institution") or "").strip()
                if not inst: continue
                if (inst.lower(), (edu.get("degree") or "").lower()) in seen_edu: continue
                seen_edu.add((inst.lower(), (edu.get("degree") or "").lower()))
                buf_education.append({
                    "id": str(uuid.uuid4()), "candidate_id": cid,
                    "institution": inst, "degree": edu.get("degree", ""),
                    "field": edu.get("field", ""), "end_year": edu.get("end_year"),
                    "source": "linkedin_openai", "source_cert": cert, "created_at": now_iso(),
                })

            done.add(username)
            li  = "✓ LI" if ai.get("linkedin_url") else "✗ LI"
            loc = (ai.get("location") or "—")[:28]
            print(f"         {li} [{ai.get('confidence','?'):<6}] | {loc}")

        # ── Write one Parquet file per table per cert ────────────────────────
        prefix = f"silver/candidate/source=credly/cert={cert}/{dp}"
        parquet_write(prefix, buf_candidate, run_id)
        parquet_write(f"silver/candidate_certification/source=credly/cert={cert}/{dp}", buf_cert, run_id)
        parquet_write(f"silver/candidate_skill/source=credly/cert={cert}/{dp}",         buf_skill, run_id)
        parquet_write(f"silver/candidate_experience/source=credly/cert={cert}/{dp}",    buf_experience, run_id)
        parquet_write(f"silver/candidate_education/source=credly/cert={cert}/{dp}",     buf_education, run_id)
        print(f"  Wrote Parquet: {len(buf_candidate)} candidates, {len(buf_cert)} certs, "
              f"{len(buf_skill)} skills, {len(buf_experience)} exp, {len(buf_education)} edu\n")

    set_watermark(JOB_NAME, now_iso())
    print("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--certs",   nargs="+", default=["CIS-VR", "CIS-CSM"])
    parser.add_argument("--limit",   type=int,  default=0)
    parser.add_argument("--api-key", default=os.getenv("OPENAI_API_KEY", ""))
    args, _ = parser.parse_known_args()
    if not args.api_key:
        print("ERROR: --api-key or OPENAI_API_KEY required")
        sys.exit(1)
    run(args.certs, args.limit or None, args.api_key)
