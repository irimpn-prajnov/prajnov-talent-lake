"""
Bronze → Silver enrichment pipeline using OpenAI web search.

Per candidate (from bronze Credly):
  - Searches LinkedIn via OpenAI web_search_preview
  - Extracts: linkedin_url, location, title, company, summary,
              years_experience, work_history, education, skills

Writes 5 silver tables:
  silver/candidate/                  — identity + location + all URLs
  silver/candidate_certification/    — from bronze badges[] (verified via Credly)
  silver/candidate_skill/            — merged from Credly badges + OpenAI LinkedIn
  silver/candidate_experience/       — work history from OpenAI
  silver/candidate_education/        — education from OpenAI

Usage:
  python processing/enrich_linkedin.py --certs CIS-VR CIS-CSM
  python processing/enrich_linkedin.py --certs CIS-VR --limit 10
"""
import argparse, json, os, sys, time, uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3
from openai import OpenAI

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion"))
from common import BRONZE_BUCKET, s3_put

BUCKET = BRONZE_BUCKET

CERT_NAMES = {
    "CIS-CSM":  "Customer Service Management",
    "CIS-VR":   "Vulnerability Response",
    "CIS-ITSM": "IT Service Management",
    "CIS-HR":   "Human Resources",
    "CIS-ITAM": "IT Asset Management",
    "CIS-FSM":  "Field Service Management",
    "CIS-PPM":  "Project Portfolio Management",
    "CIS-RC":   "Risk and Compliance",
    "CIS-SAM":  "Software Asset Management",
    "CIS-SIR":  "Security Incident Response",
    "CIS-SPM":  "Strategic Portfolio Management",
    "CIS-HAM":  "Hardware Asset Management",
    "CIS-ITOM": "IT Operations Management",
    "CIS-WSD":  "Workplace Service Delivery",
    "CIS-HRSD": "HR Service Delivery",
}

COUNTRY_CODES = {
    "united states": "US", "united kingdom": "GB", "canada": "CA",
    "australia": "AU", "india": "IN", "germany": "DE", "france": "FR",
    "netherlands": "NL", "singapore": "SG", "ireland": "IE",
    "new zealand": "NZ", "south africa": "ZA",
}

US_STATES_LOWER = {
    "alabama","alaska","arizona","arkansas","california","colorado","connecticut",
    "delaware","florida","georgia","hawaii","idaho","illinois","indiana","iowa",
    "kansas","kentucky","louisiana","maine","maryland","massachusetts","michigan",
    "minnesota","mississippi","missouri","montana","nebraska","nevada",
    "new hampshire","new jersey","new mexico","new york","north carolina",
    "north dakota","ohio","oklahoma","oregon","pennsylvania","rhode island",
    "south carolina","south dakota","tennessee","texas","utah","vermont",
    "virginia","washington","west virginia","wisconsin","wyoming",
    "al","ak","az","ar","ca","co","ct","de","fl","ga","hi","id","il","in",
    "ia","ks","ky","la","me","md","ma","mi","mn","ms","mo","mt","ne","nv",
    "nh","nj","nm","ny","nc","nd","oh","ok","or","pa","ri","sc","sd","tn",
    "tx","ut","vt","va","wa","wv","wi","wy","dc",
}

OPENAI_PROMPT = """\
Search LinkedIn for "{name}" who holds a ServiceNow {cert_name} ({cert_code}) certification.

Try these searches in order:
1. site:linkedin.com/in "{name}" ServiceNow
2. "{name}" ServiceNow {cert_code} linkedin

Return ONLY a valid JSON object — no markdown fences, no explanation:
{{
  "linkedin_url": "",
  "location": "",
  "current_title": "",
  "current_company": "",
  "summary": "",
  "years_experience": null,
  "work_history": [
    {{"title": "", "company": "", "start_year": null, "end_year": null, "is_current": false, "location": ""}}
  ],
  "education": [
    {{"institution": "", "degree": "", "field": "", "end_year": null}}
  ],
  "skills": [],
  "confidence": "high|medium|low"
}}

Rules:
- linkedin_url: must be a real linkedin.com/in/... URL or empty string
- work_history: all roles visible on the profile, most recent first, empty array if none found
- education: all education entries, empty array if none found
- skills: list of skill name strings from the profile, empty array if none found
- years_experience: total years as integer or null
- confidence: high = found exact match, medium = likely match, low = not found
"""


# ── S3 ────────────────────────────────────────────────────────────────────────

def s3_client():
    from botocore.session import get_session
    bs = get_session()
    bs.set_config_variable("profile", os.getenv("AWS_PROFILE", "prajnov"))
    return boto3.Session(botocore_session=bs).client("s3", region_name="ap-south-1")


def list_keys(prefix):
    pag = s3_client().get_paginator("list_objects_v2")
    return [
        o["Key"] for page in pag.paginate(Bucket=BUCKET, Prefix=prefix)
        for o in page.get("Contents", [])
        if o["Key"].endswith(".json") and "_manifests" not in o["Key"]
    ]


def read_json(key):
    return json.loads(s3_client().get_object(Bucket=BUCKET, Key=key)["Body"].read())


def already_processed(certs):
    seen = set()
    for cert in certs:
        for key in list_keys(f"silver/candidate/source_cert={cert}/"):
            fname = key.split("/")[-1].replace(".json", "")
            if "__" in fname:
                seen.add(fname.split("__")[0])
            else:
                try:
                    un = read_json(key).get("credly_username", "")
                    if un:
                        seen.add(un)
                except Exception:
                    pass
    return seen


# ── OpenAI ────────────────────────────────────────────────────────────────────

def call_openai(client, name, cert_code, cert_name):
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
            return json.loads(text[start:end])
    except Exception as e:
        print(f"    [OpenAI ERROR] {name}: {e}")
    return {}


# ── Location ──────────────────────────────────────────────────────────────────

def parse_location(raw):
    if not raw:
        return "", "", ""
    parts   = [p.strip() for p in raw.split(",")]
    country = ""
    if parts and parts[-1].lower() in COUNTRY_CODES:
        country = COUNTRY_CODES[parts[-1].lower()]
        parts   = parts[:-1]
    elif any(p.lower() in US_STATES_LOWER for p in parts):
        country = "US"
    city  = parts[0] if parts else ""
    state = parts[1] if len(parts) > 1 else ""
    return city, state, country


# ── Silver writers ────────────────────────────────────────────────────────────

def write_candidate(cert, date_path, username, candidate_id, bronze, ai):
    loc_raw = ai.get("location") or bronze.get("location") or ""
    city, state, country = parse_location(loc_raw)
    linkedin_url = ai.get("linkedin_url") or ""
    now = datetime.now(timezone.utc).isoformat()

    rec = {
        "candidate_id":      candidate_id,
        "canonical_name":    bronze.get("name", ""),
        "first_name":        bronze.get("first_name", ""),
        "last_name":         bronze.get("last_name", ""),
        "current_title":     ai.get("current_title", ""),
        "current_company":   ai.get("current_company", ""),
        "years_experience":  ai.get("years_experience"),
        "summary":           ai.get("summary", ""),
        "location_city":     city,
        "location_state":    state,
        "location_country":  country,
        "location_raw":      loc_raw,
        "credly_url":        bronze.get("credly_url", ""),
        "credly_username":   username,
        "credly_id":         bronze.get("credly_id", ""),
        "badge_count":       bronze.get("badge_count", 0),
        "linkedin_url":      linkedin_url,
        "signalhire_uid":    "",
        "signalhire_url":    "",
        "discovery_status":  "enriched" if linkedin_url else "discovered",
        "enriched_by":       "openai_web_search",
        "openai_confidence": ai.get("confidence", ""),
        "source_cert":       cert,
        "created_at":        now,
        "updated_at":        now,
    }
    s3_put(f"silver/candidate/source_cert={cert}/{date_path}/{username}__{candidate_id}.json", rec)
    return rec


def write_certifications(cert, date_path, username, candidate_id, bronze):
    badges = bronze.get("badges") or [
        {"name": CERT_NAMES.get(cert, cert), "date": "", "url": bronze.get("credly_url", "")}
    ]
    now = datetime.now(timezone.utc).isoformat()
    for badge in badges:
        issued_raw  = badge.get("date", "")
        issued_date = issued_raw[:10] if issued_raw else ""
        rec = {
            "id":                  str(uuid.uuid4()),
            "candidate_id":        candidate_id,
            "cert_code":           cert,
            "cert_name":           badge.get("name", CERT_NAMES.get(cert, cert)),
            "issuer":              "ServiceNow",
            "credential_id":       "",
            "issued_date":         issued_date,
            "expiration_date":     "",
            "verification_url":    badge.get("url", bronze.get("credly_url", "")),
            "verification_status": "verified",
            "source":              "credly",
            "bronze_ref":          bronze.get("_meta", {}).get("source_url", ""),
            "credly_username":     username,
            "last_verified":       now,
        }
        s3_put(
            f"silver/candidate_certification/cert_code={cert}/{date_path}/{username}__{rec['id']}.json",
            rec,
        )


def write_skills(cert, date_path, username, candidate_id, bronze, ai):
    """Merge skills from Credly (high confidence) and OpenAI/LinkedIn."""
    now       = datetime.now(timezone.utc).isoformat()
    skill_map = {}

    def add(name, source, confidence):
        k = name.lower().strip()
        if not k:
            return
        if k not in skill_map:
            skill_map[k] = {"name": name, "sources": [source], "confidence": confidence}
        else:
            if source not in skill_map[k]["sources"]:
                skill_map[k]["sources"].append(source)
            skill_map[k]["confidence"] = max(skill_map[k]["confidence"], confidence)

    for s in (bronze.get("skills") or []):
        add(s, "credly", 0.90)
    for s in (ai.get("skills") or []):
        add(s, "linkedin_openai", 0.75)

    for skill_data in skill_map.values():
        rec = {
            "id":           str(uuid.uuid4()),
            "candidate_id": candidate_id,
            "skill_name":   skill_data["name"],
            "raw_name":     skill_data["name"],
            "years":        None,
            "level":        "",
            "is_inferred":  False,
            "sources":      skill_data["sources"],
            "source":       skill_data["sources"][0],
            "confidence":   round(skill_data["confidence"], 2),
            "evidence":     json.dumps({"badge_count": bronze.get("badge_count", 0)}),
            "source_cert":  cert,
            "created_at":   now,
        }
        s3_put(
            f"silver/candidate_skill/source_cert={cert}/{date_path}/{username}__{rec['id']}.json",
            rec,
        )
    return len(skill_map)


def write_experience(cert, date_path, username, candidate_id, ai):
    work_history = ai.get("work_history") or []
    now  = datetime.now(timezone.utc).isoformat()
    seen = set()
    n    = 0
    for job in work_history:
        company = (job.get("company") or "").strip()
        title   = (job.get("title") or "").strip()
        if not company and not title:
            continue
        key = (company.lower(), title.lower())
        if key in seen:
            continue
        seen.add(key)
        rec = {
            "id":           str(uuid.uuid4()),
            "candidate_id": candidate_id,
            "company":      company,
            "title":        title,
            "start_year":   job.get("start_year"),
            "end_year":     job.get("end_year"),
            "is_current":   job.get("is_current", False),
            "location":     job.get("location", ""),
            "description":  job.get("description", ""),
            "source":       "linkedin_openai",
            "confidence":   0.75,
            "source_cert":  cert,
            "created_at":   now,
        }
        s3_put(
            f"silver/candidate_experience/source_cert={cert}/{date_path}/{username}__{rec['id']}.json",
            rec,
        )
        n += 1
    return n


def write_education(cert, date_path, username, candidate_id, ai):
    education = ai.get("education") or []
    now  = datetime.now(timezone.utc).isoformat()
    seen = set()
    n    = 0
    for edu in education:
        institution = (edu.get("institution") or "").strip()
        if not institution:
            continue
        key = (institution.lower(), (edu.get("degree") or "").lower())
        if key in seen:
            continue
        seen.add(key)
        rec = {
            "id":           str(uuid.uuid4()),
            "candidate_id": candidate_id,
            "institution":  institution,
            "degree":       edu.get("degree", ""),
            "field":        edu.get("field", ""),
            "start_year":   edu.get("start_year"),
            "end_year":     edu.get("end_year"),
            "source":       "linkedin_openai",
            "source_cert":  cert,
            "created_at":   now,
        }
        s3_put(
            f"silver/candidate_education/source_cert={cert}/{date_path}/{username}__{rec['id']}.json",
            rec,
        )
        n += 1
    return n


# ── Main ──────────────────────────────────────────────────────────────────────

def run(certs, limit, openai_key):
    client    = OpenAI(api_key=openai_key)
    date_path = datetime.now(timezone.utc).strftime("%Y/%m/%d")

    print("Checking already processed...")
    done = already_processed(certs)
    print(f"  {len(done)} already in silver\n")

    total_new = 0

    for cert in certs:
        cert_slug = cert.lower().replace("-", "_")
        cert_name = CERT_NAMES.get(cert, cert)
        all_keys  = list_keys(f"bronze/discovery/credly/certs/{cert_slug}/")
        pending   = []
        for key in all_keys:
            try:
                bronze = read_json(key)
                if bronze.get("credly_username") not in done:
                    pending.append((key, bronze))
            except Exception:
                pass

        print(f"=== {cert}: {len(all_keys)} bronze | {len(done)} done | {len(pending)} pending ===\n")

        for i, (key, bronze) in enumerate(pending):
            if limit and i >= limit:
                break

            username = bronze.get("credly_username", "")
            name     = bronze.get("name", "")
            if not name:
                continue

            print(f"  [{i+1}/{len(pending)}] {name}")

            ai           = call_openai(client, name, cert, cert_name)
            candidate_id = str(uuid.uuid4())
            time.sleep(0.3)

            silver  = write_candidate(cert, date_path, username, candidate_id, bronze, ai)
            write_certifications(cert, date_path, username, candidate_id, bronze)
            n_sk    = write_skills(cert, date_path, username, candidate_id, bronze, ai)
            n_exp   = write_experience(cert, date_path, username, candidate_id, ai)
            n_edu   = write_education(cert, date_path, username, candidate_id, ai)

            done.add(username)
            total_new += 1

            li  = "✓ LI" if silver.get("linkedin_url") else "✗ LI"
            loc = (silver.get("location_raw") or "—")[:30]
            ttl = (silver.get("current_title") or "")[:35]
            conf = silver.get("openai_confidence", "")
            print(f"         {li} [{conf:<6}] | {loc:<30} | sk={n_sk} exp={n_exp} edu={n_edu} | {ttl}")

        print(f"\n  → {len(pending)} pending processed for {cert}\n")

    print(f"{'='*60}")
    print(f"Done. {total_new} new candidates written to silver.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--certs",   nargs="+", default=["CIS-VR", "CIS-CSM"])
    parser.add_argument("--limit",   type=int,  default=0)
    parser.add_argument("--api-key", default=os.getenv("OPENAI_API_KEY", ""))
    args, _ = parser.parse_known_args()

    if not args.api_key:
        print("ERROR: OpenAI API key required via --api-key or OPENAI_API_KEY")
        sys.exit(1)

    run(args.certs, args.limit, args.api_key)
