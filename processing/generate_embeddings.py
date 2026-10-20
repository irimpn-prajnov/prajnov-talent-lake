"""
Gold → Embeddings pipeline.

Reads candidate profiles from Aurora, assembles profile text,
calls Bedrock Titan Embeddings v2, writes vectors to candidate_embedding.

Embedding types generated per candidate:
  full_profile  — name + title + company + summary + skills + certs + experience
  skills        — skills list only (for skill-focused matching)
  experience    — work history narrative (for domain/seniority matching)

Usage:
  python processing/generate_embeddings.py
  python processing/generate_embeddings.py --cert CIS-CSM
  python processing/generate_embeddings.py --type full_profile
  python processing/generate_embeddings.py --limit 100
"""
import argparse, json, os, sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
import psycopg2
import psycopg2.extras

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion"))

REGION         = "ap-south-1"
EMBED_MODEL    = "amazon.titan-embed-text-v2:0"
EMBED_DIM      = 1024   # Titan v2 native dimension
BATCH_SIZE     = 50     # candidates per commit


# ── Connections ───────────────────────────────────────────────────────────────

def get_db_url():
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    sm     = boto3.client("secretsmanager", region_name=REGION)
    secret = json.loads(sm.get_secret_value(SecretId="prajnov/aurora/credentials")["SecretString"])
    return f"postgresql://{secret['username']}:{secret['password']}@{secret['host']}:{secret['port']}/{secret['dbname']}"


def get_conn():
    return psycopg2.connect(get_db_url(), cursor_factory=psycopg2.extras.RealDictCursor)


def get_bedrock():
    return boto3.client("bedrock-runtime", region_name=REGION)


# ── Profile text assembly ─────────────────────────────────────────────────────

def build_full_profile_text(c, skills, experience, certs):
    """Assembles a rich natural language profile for embedding.
    Designed to match well against JD embeddings in semantic search."""
    parts = []

    # Identity
    title_co = " | ".join(filter(None, [c.get("current_title"), c.get("current_company")]))
    if title_co:
        parts.append(title_co)

    loc = " | ".join(filter(None, [c.get("city"), c.get("state"), c.get("country")]))
    if loc:
        parts.append(f"Location: {loc}")

    if c.get("years_experience"):
        parts.append(f"Years of experience: {c['years_experience']}")

    if c.get("summary"):
        parts.append(c["summary"])

    # Certifications
    if certs:
        cert_names = [f"{r['cert_code']} ({r['cert_name']})" for r in certs]
        parts.append("Certifications: " + ", ".join(cert_names))

    # Skills
    if skills:
        skill_names = [r["skill_name"] for r in sorted(skills, key=lambda x: -x.get("confidence", 0))[:40]]
        parts.append("Skills: " + ", ".join(skill_names))

    # Experience
    for exp in experience[:5]:
        line = " at ".join(filter(None, [exp.get("title"), exp.get("company")]))
        years = " - ".join(filter(None, [str(exp["start_year"]) if exp.get("start_year") else None,
                                          "present" if exp.get("is_current") else str(exp["end_year"]) if exp.get("end_year") else None]))
        if years:
            line = f"{line} ({years})"
        if line:
            parts.append(line)

    return "\n".join(parts)


def build_skills_text(skills):
    if not skills:
        return ""
    names = [r["skill_name"] for r in sorted(skills, key=lambda x: -x.get("confidence", 0))]
    return "Skills and expertise: " + ", ".join(names)


def build_experience_text(c, experience):
    if not experience:
        return ""
    parts = []
    if c.get("current_title"):
        parts.append(f"Current role: {c['current_title']} at {c.get('current_company','')}")
    for exp in experience:
        line = " at ".join(filter(None, [exp.get("title"), exp.get("company")]))
        if exp.get("start_year"):
            end = "present" if exp.get("is_current") else str(exp.get("end_year", ""))
            line = f"{line} ({exp['start_year']}–{end})"
        if line:
            parts.append(line)
    return "\n".join(parts)


# ── Bedrock embedding ─────────────────────────────────────────────────────────

def embed(bedrock, text):
    if not text or not text.strip():
        return None
    # Titan v2 max input is 8192 tokens — truncate to ~6000 chars to be safe
    text = text[:6000]
    resp = bedrock.invoke_model(
        modelId=EMBED_MODEL,
        body=json.dumps({"inputText": text, "dimensions": EMBED_DIM, "normalize": True}),
        contentType="application/json",
        accept="application/json",
    )
    return json.loads(resp["body"].read())["embedding"]


# ── Main ──────────────────────────────────────────────────────────────────────

def run(cert_filter, embed_types, limit):
    conn    = get_conn()
    bedrock = get_bedrock()
    now     = datetime.now(timezone.utc).isoformat()

    try:
        with conn.cursor() as cur:
            # Fetch candidates not yet embedded (or all if re-running)
            where = ""
            params = []
            if cert_filter:
                where  = "WHERE c.source_cert = %s"
                params = [cert_filter]

            cur.execute(f"""
                SELECT DISTINCT c.id, c.canonical_name, c.current_title, c.current_company,
                       c.years_experience, c.summary, c.source_cert,
                       l.city, l.state, l.country
                FROM candidate c
                LEFT JOIN candidate_location l ON l.candidate_id = c.id AND l.is_current = true
                {where}
                AND c.id NOT IN (
                    SELECT DISTINCT candidate_id FROM candidate_embedding
                    WHERE embedding_type = 'full_profile'
                )
                ORDER BY c.canonical_name
                {"LIMIT %s" if limit else ""}
            """, params + ([limit] if limit else []))

            candidates = cur.fetchall()
            print(f"Candidates to embed: {len(candidates)}\n")

            stats = {"embedded": 0, "skipped": 0, "errors": 0}

            for i, c in enumerate(candidates):
                cid = c["id"]
                try:
                    # Load related data
                    cur.execute("SELECT skill_name, confidence FROM candidate_skill WHERE candidate_id = %s", (cid,))
                    skills = cur.fetchall()

                    cur.execute("SELECT company, title, start_year, end_year, is_current FROM candidate_experience WHERE candidate_id = %s ORDER BY start_year DESC NULLS LAST", (cid,))
                    experience = cur.fetchall()

                    cur.execute("SELECT cert_code, cert_name FROM candidate_certification WHERE candidate_id = %s", (cid,))
                    certs = cur.fetchall()

                    vectors_written = 0
                    for embed_type in embed_types:
                        if embed_type == "full_profile":
                            text = build_full_profile_text(c, skills, experience, certs)
                        elif embed_type == "skills":
                            text = build_skills_text(skills)
                        elif embed_type == "experience":
                            text = build_experience_text(c, experience)
                        else:
                            continue

                        vector = embed(bedrock, text)
                        if not vector:
                            continue

                        cur.execute("""
                            INSERT INTO candidate_embedding
                                (candidate_id, embedding_type, model, vector, generated_at, source_text)
                            VALUES (%s, %s, %s, %s::vector, now(), %s)
                            ON CONFLICT DO NOTHING
                        """, (cid, embed_type, EMBED_MODEL, str(vector), text[:500]))
                        vectors_written += 1

                    stats["embedded"] += 1

                    if (i + 1) % BATCH_SIZE == 0:
                        conn.commit()
                        print(f"  [{i+1}/{len(candidates)}] committed batch | embedded={stats['embedded']}")

                    sk  = len(skills)
                    exp = len(experience)
                    print(f"  ✓ {c['canonical_name']:<35} | sk={sk} exp={exp} | {vectors_written} vectors")

                except Exception as e:
                    print(f"  [ERROR] {c.get('canonical_name', cid)}: {e}")
                    stats["errors"] += 1

            conn.commit()

    finally:
        conn.close()

    print(f"\n{'='*60}")
    print(f"Done. embedded={stats['embedded']} errors={stats['errors']}")
    print(f"\nAIR semantic query example:")
    print(f"  SELECT c.canonical_name, c.current_title, l.state,")
    print(f"         1 - (e.vector <=> $jd_vector) AS score")
    print(f"  FROM candidate_embedding e")
    print(f"  JOIN candidate c ON c.id = e.candidate_id")
    print(f"  JOIN candidate_location l ON l.candidate_id = c.id")
    print(f"  WHERE e.embedding_type = 'full_profile'")
    print(f"    AND l.country = 'US'")
    print(f"  ORDER BY score DESC LIMIT 20;")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cert",  default=None, help="Filter to one cert e.g. CIS-CSM")
    parser.add_argument("--type",  nargs="+", default=["full_profile", "skills", "experience"],
                        choices=["full_profile", "skills", "experience"])
    parser.add_argument("--limit", type=int, default=0)
    args, _ = parser.parse_known_args()

    run(args.cert, args.type, args.limit or None)
