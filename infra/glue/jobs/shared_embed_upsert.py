"""
Glue Job: shared-embed-upsert
Reads candidates updated since watermark, assembles profile text,
calls Bedrock Titan Embeddings v2 (1024-dim), writes to embedding_titan column.

Runs as the final step in both pipelines:
  Credly:     ... → GoldMerge    → EmbedUpsert
  BrightData: ... → GoldUpsert   → EmbedUpsert

Profile text assembled from (in order):
  {current_title} at {current_company}
  Location: {city}, {state}
  Experience: {experience_years} years
  Certifications: {verified_certifications joined}
  Skills: {skills joined}
  {about}
  {experience_text}
"""
import json, logging, os, sys
from datetime import datetime, timezone

import boto3, psycopg2, psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _arg(name, default=""):
    key = f"--{name}"
    try:
        idx = sys.argv.index(key)
        return sys.argv[idx + 1]
    except (ValueError, IndexError):
        return os.getenv(name, default)


BUCKET    = _arg("BUCKET",            "prajnov-talent-lake")
REGION    = _arg("AWS_REGION",        "ap-south-1")
DB_HOST   = _arg("PGVECTOR_HOST",     "ai-recruiter.c7q8sg00wqte.ap-south-1.rds.amazonaws.com")
DB_NAME   = _arg("PGVECTOR_DB",       "recruiter")
DB_USER   = _arg("PGVECTOR_USER",     "prajnov")
DB_PASS   = _arg("PGVECTOR_PASSWORD", "")

EMBED_MODEL  = "amazon.titan-embed-text-v2:0"
EMBED_DIM    = 1024
BATCH_SIZE   = 50
STATE_KEY    = "glue-state/shared_embed_upsert/last_run.json"


def db():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


def s3():
    return boto3.client("s3", region_name=REGION)


def bedrock():
    return boto3.client("bedrock-runtime", region_name=REGION)


def get_watermark():
    try:
        body = s3().get_object(Bucket=BUCKET, Key=STATE_KEY)["Body"].read()
        return json.loads(body)["last_run"]
    except Exception:
        return "1970-01-01T00:00:00+00:00"


def set_watermark(ts):
    s3().put_object(
        Bucket=BUCKET, Key=STATE_KEY,
        Body=json.dumps({"last_run": ts}).encode(),
        ContentType="application/json",
    )


def build_profile_text(r):
    parts = []

    role = " at ".join(filter(None, [r.get("current_title"), r.get("current_company")]))
    if role:
        parts.append(role)

    if r.get("headline"):
        parts.append(r["headline"])

    loc = ", ".join(filter(None, [r.get("city"), r.get("state")]))
    if loc:
        parts.append(f"Location: {loc}")

    if r.get("experience_years") and r["experience_years"] > 0:
        parts.append(f"Experience: {r['experience_years']} years")

    certs = r.get("verified_certifications") or []
    if certs:
        parts.append("Certifications: " + ", ".join(certs))

    skills = r.get("skills") or []
    if skills:
        parts.append("Skills: " + ", ".join(skills[:40]))

    if r.get("about"):
        parts.append(r["about"][:1000])

    if r.get("experience_text"):
        parts.append(r["experience_text"][:1500])

    return "\n".join(parts)


def embed(client, text):
    text = text[:6000]  # Titan v2 max ~8192 tokens
    resp = client.invoke_model(
        modelId=EMBED_MODEL,
        body=json.dumps({"inputText": text, "dimensions": EMBED_DIM, "normalize": True}),
        contentType="application/json",
        accept="application/json",
    )
    return json.loads(resp["body"].read())["embedding"]


def run():
    run_start = datetime.now(timezone.utc).isoformat()
    watermark = get_watermark()
    logger.info("=== shared-embed-upsert started | watermark=%s ===", watermark)

    conn    = db()
    bedrock_client = bedrock()
    stats   = {"embedded": 0, "skipped": 0, "errors": 0}

    with conn.cursor() as cur:
        cur.execute("""
            SELECT uid, current_title, current_company, headline,
                   city, state, experience_years,
                   verified_certifications, skills, about, experience_text
            FROM candidates
            WHERE updated_at > %s
            ORDER BY updated_at
        """, (watermark,))
        rows = cur.fetchall()

    logger.info("Candidates to embed: %d", len(rows))

    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        texts = [build_profile_text(dict(r)) for r in batch]

        for r, text in zip(batch, texts):
            if not text.strip():
                stats["skipped"] += 1
                continue
            try:
                vec = embed(bedrock_client, text)
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE candidates SET embedding_titan = %s::vector, updated_at = now() WHERE uid = %s",
                        (vec, r["uid"])
                    )
                stats["embedded"] += 1
            except Exception as e:
                stats["errors"] += 1
                if stats["errors"] <= 5:
                    logger.warning("embed error uid=%s: %s", r["uid"], e)

        conn.commit()
        logger.info("  [%d/%d] embedded=%d errors=%d",
                    min(i + BATCH_SIZE, len(rows)), len(rows),
                    stats["embedded"], stats["errors"])

    conn.close()
    set_watermark(run_start)
    logger.info("=== shared-embed-upsert done | embedded=%d skipped=%d errors=%d ===",
                stats["embedded"], stats["skipped"], stats["errors"])


if __name__ == "__main__":
    run()
