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

import boto3
import psycopg2
import psycopg2.extras
from botocore.exceptions import ClientError

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
SECRET_ARN = _arg("PGVECTOR_SECRET_ARN", "arn:aws:secretsmanager:ap-south-1:201727487159:secret:prajnov/recruiter-F8sc3X")

def _get_db_pass() -> str:
    env_pass = os.getenv("PGVECTOR_PASSWORD", "")
    if env_pass:
        return env_pass
    client = boto3.client("secretsmanager", region_name=REGION)
    secret = json.loads(client.get_secret_value(SecretId=SECRET_ARN)["SecretString"])
    return secret.get("PGVECTOR_PASSWORD", "")


EMBED_MODEL  = "amazon.titan-embed-text-v2:0"
EMBED_DIM    = 1024
BATCH_SIZE   = 50
MAX_ROWS     = 10000
THREADS      = 20
STATE_KEY    = "glue-state/shared_embed_upsert/last_run.json"


def db():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=_get_db_pass(),
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


def embed_one(args):
    """Each thread creates its own Bedrock client."""
    import time
    uid, text = args
    if not text.strip():
        return uid, None
    client = boto3.client("bedrock-runtime", region_name=REGION)
    for attempt in range(5):
        try:
            return uid, embed(client, text)
        except Exception as e:
            if "Throttling" in str(e) or "TooManyRequests" in str(e):
                time.sleep(2 ** attempt)
                continue
            return uid, e
    return uid, Exception("throttled after 5 retries")


def run():
    run_start = datetime.now(timezone.utc).isoformat()
    watermark = get_watermark()
    print(f"=== shared-embed-upsert started | watermark={watermark} ===", flush=True)

    conn  = db()
    stats = {"embedded": 0, "skipped": 0, "errors": 0}

    with conn.cursor() as cur:
        cur.execute("""
            SELECT uid, current_title, current_company, headline,
                   city, state, experience_years,
                   verified_certifications, skills, about, experience_text
            FROM candidates
            WHERE embedding_titan IS NULL
            ORDER BY added_at
            LIMIT %s
        """, (MAX_ROWS,))
        rows = cur.fetchall()

    print(f"Candidates to embed: {len(rows)} (cap={MAX_ROWS})", flush=True)
    if not rows:
        print("Nothing to embed. Done.", flush=True)
        conn.close()
        return

    tasks = [(dict(r)["uid"], build_profile_text(dict(r))) for r in rows]

    from concurrent.futures import ThreadPoolExecutor, as_completed
    batch_updates = []
    completed = 0
    with ThreadPoolExecutor(max_workers=THREADS) as pool:
        futures = {pool.submit(embed_one, t): t[0] for t in tasks}
        for future in as_completed(futures):
            uid, result = future.result()
            completed += 1
            if result is None:
                stats["skipped"] += 1
            elif isinstance(result, Exception):
                stats["errors"] += 1
                print(f"ERROR uid={uid}: {result}", flush=True)
            else:
                batch_updates.append((result, uid))
                stats["embedded"] += 1

            if completed % 100 == 0:
                print(f"  progress={completed}/{len(tasks)} embedded={stats['embedded']} errors={stats['errors']}", flush=True)

            if len(batch_updates) >= BATCH_SIZE:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(
                        cur,
                        "UPDATE candidates SET embedding_titan = data.vec::vector, updated_at = now() FROM (VALUES %s) AS data(vec, uid) WHERE candidates.uid = data.uid",
                        batch_updates, template="(%s::float4[], %s)"
                    )
                conn.commit()
                print(f"  flushed | embedded={stats['embedded']}", flush=True)
                batch_updates = []

    # Flush remainder
    if batch_updates:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                "UPDATE candidates SET embedding_titan = data.vec::vector, updated_at = now() FROM (VALUES %s) AS data(vec, uid) WHERE candidates.uid = data.uid",
                batch_updates, template="(%s::float4[], %s)"
            )
        conn.commit()

    conn.close()
    set_watermark(run_start)
    print(f"=== shared-embed-upsert done | embedded={stats['embedded']} skipped={stats['skipped']} errors={stats['errors']} ===", flush=True)

    # Refresh search materialized view so candidate search reflects new data
    print("Refreshing candidates_search materialized view...", flush=True)
    import psycopg2 as _pg2
    ref_conn = _pg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=_get_db_pass())
    ref_conn.autocommit = True
    ref_conn.cursor().execute("REFRESH MATERIALIZED VIEW CONCURRENTLY candidates_search")
    ref_conn.close()
    print("candidates_search refreshed", flush=True)


run()
