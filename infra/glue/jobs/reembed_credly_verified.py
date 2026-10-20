"""
Re-embed 2,001 BD credly_verified candidates whose embeddings predate cert verification.
Embeddings are rebuilt to include verified_certifications in the text.
"""
import sys, json
from pathlib import Path
from sentence_transformers import SentenceTransformer

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "ingestion"))
from common import get_db

EMBED_MODEL = "all-MiniLM-L6-v2"
BATCH_SIZE  = 100


def build_embed_text(r: dict) -> str:
    cert_text  = " ".join(r.get("verified_certifications") or [])
    skill_text = ", ".join((r.get("skills") or [])[:30])
    return " | ".join(filter(None, [
        r.get("name", ""),
        r.get("current_title", ""),
        r.get("current_company", ""),
        f"Location: {r.get('city','')} {r.get('state','')}".strip(),
        f"Certifications: {cert_text}" if cert_text else "",
        f"Skills: {skill_text}"        if skill_text else "",
    ]))


def run():
    conn  = get_db()
    model = SentenceTransformer(EMBED_MODEL)

    with conn.cursor() as cur:
        cur.execute("""
            SELECT uid, name, current_title, current_company, city, state,
                   skills, verified_certifications
            FROM candidates
            WHERE source = 'brightdata' AND cert_verification_status = 'credly_verified'
        """)
        rows = cur.fetchall()

    print(f"Re-embedding {len(rows)} BD credly_verified candidates...")

    updated = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        texts = [build_embed_text(dict(r)) for r in batch]
        vecs  = model.encode(texts, normalize_embeddings=True, batch_size=BATCH_SIZE)

        with conn.cursor() as cur:
            for r, vec in zip(batch, vecs):
                cur.execute(
                    "UPDATE candidates SET embedding = %s::vector, updated_at = now() WHERE uid = %s",
                    (vec.tolist(), r["uid"])
                )
                updated += cur.rowcount
        conn.commit()
        print(f"  [{min(i + BATCH_SIZE, len(rows))}/{len(rows)}] updated={updated}")

    conn.close()
    print(f"\nDone. {updated} embeddings updated.")


if __name__ == "__main__":
    run()
