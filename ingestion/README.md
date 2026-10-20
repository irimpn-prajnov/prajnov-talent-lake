# Ingestion Pipelines

All pipelines write Bronze records to `s3://prajnov-talent-lake/bronze/`.
Every record includes a `_meta` block with source, timestamp, hash, and retention class.

## Setup

```bash
cd ingestion
pip install -r requirements.txt
cp .env.example .env   # fill in credentials
export $(cat .env | xargs)
```

---

## Pipelines

### 1. `linkedin` — Brightdata Batch
Streams the 1.8GB Brightdata JSON from S3. No external API key needed.

```bash
python linkedin/ingest.py --dry-run --limit 100
python linkedin/ingest.py
python linkedin/ingest.py --dataset s3://prajnov-brightdata-datasets/servicenow-us/bd_20260628_065102.json
```

Output: `bronze/linkedin/profiles/YYYY/MM/DD/{uuid}.json`

---

### 2. `web_search` — Google CSE → LinkedIn URL Discovery
Queries Google for `site:linkedin.com/in "{title}" "United States"`.
Extracts name, headline, company, location from snippet only. No LinkedIn requests.

**Requires**: `GOOGLE_SEARCH_API_KEY`, `GOOGLE_SEARCH_CX`

```bash
python web_search/ingest.py --dry-run --limit 50
python web_search/ingest.py --groups servicenow,aws --location "United States"
python web_search/ingest.py  # runs all title groups
```

Output: `bronze/discovery/web_search/YYYY/MM/DD/{uuid}.json`

**Rate limit**: 100 queries/day free. ~900 profiles/day on free tier.

---

### 3. `dice` — Dice Candidate Discovery via Google CSE
Dice profiles require login — not publicly accessible.
Uses Google CSE targeting `site:dice.com/resume` to discover candidate pages.

**Requires**: `GOOGLE_SEARCH_API_KEY`, `GOOGLE_SEARCH_CX`

```bash
python dice/ingest.py --mode google_cse --dry-run --limit 50
python dice/ingest.py --mode google_cse --groups aws,devops
python dice/ingest.py --mode resume_page --input-file dice_urls.txt  # if you have URLs
```

Output: `bronze/dice/profiles/YYYY/MM/DD/{uuid}.json`

---

### 4. `github` — GitHub User Profiles + Repos
Searches GitHub users by title/location, fetches profile + top 10 repos with languages.

**Requires**: `GITHUB_TOKEN` (free PAT, no scopes needed for public data)

```bash
python github/ingest.py --token $GITHUB_TOKEN --dry-run --limit 50
python github/ingest.py --token $GITHUB_TOKEN
python github/ingest.py --token $GITHUB_TOKEN --queries "ServiceNow developer location:US"
```

Output: `bronze/github/users/YYYY/MM/DD/{uuid}.json`

**Rate limit**: 5,000 req/hr with PAT. ~500 full profiles/hr (profile + repos).

---

### 5. `signalhire` — SignalHire Search + Enrichment
Two modes:
- `search` — find new candidates by title/location
- `enrich` — reveal contact info for known LinkedIn URLs

**Requires**: `SIGNALHIRE_API_KEY`

```bash
python signalhire/ingest.py --mode search --dry-run --limit 50
python signalhire/ingest.py --mode search --title "ServiceNow Architect"
python signalhire/ingest.py --mode enrich --input-file linkedin_urls.txt
```

Output: `bronze/enrichment/signalhire/YYYY/MM/DD/{uuid}.json`

---

### 6. `credly` — Certification Badges

Three modes:

| Mode | Key needed? | Use case |
|---|---|---|
| `catalog` | ✅ Partner key | Pull all badge templates per org |
| `verify` | ❌ No key | Candidate shares a badge URL |
| `profile` | ❌ No key | Candidate shares their Credly username |

```bash
# No key needed — use when candidates share their Credly info
python credly/ingest.py --mode verify --badge-url https://www.credly.com/badges/abc-123
python credly/ingest.py --mode profile --username john-smith-abc123
python credly/ingest.py --mode profile --input-file credly_usernames.txt

# Partner key required
python credly/ingest.py --mode catalog --api-key $CREDLY_API_KEY --orgs aws,microsoft,servicenow
```

Output: `bronze/certifications/credly/YYYY/MM/DD/{uuid}.json`

Apply for partner key: https://info.credly.com/contact-us

---

### 7. `resumes` — Own Data Migration
Migrates existing S3 data from `prajnov-ai-recuiter-candidate-profiles`:
- `resumes/{id}/*.pdf` → bronze (with pdfplumber text extraction)
- `candidate_db/candidates.json` → bronze (3,746 existing candidates)

```bash
python resumes/ingest.py --mode all --dry-run --limit 50
python resumes/ingest.py --mode candidates   # migrate candidates.json only
python resumes/ingest.py --mode resumes      # migrate PDFs only
python resumes/ingest.py --mode all          # both
```

Output:
- `bronze/candidates/resumes/YYYY/MM/DD/{uuid}.json` (+ `.pdf` copy)
- `bronze/candidates/profiles/YYYY/MM/DD/{uuid}.json`

---

### 8. `indeed` — Indeed Partner API (stub)
Pending Indeed partner API approval.

```bash
# Apply at https://developer.indeed.com first
python indeed/ingest.py --dry-run
```

---

## Run All

```bash
python run_all.py --dry-run --limit 50                          # test all
python run_all.py --pipelines resumes,linkedin                  # specific pipelines
python run_all.py --pipelines web_search,dice,github,signalhire # discovery + enrichment
python run_all.py                                               # everything
```

---

## Pipeline Priority / Recommended Order

```
Phase 1 — Own data (free, immediate)
  resumes → migrate existing 3,746 candidates + PDFs

Phase 2 — Discovery (build the URL graph)
  linkedin   → ingest existing Brightdata 1.8GB
  web_search → Google CSE → LinkedIn URLs for all IT titles
  dice       → Google CSE → Dice profile URLs

Phase 3 — Enrichment (fill in skills + contact)
  github     → developer skill evidence
  credly     → certification verification
  signalhire → contact reveal for prioritized candidates

Phase 4 — Pending approval
  indeed     → activate once partner API approved
```

---

## Bronze Output Structure

```
s3://prajnov-talent-lake/
└── bronze/
    ├── linkedin/profiles/YYYY/MM/DD/
    ├── discovery/web_search/YYYY/MM/DD/
    ├── dice/profiles/YYYY/MM/DD/
    ├── github/users/YYYY/MM/DD/
    ├── enrichment/signalhire/YYYY/MM/DD/
    ├── certifications/credly/YYYY/MM/DD/
    ├── candidates/resumes/YYYY/MM/DD/
    ├── candidates/profiles/YYYY/MM/DD/
    ├── indeed/applications/YYYY/MM/DD/
    └── {pipeline}/_manifests/          ← run manifests
```
