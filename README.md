# Prajnov Talent Intelligence Lake

AI-powered candidate intelligence platform built on AWS lakehouse architecture.
Combines multi-source candidate data, certification intelligence, technical evidence,
and GenAI matching to power the Prajnov AI Recruiter.

---

## Table of Contents

- [Architecture](#architecture)
- [AWS Components](#aws-components)
- [Data Pipelines](#data-pipelines)
  - [Credly Pipeline](#credly-pipeline)
  - [BrightData Pipeline](#brightdata-pipeline)
  - [Embed Pipeline](#embed-pipeline)
- [Glue Jobs — Detailed](#glue-jobs--detailed)
- [CI/CD Setup](#cicd-setup)
- [Local Development](#local-development)
- [Running Pipelines Manually](#running-pipelines-manually)
- [Monitoring](#monitoring)
- [Infrastructure (Terraform)](#infrastructure-terraform)
- [Workspace Structure](#workspace-structure)

---

## Architecture

```
  DATA SOURCES
  ────────────
  BrightData (LinkedIn profiles)     Credly (cert badge verification)
  SignalHire (contact enrichment)    Resume uploads / manual entry

        │                                    │
        ▼                                    ▼
  ┌─────────────────────────────────────────────────────┐
  │              S3 Bronze  (raw, immutable)             │
  │  s3://prajnov-talent-lake/bronze/                    │
  │  ├── discovery/credly/certs/{slug}/consolidated/     │
  │  └── candidates/brightdata/{dataset}/{date}/         │
  └──────────────────────┬──────────────────────────────┘
                         │  Glue Python Shell jobs
                         ▼
  ┌─────────────────────────────────────────────────────┐
  │              S3 Silver  (normalized Parquet)         │
  │  s3://prajnov-talent-lake/silver/                    │
  │  ├── candidate/source=brightdata/                    │
  │  ├── candidate_certification/source=brightdata/      │
  │  ├── candidate_skill/source=brightdata/              │
  │  ├── candidate_experience/source=brightdata/         │
  │  └── candidate_education/source=brightdata/          │
  └──────────────────────┬──────────────────────────────┘
                         │
                         ▼
  ┌─────────────────────────────────────────────────────┐
  │         Aurora PostgreSQL  (Gold — candidates)       │
  │  host: ai-recruiter.c7q8sg00wqte.ap-south-1.rds     │
  │  db:   recruiter                                     │
  │  table: candidates  (131k rows, pgvector embeddings) │
  └──────────────────────┬──────────────────────────────┘
                         │
                         ▼
  ┌─────────────────────────────────────────────────────┐
  │         Bedrock Titan Embeddings v2  (1024-dim)      │
  │  embedding_titan column → pgvector ANN search        │
  └─────────────────────────────────────────────────────┘
                         │
                         ▼
              Prajnov AI Recruiter (web app)
```

---

## AWS Components

| Component | Service | Detail |
|---|---|---|
| Raw storage | S3 | `prajnov-talent-lake` bucket, Bronze/Silver/Gold prefixes |
| ETL | Glue Python Shell | 8 jobs, `glue_version=4.0`, `python 3.9` |
| Orchestration | Step Functions | 2 state machines — Credly + BrightData pipelines |
| Scheduling | EventBridge Scheduler | Weekly Sunday 01:00 UTC (Credly), 02:00 UTC (BrightData) |
| Structured store | Aurora PostgreSQL Serverless v2 | `db.serverless`, 0.5–4 ACU, pgvector extension |
| Embeddings | Bedrock Titan Embeddings v2 | `amazon.titan-embed-text-v2:0`, 1024-dim, region `ap-south-1` |
| Secrets | Secrets Manager | `prajnov/recruiter` — DB password |
| Notifications | SNS + CloudWatch | Email alerts on pipeline success/failure to `navin@prajnov.com` |
| IAM | Roles | `prajnov-glue-talent-lake`, `prajnov-sfn-talent-lake`, `prajnov-scheduler-talent-lake` |

---

## Data Pipelines

### Credly Pipeline

Scrapes ServiceNow certification earners from Credly, enriches candidate records with verified certifications.

```
Step 1 — Bronze  (credly_bronze_ingest)
Step 2 — Silver  (credly_silver_enrich)
Step 3 — DQ Fix  (shared_dq_fix)
Step 4 — Gold    (shared_gold_merge)
Step 5 — Embed   (shared_embed_upsert)
```

**Schedule**: Every Sunday 01:00 UTC
**Step Functions ARN**: `arn:aws:states:ap-south-1:201727487159:stateMachine:prajnov-talent-lake-pipeline`

---

### BrightData Pipeline

Ingests LinkedIn profile data from BrightData datasets bucket, normalizes and upserts into candidates table.

```
Step 1 — Bronze  (brightdata_bronze_ingest)
Step 2 — Silver  (brightdata_silver_normalize)
Step 3 — Gold    (brightdata_gold_upsert)
Step 4 — Embed   (shared_embed_upsert)
```

**Schedule**: Every Sunday 02:00 UTC
**Step Functions ARN**: `arn:aws:states:ap-south-1:201727487159:stateMachine:prajnov-brightdata-pipeline`

---

### Embed Pipeline

Standalone backfill job — embeds all candidates where `embedding_titan IS NULL`.

```
shared_embed_upsert
  └── Reads up to 10,000 candidates with no embedding
  └── Assembles profile text per candidate
  └── Calls Bedrock Titan v2 (20 threads, per-thread client)
  └── Writes 1024-dim vector to embedding_titan column
  └── Refreshes candidates_search materialized view
```

To run until all candidates are embedded:

```bash
nohup bash infra/glue/embed_loop.sh > /tmp/embed_loop.log 2>&1 &
tail -f /tmp/embed_loop.log
```

---

## Glue Jobs — Detailed

### 1. `prajnov-credly-bronze-ingest`

**What it does**: Scrapes all ServiceNow certification earners from the Credly public directory API and writes them to S3 bronze.

**How it works**:
- Iterates over 43 ServiceNow cert types (CSA, CAD, CTA, all CIS-* and SUITE-* certs)
- For each cert, paginates through the Credly directory API (`/api/v1/directory`) filtering by badge name
- Runs 4 certs in parallel (ThreadPoolExecutor) with 0.3s rate limiting per page
- Incremental — loads the existing consolidated file per cert, only adds new earners by `credly_id`
- Writes one consolidated JSON array per cert: `bronze/discovery/credly/certs/{slug}/consolidated/all.json`
- Writes a run manifest with per-cert counts to `bronze/discovery/credly/_manifests/`

**Output**: ~43 consolidated JSON files in S3 bronze, one per cert slug
**Capacity**: 0.0625 DPU | **Timeout**: 3 hours

---

### 2. `prajnov-credly-silver-enrich`

**What it does**: Matches Credly earners against existing candidates in the DB and enriches them with verified certifications. Inserts unmatched earners as new candidate rows.

**How it works**:
- Loads all 43 consolidated bronze files from S3
- Deduplicates by `credly_id` across all certs — one record per person with all their cert codes
- Loads records into a temp staging table in the DB
- **Tier 1 match**: exact `linkedin_id` match — highest confidence
- **Tier 2 match**: normalized name + state match — used only when Tier 1 fails
- For matched candidates: appends cert codes to `verified_certifications[]`, sets `cert_verification_status = 'credly_verified'`
- For unmatched earners: inserts as new `source='credly'` candidates with `verified_certifications` populated
- Creates a GIN index on `verified_certifications` for fast cert-based filtering

**Output**: Updated `verified_certifications[]` on matched candidates, new rows for unmatched Credly earners
**Capacity**: 0.0625 DPU | **Timeout**: 2 hours

---

### 3. `prajnov-shared-dq-fix`

**What it does**: Fixes data quality issues across the candidates table — normalizes dirty cert codes, deduplicates by LinkedIn ID, recalculates experience years, and re-parses broken location fields.

**How it works**:

**Fix 1 — Normalize dirty cert codes**:
- Reads all candidates with non-null `certifications` or `verified_certifications`
- Maps dirty/verbose cert strings (e.g. `"cis-itsm certified implementation specialist - it service management"`) to canonical codes (e.g. `"cis-itsm"`)
- Removes micro-certs, suite certs, and unrecognized codes from `verified_certifications`
- Batch-updates via a temp table — single SQL UPDATE

**Fix 2 — Deduplicate by linkedin_id**:
- Finds all `linkedin_id` values with more than one candidate row
- Merges `verified_certifications`, `certifications`, `skills`, and `about` from all duplicate rows into the canonical row (source priority: credly > signalhire > brightdata)
- Flags non-canonical duplicates with `source = source || '+duplicate'`
- Uses a single CTE-based SQL pass — no correlated subqueries

**Fix 3 — Recalculate experience_years**:
- Finds candidates where `experience_years` is NULL, 0, or negative
- Derives years from the earliest `start_year` in the `experience` JSON array
- Caps at 40 years, minimum 1

**Fix 4 — Re-parse broken locations**:
- Finds candidates with a `location_raw` value but missing `state`
- Parses `"City, State"` format using a full US state name/abbreviation map
- Updates `city`, `state`, `country` fields

**Output**: Cleaned `candidates` table — normalized certs, deduped rows, corrected locations and experience years
**Capacity**: 0.0625 DPU | **Timeout**: 1 hour

---

### 4. `prajnov-shared-gold-merge`

**What it does**: Entity resolution across all sources — merges duplicate candidate rows (same person from Credly + BrightData + SignalHire) into a single gold record.

**How it works**:
- Finds all `linkedin_id` values with more than one candidate row
- For each duplicate group, fetches all rows sorted by source priority (credly > signalhire > brightdata)
- Merges fields using source-specific rules:
  - `verified_certifications` — union from ALL sources (Credly wins)
  - `current_title`, `current_company` — BrightData wins (most up-to-date)
  - `about`, `experience`, `experience_text` — longest value wins
  - `skills`, `companies` — union from all sources
  - `email`, `phone` — SignalHire wins
  - `cert_verification_status` — highest confidence wins (verified > checked_not_found > unverified)
- Upserts the merged record back to the canonical row
- Flags duplicate rows with `source = source || '+duplicate'`
- Calculates a `data_quality_score` (0–1) based on field completeness

**Output**: Merged gold candidate records, duplicate rows flagged
**Capacity**: 0.0625 DPU | **Timeout**: 1 hour

---

### 5. `prajnov-brightdata-bronze-ingest`

**What it does**: Scans the BrightData source bucket for new LinkedIn dataset files and streams them into S3 bronze as batched JSONL files.

**How it works**:
- Lists all `.json` and `.parquet` files in `s3://prajnov-brightdata-datasets/`
- Uses an ETag-based watermark (`glue-state/brightdata_bronze_ingest/processed_files.json`) — each file is only processed once
- Streams large files record-by-record using `ijson` (JSON) or PyArrow (Parquet) — never loads full file into memory
- Normalizes each raw BrightData record into a consistent bronze schema:
  - Standardizes LinkedIn URL format
  - Parses `experience[]` with `start_year`/`end_year` extracted from date strings
  - Derives `experience_years` from earliest start year
  - Extracts `skills[]`, `certifications[]`, `education[]`
- Deduplicates by `linkedin_id` within each file (capped at 200k seen set)
- Flushes every 1,000 records as a JSONL batch to `bronze/candidates/brightdata/{slug}/{date}/batch_{n}.jsonl`
- Updates the ETag watermark on completion

**Output**: JSONL batch files in S3 bronze, one batch per 1,000 records
**Capacity**: 1.0 DPU (4 vCPU / 16 GB — needed for large dataset files) | **Timeout**: 2 hours

---

### 6. `prajnov-brightdata-silver-normalize`

**What it does**: Reads all new bronze JSONL batches since the last run and normalizes them into 5 structured Parquet tables in S3 silver.

**How it works**:
- Uses a timestamp watermark (`glue-state/brightdata_silver_normalize/last_run.json`) to find new bronze JSONL files
- Fetches all new JSONL files in parallel (20 threads)
- Deduplicates by `linkedin_id` — one silver row per person
- For each record, produces 5 output tables:
  - `silver/candidate/` — core profile fields (name, title, company, location, about)
  - `silver/candidate_certification/` — normalized cert codes with `verification_status='self_reported'`
  - `silver/candidate_skill/` — one row per skill with confidence score
  - `silver/candidate_experience/` — one row per job with start/end years
  - `silver/candidate_education/` — one row per institution
- Normalizes cert strings to canonical codes (e.g. `"it service management"` → `"cis-itsm"`)
- Parses location into `city`, `state`, `country` using US state map + country code map
- Writes each table as a Snappy-compressed Parquet file

**Output**: 5 Parquet files per run in S3 silver
**Capacity**: 1.0 DPU | **Timeout**: 1 hour

---

### 7. `prajnov-brightdata-gold-upsert`

**What it does**: Reads silver Parquet files and upserts candidate records into the gold `candidates` table in Aurora PostgreSQL.

**How it works**:
- Uses a timestamp watermark to find new silver Parquet files since last run
- Loads all 5 silver tables and indexes `certifications`, `skills`, `experience`, `education` by `candidate_id`
- Builds a `uid = "bd_{linkedin_id}"` for each candidate
- Assembles `experience_text` as a pipe-delimited string of `"title at company"` entries
- Upserts in batches of 2,000 using `ON CONFLICT (uid) DO UPDATE`:
  - New candidates: inserted with `source='brightdata'`, `verified_certifications=[]`, `cert_verification_status='unverified'`
  - Existing candidates: updates title/company, about (if longer), skills (if more), experience, education, email/phone (if missing)
  - Never overwrites `verified_certifications` — Credly-verified certs are protected
  - Appends `+brightdata` to `source` field if not already present

**Output**: Upserted candidate rows in Aurora PostgreSQL
**Capacity**: 1.0 DPU | **Timeout**: 2 hours

---

### 8. `prajnov-shared-embed-upsert`

**What it does**: Generates 1024-dimensional vector embeddings for all candidates without an embedding and writes them to the `embedding_titan` column for semantic search.

**How it works**:
- Reads up to 10,000 candidates where `embedding_titan IS NULL`, ordered by `added_at`
- Assembles a profile text string per candidate from:
  - `current_title at current_company`
  - `headline`
  - `Location: city, state`
  - `Experience: N years`
  - `Certifications: cert1, cert2, ...` (from `verified_certifications`)
  - `Skills: skill1, skill2, ...` (first 40 skills)
  - `about` (first 1,000 chars)
  - `experience_text` (first 1,500 chars)
- Calls Bedrock Titan Embeddings v2 (`amazon.titan-embed-text-v2:0`) with 20 parallel threads
- Each thread creates its own `boto3` Bedrock client (shared client causes deadlock)
- Retries on throttling with exponential backoff (up to 5 attempts)
- Flushes DB writes every 50 embeddings
- On completion, refreshes the `candidates_search` materialized view so AIR search reflects new data

**Output**: `embedding_titan` column populated (1024-dim vector), `candidates_search` view refreshed
**Capacity**: 0.0625 DPU | **Timeout**: 4 hours


---

## CI/CD Setup

The CI/CD pipeline uses **GitHub Actions** (`.github/workflows/deploy.yml`).

On every push to `main` it:
1. Detects changed Glue scripts and syncs only those to S3
2. Runs `terraform plan` + `terraform apply` for any infra changes

On pull requests it runs `terraform plan` only (no apply).

### Required GitHub Secrets

Go to **Settings → Secrets and variables → Actions** in the GitHub repo and add:

| Secret | Value |
|---|---|
| `AWS_ACCESS_KEY_ID` | IAM user access key with Glue + S3 + Terraform permissions |
| `AWS_SECRET_ACCESS_KEY` | Corresponding secret key |
| `PGVECTOR_SECRET_ARN` | `arn:aws:secretsmanager:ap-south-1:201727487159:secret:prajnov/recruiter-F8sc3X` |

### IAM permissions needed for the CI/CD IAM user

```json
{
  "Effect": "Allow",
  "Action": [
    "s3:PutObject", "s3:GetObject", "s3:ListBucket",
    "glue:*", "states:*",
    "iam:GetRole", "iam:PassRole",
    "scheduler:*", "sns:*", "events:*",
    "cloudwatch:*", "secretsmanager:GetSecretValue", "athena:*"
  ],
  "Resource": "*"
}
```

### How it works

```
git push → main
    │
    ├── deploy-scripts job
    │     └── git diff HEAD~1 HEAD -- infra/glue/jobs/*.py ingestion/common.py
    │     └── aws s3 cp <changed>.py s3://prajnov-talent-lake/glue-scripts/
    │
    └── terraform job (runs after deploy-scripts)
          └── terraform init → validate → plan → apply (main only)
```

### Terraform remote state (recommended)

Currently Terraform state is stored locally. For team use, migrate to S3 backend:

```hcl
terraform {
  backend "s3" {
    bucket  = "prajnov-talent-lake"
    key     = "terraform/glue/terraform.tfstate"
    region  = "ap-south-1"
    encrypt = true
  }
}
```

Then run `terraform init -migrate-state` once locally.

---

## Local Development

### Prerequisites

- Python 3.9+
- AWS CLI v2 with SSO configured
- Terraform >= 1.7
- SSH key at `~/.ssh/prajnov-recruiter`

### AWS SSO Login

```bash
aws sso login --profile 201727487159_AdministratorAccess

# Export credentials for Terraform
eval $(aws configure export-credentials --profile 201727487159_AdministratorAccess --format env)
```

### Python environment

```bash
cd ingestion
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## Running Pipelines Manually

### Trigger full Credly pipeline

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:ap-south-1:201727487159:stateMachine:prajnov-talent-lake-pipeline \
  --profile 201727487159_AdministratorAccess \
  --region ap-south-1
```

### Trigger full BrightData pipeline

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:ap-south-1:201727487159:stateMachine:prajnov-brightdata-pipeline \
  --profile 201727487159_AdministratorAccess \
  --region ap-south-1
```

### Trigger individual Glue jobs

```bash
aws glue start-job-run \
  --job-name JOB_NAME \
  --profile 201727487159_AdministratorAccess \
  --region ap-south-1
```

| Job Name | Description |
|---|---|
| `prajnov-credly-bronze-ingest` | Scrape all 43 SN certs from Credly → S3 bronze |
| `prajnov-credly-silver-enrich` | Match Credly earners → candidates, enrich verified_certifications |
| `prajnov-shared-dq-fix` | Normalize cert codes, dedup, fix locations + experience_years |
| `prajnov-shared-gold-merge` | Entity resolution → merge multi-source duplicates |
| `prajnov-brightdata-bronze-ingest` | Ingest BrightData datasets → bronze JSONL batches |
| `prajnov-brightdata-silver-normalize` | Normalize bronze → 5 silver Parquet tables |
| `prajnov-brightdata-gold-upsert` | Upsert silver → candidates table |
| `prajnov-shared-embed-upsert` | Generate Bedrock Titan v2 embeddings → embedding_titan |

### Check job status

```bash
aws glue get-job-runs \
  --job-name prajnov-shared-embed-upsert \
  --max-results 5 \
  --profile 201727487159_AdministratorAccess \
  --region ap-south-1 \
  --query 'JobRuns[*].{ID:Id,Status:JobRunState,Started:StartedOn,Completed:CompletedOn}' \
  --output table
```

### Deploy a single Glue script manually

```bash
aws s3 cp infra/glue/jobs/shared_embed_upsert.py \
  s3://prajnov-talent-lake/glue-scripts/shared_embed_upsert.py \
  --profile 201727487159_AdministratorAccess
```

### Run embed backfill loop

```bash
# Auto-retriggers until all candidates are embedded
nohup bash infra/glue/embed_loop.sh > /tmp/embed_loop.log 2>&1 &

# Monitor
tail -f /tmp/embed_loop.log
```

---

## Monitoring

### CloudWatch logs

All Glue job output logs are at:
- Output: `/aws-glue/python-jobs/output/{run-id}`
- Errors: `/aws-glue/python-jobs/error/{run-id}`

```bash
aws logs get-log-events \
  --log-group-name /aws-glue/python-jobs/error \
  --log-stream-name <run-id> \
  --profile 201727487159_AdministratorAccess \
  --region ap-south-1 \
  --query 'events[*].message' --output text
```

### Pipeline notifications

SNS email alerts sent to `navin@prajnov.com` on:
- Pipeline SUCCEEDED
- Pipeline FAILED / TIMED_OUT / ABORTED

CloudWatch alarms: `prajnov-talent-lake-pipeline-failure`, `prajnov-brightdata-pipeline-failure`

### DB candidate counts

```bash
ssh -i ~/.ssh/prajnov-recruiter ec2-user@<ec2-ip> 'python3 -c "
import psycopg2
conn = psycopg2.connect(host=\"ai-recruiter.c7q8sg00wqte.ap-south-1.rds.amazonaws.com\", dbname=\"recruiter\", user=\"prajnov\", password=\"<password>\")
cur = conn.cursor()
cur.execute(\"SELECT COUNT(*) FILTER (WHERE embedding_titan IS NOT NULL) as embedded, COUNT(*) FILTER (WHERE embedding_titan IS NULL) as pending, COUNT(*) as total FROM candidates\")
r = cur.fetchone()
print(f\"embedded={r[0]}  pending={r[1]}  total={r[2]}\")
"'
```

---

## Infrastructure (Terraform)

All infrastructure is managed via Terraform in `infra/glue/`.

```bash
cd infra/glue

# Authenticate
eval $(aws configure export-credentials --profile 201727487159_AdministratorAccess --format env)

# Init (first time only)
terraform init

# Preview changes
terraform plan

# Apply
terraform apply
```

### What Terraform manages

- IAM roles: `prajnov-glue-talent-lake`, `prajnov-sfn-talent-lake`, `prajnov-scheduler-talent-lake`
- All 8 Glue jobs + S3 script uploads
- Glue catalog databases: `prajnov_bronze`, `prajnov_silver`, `prajnov_gold`
- Step Functions state machines: Credly pipeline + BrightData pipeline
- EventBridge schedules: weekly Sunday triggers
- SNS topic + email subscription + CloudWatch alarms
- Athena workgroup

### What is NOT in Terraform (manual)

- S3 bucket `prajnov-talent-lake` (created manually)
- Aurora RDS cluster (managed separately in `infra/rds/`)
- EC2 instance `prajnov-recruiter-v2` (web app host)
- Terraform state backend (currently local)

---

## Workspace Structure

```
prajnov-talent-lake/
├── .github/
│   └── workflows/
│       └── deploy.yml          # CI/CD — script sync + terraform apply
│
├── docs/
│   ├── architecture.md         # Full system architecture
│   ├── data-sources.md         # Source APIs, access models, rate limits
│   └── skill-taxonomy.md       # Skill/certification knowledge graph design
│
├── infra/
│   ├── glue/
│   │   ├── jobs/               # All 8 Glue Python Shell job scripts
│   │   │   ├── credly_bronze_ingest.py
│   │   │   ├── credly_silver_enrich.py
│   │   │   ├── shared_dq_fix.py
│   │   │   ├── shared_gold_merge.py
│   │   │   ├── brightdata_bronze_ingest.py
│   │   │   ├── brightdata_silver_normalize.py
│   │   │   ├── brightdata_gold_upsert.py
│   │   │   └── shared_embed_upsert.py
│   │   ├── main.tf             # Provider, IAM, S3 uploads, Glue catalog
│   │   ├── jobs.tf             # All Glue job definitions
│   │   ├── schedule.tf         # Step Functions, EventBridge, SNS, CloudWatch
│   │   ├── variables.tf        # Region, bucket, DB config, cert list
│   │   ├── outputs.tf          # Job names, pipeline ARNs
│   │   └── embed_loop.sh       # Local loop — retriggers embed job until done
│   ├── iam/
│   └── rds/
│       ├── main.tf             # Aurora PostgreSQL Serverless v2
│       └── schema.sql          # candidates table + pgvector setup
│
├── ingestion/                  # Local ingestion scripts (run manually)
│   ├── credly/
│   ├── signalhire/
│   ├── web_search/
│   ├── common.py               # Shared S3/DB utilities
│   ├── requirements.txt
│   └── .env.example
│
├── schemas/                    # Data model documentation
├── search/                     # Hybrid search & matching (planned)
├── agents/                     # GenAI recruiter agents (planned)
└── README.md
```

---

## Releases

| Release | Scope | Status |
|---|---|---|
| R1 | S3 lakehouse + BrightData ingestion + Candidate 360 schema | ✅ Done |
| R2 | Credly cert verification + embedding pipeline | ✅ Done |
| R3 | Hybrid search (pgvector ANN + hard filters) | In Progress |
| R4 | GitHub + SignalHire enrichment | Planned |
| R5 | Indeed integration + automated ingestion | Planned |
| R6 | Recruiter agents + feedback loop + analytics | Planned |
