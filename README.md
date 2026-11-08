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
  │  └── brightdata/                                     │
  └──────────────────────┬──────────────────────────────┘
                         │  Glue Python Shell jobs
                         ▼
  ┌─────────────────────────────────────────────────────┐
  │              S3 Silver  (normalized Parquet)         │
  │  s3://prajnov-talent-lake/silver/                    │
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
Bronze (credly_bronze_ingest)
  └── Scrapes 40+ SN cert slugs from Credly directory
  └── Output: s3://prajnov-talent-lake/bronze/discovery/credly/certs/{slug}/consolidated/all.json

Silver (credly_silver_enrich)
  └── Matches Credly earners against BrightData candidates by name/LinkedIn
  └── Writes verified_certifications[] to candidates table

DQ Fix (shared_dq_fix)
  └── Normalizes dirty cert codes (e.g. "cis-itsm" → "CIS-ITSM")
  └── Deduplicates by linkedin_id, fixes location fields

Gold Merge (shared_gold_merge)
  └── Entity resolution across all sources → gold candidates table

Embed Upsert (shared_embed_upsert)
  └── Assembles profile text → Bedrock Titan v2 → embedding_titan column
```

**Schedule**: Every Sunday 01:00 UTC via `prajnov-talent-lake-pipeline` Step Functions state machine.

**Step Functions ARN**: `arn:aws:states:ap-south-1:201727487159:stateMachine:prajnov-talent-lake-pipeline`

---

### BrightData Pipeline

Ingests LinkedIn profile data from BrightData datasets bucket, normalizes and upserts into candidates table.

```
Bronze (brightdata_bronze_ingest)
  └── Scans s3://prajnov-brightdata-datasets for new files (ETag watermark)
  └── Streams into bronze JSONL batches

Silver (brightdata_silver_normalize)
  └── Normalizes JSONL → 5 Parquet tables (profiles, experience, education, skills, certs)

Gold (brightdata_gold_upsert)
  └── Upserts silver into candidates table — entity resolution by linkedin_id

Embed Upsert (shared_embed_upsert)
  └── Assembles profile text → Bedrock Titan v2 → embedding_titan column
```

**Schedule**: Every Sunday 02:00 UTC via `prajnov-brightdata-pipeline` Step Functions state machine.

**Step Functions ARN**: `arn:aws:states:ap-south-1:201727487159:stateMachine:prajnov-brightdata-pipeline`

---

### Embed Pipeline

Standalone embedding job that can be run independently to backfill or catch up on unembedded candidates.

```
shared_embed_upsert
  └── Reads candidates WHERE embedding_titan IS NULL (up to MAX_ROWS=10000)
  └── Assembles profile text: title + company + location + certs + skills + about + experience
  └── Calls Bedrock Titan Embeddings v2 (20 threads, per-thread boto3 client)
  └── Writes 1024-dim vector to embedding_titan column
  └── Refreshes candidates_search materialized view on completion
```

**Glue job**: `prajnov-shared-embed-upsert`
**Timeout**: 240 mins | **Capacity**: 0.0625 DPU (1 GB RAM)

To run the embed loop until all candidates are embedded:

```bash
# Start loop (auto-retriggers until pending = 0)
nohup bash infra/glue/embed_loop.sh > /tmp/embed_loop.log 2>&1 &

# Monitor
tail -f /tmp/embed_loop.log

# Check DB progress directly
ssh -i ~/.ssh/prajnov-recruiter ec2-user@<ec2-ip> 'python3 -c "
import psycopg2
conn = psycopg2.connect(host=\"ai-recruiter.c7q8sg00wqte.ap-south-1.rds.amazonaws.com\", dbname=\"recruiter\", user=\"prajnov\", password=\"<password>\")
cur = conn.cursor()
cur.execute(\"SELECT COUNT(*) FILTER (WHERE embedding_titan IS NOT NULL), COUNT(*) FILTER (WHERE embedding_titan IS NULL) FROM candidates\")
r = cur.fetchone()
print(f\"embedded={r[0]}  pending={r[1]}\")
"'
```

---

## CI/CD Setup

The CI/CD pipeline uses **GitHub Actions** (`.github/workflows/deploy.yml`).

On every push to `main` it:
1. Detects changed Glue scripts and syncs them to S3
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

The IAM user used by GitHub Actions needs at minimum:

```json
{
  "Effect": "Allow",
  "Action": [
    "s3:PutObject", "s3:GetObject", "s3:ListBucket",
    "glue:*",
    "states:*",
    "iam:GetRole", "iam:PassRole",
    "scheduler:*",
    "sns:*",
    "events:*",
    "cloudwatch:*",
    "secretsmanager:GetSecretValue",
    "athena:*"
  ],
  "Resource": "*"
}
```

### How it works

```
git push → main
    │
    ├── deploy-scripts job
    │     └── git diff HEAD~1 HEAD -- infra/glue/jobs/*.py
    │     └── aws s3 cp <changed>.py s3://prajnov-talent-lake/glue-scripts/
    │
    └── terraform job (runs after deploy-scripts)
          └── terraform init
          └── terraform validate
          └── terraform plan
          └── terraform apply  ← only on push to main
```

### Terraform remote state (recommended next step)

Currently Terraform state is stored locally (`infra/glue/terraform.tfstate`). For team use, migrate to S3 backend:

```hcl
# Add to infra/glue/main.tf
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

### Environment variables

```bash
cp ingestion/.env.example ingestion/.env
# Fill in API keys — see .env.example for required vars
```

---

## Running Pipelines Manually

### Trigger full Credly pipeline (Step Functions)

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
# Replace JOB_NAME with any of the jobs listed below
aws glue start-job-run \
  --job-name JOB_NAME \
  --profile 201727487159_AdministratorAccess \
  --region ap-south-1
```

| Job Name | Description |
|---|---|
| `prajnov-credly-bronze-ingest` | Scrape Credly cert earners |
| `prajnov-credly-silver-enrich` | Match Credly → candidates, enrich certs |
| `prajnov-shared-dq-fix` | Normalize cert codes, dedup, fix locations |
| `prajnov-shared-gold-merge` | Entity resolution → gold candidates |
| `prajnov-brightdata-bronze-ingest` | Ingest BrightData datasets → bronze JSONL |
| `prajnov-brightdata-silver-normalize` | Normalize bronze → silver Parquet |
| `prajnov-brightdata-gold-upsert` | Upsert silver → candidates table |
| `prajnov-shared-embed-upsert` | Generate Bedrock embeddings → embedding_titan |

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

SNS email alerts are sent to `navin@prajnov.com` on:
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

# Destroy (careful!)
terraform destroy
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
- Terraform state backend (currently local — see [remote state](#terraform-remote-state-recommended-next-step))

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
│   │   ├── jobs/               # All Glue Python Shell job scripts
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
│   │   └── embed_loop.sh       # Local loop script — retriggers embed job until done
│   ├── iam/                    # (placeholder — IAM managed in main.tf)
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
├── processing/                 # Ad-hoc processing scripts
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
