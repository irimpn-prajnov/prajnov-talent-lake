# Prajnov Talent Intelligence Lake — Architecture

## System Overview

```
                        PRAJNOV TALENT INTELLIGENCE LAKE
                        ==================================

  ┌──────────────────────────── DATA SOURCES ─────────────────────────────┐
  │                                                                        │
  │  Tier 1 — Own Data          Tier 2 — Partner APIs                     │
  │  ─────────────────          ──────────────────────                     │
  │  Recruiter uploads          Indeed Partner API                         │
  │  Email attachments          GitHub REST API                            │
  │  Previous submissions       SignalHire API                             │
  │  Candidate applications     Credly / badge APIs                        │
  │  Referrals                  AWS / Microsoft / Google cert APIs         │
  │                                                                        │
  │  Tier 3 — Discovery (read-only, no PII storage)                        │
  │  ──────────────────────────────────────────────                        │
  │  Google Custom Search → LinkedIn public URL discovery                  │
  │  GitHub public profiles                                                │
  │                                                                        │
  └──────────────────────────────┬─────────────────────────────────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │   INGESTION LAYER        │
                    │                          │
                    │  Lambda / ECS Tasks      │
                    │  EventBridge schedules   │
                    │  Step Functions          │
                    │  API Gateway (webhooks)  │
                    └────────────┬─────────────┘
                                 │
                                 ▼
  ┌──────────────────────────────────────────────────────────────────────┐
  │                        S3 BRONZE (Raw)                               │
  │                                                                      │
  │  s3://prajnov-talent-lake/bronze/                                    │
  │  ├── candidates/resumes/          ├── github/users/                  │
  │  ├── candidates/applications/     ├── github/repos/                  │
  │  ├── candidates/referrals/        ├── certifications/credly/         │
  │  ├── requirements/email/          ├── certifications/aws/            │
  │  ├── requirements/manual/         ├── enrichment/signalhire/         │
  │  └── discovery/web_search/        └── indeed/applications/           │
  │                                                                      │
  │  Every object: source + timestamp + raw_hash + provenance            │
  │  Immutable — never overwrite                                         │
  └──────────────────────────────┬───────────────────────────────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │  PROCESSING (Glue/Python)│
                    │                          │
                    │  Resume parsing          │
                    │  Skill normalization     │
                    │  Entity resolution       │
                    │  Deduplication           │
                    │  PII classification      │
                    │  Data quality checks     │
                    └────────────┬─────────────┘
                                 │
                                 ▼
  ┌──────────────────────────────────────────────────────────────────────┐
  │                       S3 SILVER (Normalized)                         │
  │                                                                      │
  │  Apache Iceberg tables via Glue Data Catalog                         │
  │                                                                      │
  │  silver.candidate              silver.requirement                    │
  │  silver.experience             silver.requirement_skill              │
  │  silver.education              silver.certification_catalog          │
  │  silver.skill                  silver.github_activity                │
  │  silver.candidate_skill        silver.enrichment_result              │
  │  silver.candidate_certification                                      │
  └──────────────────────────────┬───────────────────────────────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │  GOLD TRANSFORMS         │
                    │                          │
                    │  Candidate 360 assembly  │
                    │  Requirement 360 assembly│
                    │  Match feature eng.      │
                    │  Embedding generation    │
                    └────────────┬─────────────┘
                                 │
                                 ▼
  ┌──────────────────────────────────────────────────────────────────────┐
  │                          GOLD LAYER                                  │
  │                                                                      │
  │  Aurora PostgreSQL (pgvector)     OpenSearch Serverless              │
  │  ─────────────────────────────    ──────────────────────             │
  │  candidate_360                    candidate_embeddings               │
  │  requirement_360                  requirement_embeddings             │
  │  submission_history               skill_embeddings                   │
  │  placement_history                full-text search                   │
  │  recruiter_feedback                                                  │
  │  certification_registry                                              │
  └──────────────────────────────┬───────────────────────────────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │   SEARCH & MATCHING      │
                    │                          │
                    │  Hard filters (SQL)      │
                    │  Semantic search (pgvec) │
                    │  Cross-encoder rerank    │
                    │  LLM explanation         │
                    └────────────┬─────────────┘
                                 │
                                 ▼
  ┌──────────────────────────────────────────────────────────────────────┐
  │                       GENAI RECRUITER AGENTS                         │
  │                                                                      │
  │  Requirement Agent        Matching Agent        Recruiter Agent      │
  │  ─────────────────        ───────────────        ───────────────     │
  │  Parse JD                 Retrieve candidates    Candidate summary   │
  │  Extract must-haves       Apply hard filters     Outreach draft      │
  │  Extract nice-to-haves    Semantic search        Interview prep      │
  │  Normalize skills         Rerank                 Submission package  │
  │  Requirement 360          Explain match          Feedback capture    │
  └──────────────────────────────────────────────────────────────────────┘
```

---

## AWS Component Map

| Component | AWS Service | Purpose |
|---|---|---|
| Raw storage | S3 (Standard + Intelligent-Tiering) | Bronze/Silver/Gold lake |
| Table format | Apache Iceberg via Glue | ACID, schema evolution, time travel |
| ETL | AWS Glue + Python Shell jobs | Bronze → Silver → Gold |
| Orchestration | Step Functions + EventBridge | Pipeline scheduling & retry |
| Structured store | Aurora PostgreSQL Serverless v2 | Candidate/Requirement 360 |
| Vector search | pgvector (Aurora) → OpenSearch Serverless | Semantic candidate search |
| LLM | Bedrock (Claude 3 Haiku / Sonnet) | Parsing, matching, agents |
| Embeddings | Bedrock Titan Embeddings v2 | Candidate + JD embeddings |
| Query | Athena | Ad-hoc lake queries |
| Catalog | Glue Data Catalog | Iceberg table registry |
| Secrets | Secrets Manager | API keys, DB credentials |
| Compute | Lambda + ECS Fargate | Ingestion connectors |
| Monitoring | CloudWatch + X-Ray | Pipeline observability |

---

## Matching Pipeline

```
REQUIREMENT INPUT
      │
      ▼
Requirement Agent (Bedrock)
      │
      ├── role, title
      ├── mandatory_skills[]
      ├── preferred_skills[]
      ├── mandatory_certifications[]
      ├── experience_min
      ├── location + work_mode
      └── rate_max
      │
      ▼
┌─────────────────────────────────────┐
│           HARD FILTERS (SQL)        │
│  certification ∈ mandatory_certs    │
│  location matches                   │
│  work_authorization valid           │
│  rate ≤ rate_max                    │
│  experience ≥ experience_min        │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│        SEMANTIC RETRIEVAL           │
│  pgvector ANN on JD embedding       │
│  skill embedding overlap            │
│  project/domain embedding           │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│         CROSS-ENCODER RERANK        │
│  ms-marco-MiniLM or Bedrock         │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│         MATCH EXPLANATION           │
│  Bedrock Claude                     │
│  Per-candidate evidence summary     │
│  Mandatory met / gaps               │
└──────────────┬──────────────────────┘
               │
               ▼
        RANKED CANDIDATES
        (recruiter review)
```

---

## Feedback Loop

```
AI suggests candidate
        │
Recruiter reviews
        │
        ├── Submitted → client feedback → interview → offer → placement
        │
        └── Rejected → reason captured
                        │
                        ├── wrong_skill
                        ├── insufficient_experience
                        ├── wrong_location
                        ├── rate_too_high
                        ├── certification_missing
                        ├── poor_domain_experience
                        ├── duplicate
                        └── unavailable

All outcomes stored in recruiter_feedback table
→ Used for match feature engineering
→ Used for client-specific model tuning
```

---

## Data Governance

- Bronze is immutable — append only, never overwrite
- Every fact carries: `source`, `confidence`, `last_verified`, `evidence`
- PII classified at ingestion — `contact.*` fields tagged
- Inferred skills clearly distinguished from claimed skills
- Recruiter is always responsible for final candidate decisions
- No sensitive/protected attributes used in ranking
- Retention policies enforced via S3 lifecycle rules
