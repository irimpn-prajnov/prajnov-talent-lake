# Implementation Backlog — 12 Week Plan

## Release 1 — Foundation (Weeks 1–2)
**Goal**: S3 lakehouse structure + resume ingestion + Candidate 360 schema in Aurora

### Week 1
- [ ] Create S3 bucket `prajnov-talent-lake` with Bronze/Silver/Gold prefix structure
- [ ] Set up S3 lifecycle policies (Bronze: Intelligent-Tiering, Silver/Gold: Standard)
- [ ] Set up Glue Data Catalog + Iceberg table format
- [ ] Provision Aurora PostgreSQL Serverless v2 with pgvector extension
- [ ] Run Gold schema DDL (candidate, skill, certification, requirement tables)
- [ ] Build resume ingestion connector: PDF/DOCX → extracted text → Bronze S3
- [ ] Build resume parser: extracted text → structured Candidate 360 fields (Bedrock)

### Week 2
- [ ] Build Bronze → Silver Glue job for candidates
- [ ] Build Silver → Gold pipeline: assemble Candidate 360 in Aurora
- [ ] Bulk import existing resumes from `prajnov-email-reader-data` S3 bucket
- [ ] Bulk import existing candidate profiles from `prajnov-ai-recuiter-candidate-profiles`
- [ ] Build recruiter upload UI endpoint (FastAPI) → Bronze S3
- [ ] Basic Athena queries on Silver for data quality validation

**Exit criteria**: 500+ existing candidates in Gold layer with Candidate 360 records

---

## Release 2 — Requirements + Skill Taxonomy (Weeks 3–4)
**Goal**: Requirement 360 + skill normalization + basic keyword search

### Week 3
- [ ] Load skill taxonomy into `skill` table (ServiceNow, AWS, Azure, AI/ML, etc.)
- [ ] Load certification catalog into `certification` table
- [ ] Build skill normalizer: raw skill name → canonical skill_id (alias matching)
- [ ] Build JD parser: raw JD text → Requirement 360 (Bedrock Claude)
- [ ] Ingest existing job requirements from prajnov-recruiter email data

### Week 4
- [ ] Build requirement ingestion endpoint (FastAPI) → Bronze → Silver → Gold
- [ ] Build basic candidate search API: filter by skills, certs, location, work_auth
- [ ] Build skill inference from certifications (cert → inferred skills)
- [ ] Data quality dashboard: completeness scores per candidate
- [ ] Connect to prajnov-recruiter: Staffing tab can pull from talent lake

**Exit criteria**: All existing JDs parsed into Requirement 360, basic search working

---

## Release 3 — Embeddings + Hybrid Matching (Weeks 5–6)
**Goal**: Semantic search + hybrid matching pipeline

### Week 5
- [ ] Generate Bedrock Titan Embeddings v2 for all Candidate 360 records
- [ ] Generate embeddings for all Requirement 360 records
- [ ] Store in `candidate_embedding` and `requirement_embedding` tables
- [ ] Build pgvector ANN search endpoint
- [ ] Build hard filter layer (SQL): certs, location, work_auth, rate, experience

### Week 6
- [ ] Build hybrid retrieval: hard filters → semantic search → merge
- [ ] Integrate cross-encoder reranker (ms-marco-MiniLM or Bedrock)
- [ ] Build match explanation generator (Bedrock Claude)
- [ ] Build `candidate_requirement_match` record storage
- [ ] Replace prajnov-recruiter SignalHire-only search with talent lake search
- [ ] A/B test: old ranking vs new hybrid ranking

**Exit criteria**: Hybrid search live, match explanations generated per candidate

---

## Release 4 — GitHub + Certifications + SignalHire Enrichment (Weeks 7–8)
**Goal**: Technical evidence layer + certification verification

### Week 7
- [ ] Build GitHub connector: username → profile + repos → Bronze S3
- [ ] Build GitHub → Silver processor: language/topic → inferred skills
- [ ] Build Credly badge connector: badge URL → Bronze S3 → Silver certification
- [ ] Build AWS cert verification connector
- [ ] Add ServiceNow cert verification (NowLearning)

### Week 8
- [ ] Build SignalHire enrichment pipeline: candidate → reveal → Bronze → Silver contact
- [ ] Build Google Custom Search connector: name+title → LinkedIn URL discovery
- [ ] Store LinkedIn URLs in `candidate_external_identity` (URL only, no profile content)
- [ ] Update Candidate 360 with GitHub evidence + verified certifications
- [ ] Rebuild embeddings for enriched candidates

**Exit criteria**: GitHub skills + verified certs visible in Candidate 360 and match scoring

---

## Release 5 — Indeed + Automated Ingestion (Weeks 9–10)
**Goal**: Automated daily ingestion pipelines

### Week 9
- [ ] Apply for Indeed Partner API access
- [ ] Build Indeed application connector (once approved)
- [ ] Build EventBridge + Step Functions orchestration for daily ingestion
- [ ] Build incremental Bronze → Silver → Gold pipeline (only process new/changed)
- [ ] Build entity resolution: detect duplicate candidates across sources

### Week 10
- [ ] Build deduplication merge: multiple source records → single Candidate 360
- [ ] Build candidate staleness detection: flag records not updated in 90 days
- [ ] Build automated re-enrichment: trigger SignalHire for stale contacts
- [ ] Build ingestion monitoring: CloudWatch dashboard + alerts
- [ ] Build data lineage tracking: every Gold field traceable to Bronze source

**Exit criteria**: Fully automated daily pipeline, entity resolution working

---

## Release 6 — Recruiter Agents + Feedback Loop (Weeks 11–12)
**Goal**: GenAI recruiter agents + feedback learning

### Week 11
- [ ] Build Requirement Agent: paste JD → Requirement 360 (Bedrock)
- [ ] Build Matching Agent: Requirement 360 → ranked candidates with explanations
- [ ] Build Recruiter Agent: candidate summary + outreach draft + submission package
- [ ] Integrate agents into prajnov-recruiter React UI
- [ ] Build recruiter feedback capture: submitted/rejected + reason

### Week 12
- [ ] Build feedback analytics: rejection reason distribution per requirement type
- [ ] Build client-specific match signals: historical submissions → client preferences
- [ ] Build match quality dashboard: precision/recall on submitted vs placed
- [ ] Build candidate 360 completeness alerts: missing skills, expired certs
- [ ] Documentation + runbooks

**Exit criteria**: Full agent pipeline live, feedback loop capturing recruiter decisions

---

## Infrastructure Checklist

### AWS Resources to Provision
- [ ] S3 bucket: `prajnov-talent-lake` (Bronze/Silver/Gold)
- [ ] S3 bucket: `prajnov-talent-lake-artifacts` (Glue scripts, model artifacts)
- [ ] Aurora PostgreSQL Serverless v2 cluster (ap-south-1)
- [ ] Glue Data Catalog database: `prajnov_talent_lake`
- [ ] Glue IAM role with S3 + Glue + Bedrock permissions
- [ ] Lambda execution role
- [ ] Secrets Manager secrets: DB credentials, API keys
- [ ] CloudWatch log groups
- [ ] EventBridge rules for scheduled ingestion
- [ ] Step Functions state machines for pipelines
- [ ] Bedrock model access: Claude 3 Haiku, Titan Embeddings v2

### Estimated Monthly Cost (steady state)
| Service | Estimate |
|---|---|
| S3 (100GB lake) | ~$3/mo |
| Aurora Serverless v2 (0.5–2 ACU) | ~$50/mo |
| Glue ETL (daily jobs) | ~$10/mo |
| Bedrock (parsing + embeddings) | ~$20/mo |
| Lambda + Step Functions | ~$5/mo |
| **Total** | **~$90/mo** |
