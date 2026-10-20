# Prajnov Talent Intelligence Lake

AI-powered candidate intelligence platform built on AWS lakehouse architecture.
Combines multi-source candidate data, certification intelligence, technical evidence,
and GenAI matching to power the Prajnov AI Recruiter.

---

## Workspace Structure

```
prajnov-talent-lake/
├── docs/                        # Architecture, design decisions, API research
│   ├── architecture.md          # Full system architecture
│   ├── data-sources.md          # Source APIs, access models, rate limits
│   ├── entity-resolution.md     # Deduplication & identity matching design
│   └── skill-taxonomy.md        # Skill/certification knowledge graph design
│
├── schemas/                     # Data models for all layers
│   ├── bronze.md                # Raw ingestion schemas
│   ├── silver.md                # Normalized entity schemas
│   ├── gold.md                  # Candidate 360 + Requirement 360
│   └── vectors.md               # Embedding schemas
│
├── ingestion/                   # Source connectors
│   ├── github/                  # GitHub REST API connector
│   ├── indeed/                  # Indeed Partner API connector
│   ├── credly/                  # Credly badge verification
│   ├── signalhire/              # SignalHire enrichment
│   ├── resume/                  # Resume/PDF ingestion pipeline
│   └── web_search/              # Google search → LinkedIn URL discovery
│
├── processing/                  # Bronze → Silver → Gold transforms
│   ├── parsers/                 # Resume, JD, profile parsers
│   ├── normalizers/             # Skills, titles, companies, locations
│   ├── entity_resolution/       # Identity matching & deduplication
│   └── enrichment/              # SignalHire, certification verification
│
├── search/                      # Hybrid search & matching engine
│   ├── embeddings/              # Embedding generation pipelines
│   ├── retrieval/               # pgvector / OpenSearch retrieval
│   └── reranker/                # Cross-encoder + LLM reranking
│
├── agents/                      # GenAI recruiter agents
│   ├── requirement_agent/       # JD → Requirement 360
│   ├── matching_agent/          # Candidate retrieval + ranking
│   └── recruiter_agent/         # Outreach, summaries, submissions
│
└── infra/                       # AWS infrastructure (Terraform)
    ├── s3/                      # Bucket structure + lifecycle policies
    ├── glue/                    # Glue catalog + ETL jobs
    ├── rds/                     # Aurora PostgreSQL schema
    └── iam/                     # Roles + policies
```

---

## Releases

| Release | Scope | Status |
|---|---|---|
| R1 | S3 lakehouse + resume ingestion + Candidate 360 schema | Planning |
| R2 | Requirement 360 + skill taxonomy + basic search | Planned |
| R3 | Embeddings + hybrid matching | Planned |
| R4 | GitHub + Credly + SignalHire enrichment | Planned |
| R5 | Indeed integration + automated ingestion | Planned |
| R6 | Recruiter agents + feedback loop + analytics | Planned |

---

## Quick Links

- [Architecture](docs/architecture.md)
- [Data Sources](docs/data-sources.md)
- [Schemas](schemas/)
- [Skill Taxonomy](docs/skill-taxonomy.md)
