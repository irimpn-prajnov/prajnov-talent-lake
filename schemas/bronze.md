# Bronze & Silver Layer Schemas

## Bronze — S3 Object Structure

Every Bronze object is immutable. Never overwrite. Append only.

### S3 Path Convention
```
s3://prajnov-talent-lake/bronze/{source}/{entity}/{YYYY}/{MM}/{DD}/{uuid}.json
```

### Required Metadata (all Bronze objects)
```json
{
  "_meta": {
    "source":             "recruiter_upload | github | signalhire | credly | indeed | web_search",
    "source_url":         "https://...",
    "ingestion_timestamp": "2026-09-17T05:00:00Z",
    "raw_hash":           "sha256:abc123...",
    "connector_version":  "1.0.0",
    "candidate_hint":     "John Smith",
    "provenance":         "recruiter:navin@prajnov.com",
    "retention_class":    "standard | pii | sensitive"
  }
}
```

---

### Bronze Paths

```
bronze/
├── candidates/
│   ├── resumes/          # PDF/DOCX originals + extracted text
│   ├── applications/     # Web form applications
│   └── referrals/        # Recruiter-entered referrals
│
├── requirements/
│   ├── email/            # Raw email JDs
│   └── manual/           # Recruiter-entered requirements
│
├── github/
│   ├── users/            # GET /users/{username}
│   └── repos/            # GET /users/{username}/repos
│
├── certifications/
│   ├── credly/           # Credly badge API responses
│   ├── aws/              # AWS cert verification responses
│   └── servicenow/       # ServiceNow cert verification
│
├── enrichment/
│   └── signalhire/       # SignalHire reveal responses
│
├── indeed/
│   └── applications/     # Indeed Partner API applicants
│
└── discovery/
    └── web_search/       # Google search results (URL discovery only)
```

---

### Bronze Object Examples

**Resume**
```json
{
  "_meta": {
    "source": "recruiter_upload",
    "ingestion_timestamp": "2026-09-17T05:00:00Z",
    "raw_hash": "sha256:...",
    "provenance": "recruiter:navin@prajnov.com",
    "retention_class": "pii"
  },
  "original_filename": "John_Smith_Resume.pdf",
  "file_type": "pdf",
  "s3_original_key": "bronze/candidates/resumes/2026/09/17/{uuid}.pdf",
  "extracted_text": "John Smith\nSenior ServiceNow Architect...",
  "extraction_method": "pdfplumber",
  "extraction_timestamp": "2026-09-17T05:00:01Z"
}
```

**GitHub User**
```json
{
  "_meta": {
    "source": "github",
    "source_url": "https://api.github.com/users/johnsmith-ai",
    "ingestion_timestamp": "2026-09-17T05:00:00Z",
    "connector_version": "1.0.0"
  },
  "login": "johnsmith-ai",
  "name": "John Smith",
  "bio": "AWS | GenAI | Python",
  "location": "Kansas City, MO",
  "company": "Accenture",
  "public_repos": 42,
  "followers": 180
}
```

**Credly Badge**
```json
{
  "_meta": {
    "source": "credly",
    "source_url": "https://www.credly.com/badges/{badge_id}",
    "ingestion_timestamp": "2026-09-17T05:00:00Z"
  },
  "badge_id": "abc-123",
  "badge_name": "AWS Certified Solutions Architect – Professional",
  "issuer": "Amazon Web Services",
  "issued_at": "2025-03-15",
  "expires_at": "2028-03-15",
  "earner_name": "John Smith",
  "skills": ["AWS", "Cloud Architecture", "Security"],
  "verification_url": "https://www.credly.com/badges/abc-123/public_url"
}
```

---

## Silver — Iceberg Tables

Normalized, deduplicated entities. Schema-on-write via Glue Data Catalog.

### silver.candidate
```
candidate_id        STRING      -- UUID, stable across sources
canonical_name      STRING
current_title       STRING      -- normalized title
current_company     STRING      -- normalized company
years_experience    DOUBLE
work_authorization  STRING
availability        STRING
location_city       STRING
location_state      STRING
data_quality_score  DOUBLE
discovery_status    STRING      -- discovered | enriched | verified | rejected
source_ids          ARRAY<STRING>   -- all bronze object IDs
created_at          TIMESTAMP
updated_at          TIMESTAMP
```

### silver.candidate_skill
```
id                  STRING
candidate_id        STRING
skill_name          STRING      -- normalized (from skill taxonomy)
raw_name            STRING      -- as found in source
years               DOUBLE
level               STRING
is_inferred         BOOLEAN
sources             ARRAY<STRING>
confidence          DOUBLE
evidence            STRING      -- JSON
```

### silver.candidate_certification

One row per candidate × certification. All cert types (CIS-CSM, CIS-VR, AWS, etc.) land here.
The `cert_code` column is the stable key used to join with `silver.certification_catalog`.

```
id                  STRING
candidate_id        STRING      -- FK → silver.candidate
cert_code           STRING      -- CIS-CSM | CIS-VR | AWS-SAA | etc.
cert_name           STRING      -- full display name
issuer              STRING
credential_id       STRING
issued_date         DATE
expiration_date     DATE
is_expired          BOOLEAN
verification_status STRING      -- discovered | verified | unverified | expired
source              STRING      -- web_search | credly | resume | recruiter_input
bronze_ref          STRING      -- S3 key of originating bronze record
last_verified       TIMESTAMP
```

**Flow:** bronze/discovery/web_search/certs/{cert}/ → silver.candidate (discovery_status=discovered)
         + silver.candidate_certification (verification_status=discovered, source=web_search)
         → enrichment (SignalHire / Credly) updates verification_status → verified

### silver.github_activity
```
id                  STRING
candidate_id        STRING
github_username     STRING
languages           STRING      -- JSON: {"Python": 45000, "TypeScript": 12000}
topics              ARRAY<STRING>
top_repos           STRING      -- JSON array
contribution_score  DOUBLE
last_active         DATE
inferred_skills     ARRAY<STRING>
```

### silver.requirement
```
requirement_id      STRING
title               STRING
customer_name       STRING
employment_type     STRING
work_mode           STRING
location_state      STRING
location_city       STRING
experience_min      INT
rate_max            DOUBLE
work_authorization  ARRAY<STRING>
mandatory_skills    ARRAY<STRING>
preferred_skills    ARRAY<STRING>
mandatory_certs     ARRAY<STRING>
status              STRING
created_at          TIMESTAMP
```

### silver.certification_catalog
```
cert_id             STRING
name                STRING
code                STRING
issuer              STRING
category            STRING
level               STRING
skills              ARRAY<STRING>
validity_months     INT
verification_url    STRING
```

### silver.skill_taxonomy
```
skill_id            STRING
name                STRING      -- canonical: "ServiceNow CSM"
category            STRING
parent_skill_id     STRING
aliases             ARRAY<STRING>
related_skills      ARRAY<STRING>
```
