# Gold Layer Schemas — Candidate 360 & Requirement 360

## Data Flow Summary

```
bronze/discovery/web_search/certs/{cert}/   (LinkedIn URL + cert hint)
  ↓ silver ETL
silver.candidate            (discovery_status = discovered)
silver.candidate_certification  (verification_status = discovered, one table ALL cert types)
  ↓ enrichment (SignalHire / Credly)
silver.candidate            (discovery_status = enriched/verified)
silver.candidate_certification  (verification_status = verified)
  ↓ gold ETL
gold.candidate              ←─ the candidates dimension table
gold.candidate_certification←─ all certs (CIS-CSM, CIS-VR, AWS, etc.) for every candidate
gold.candidate_external_identity ←─ linkedin_url, github_url, credly profile
```

---

## Candidate 360

The canonical candidate record assembled from all sources.
Every fact carries source, confidence, and evidence.
```sql
-- Core identity
CREATE TABLE candidate (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_name      TEXT NOT NULL,
    current_title       TEXT,
    current_company     TEXT,
    years_experience    NUMERIC(4,1),
    work_authorization  TEXT,           -- USC, GC, H1B, OPT, TN, EAD...
    availability        TEXT,           -- immediate, 2_weeks, 1_month, employed
    expected_rate_min   NUMERIC(8,2),
    expected_rate_max   NUMERIC(8,2),
    rate_type           TEXT,           -- hourly, annual
    work_mode_pref      TEXT[],         -- remote, hybrid, onsite
    created_at          TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now(),
    data_quality_score  NUMERIC(3,2),   -- 0.0 - 1.0
    is_active           BOOLEAN DEFAULT true
);

-- Contact info (PII — access controlled)
CREATE TABLE candidate_contact (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id    UUID REFERENCES candidate(id),
    type            TEXT,   -- email, phone, linkedin_url, github_url, website
    value           TEXT,
    is_primary      BOOLEAN DEFAULT false,
    source          TEXT,
    confidence      NUMERIC(3,2),
    last_verified   TIMESTAMPTZ,
    revealed_by     TEXT    -- signalhire, recruiter_input, resume, application
);

-- Location
CREATE TABLE candidate_location (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id    UUID REFERENCES candidate(id),
    city            TEXT,
    state           TEXT,
    country         TEXT DEFAULT 'US',
    timezone        TEXT,
    is_current      BOOLEAN DEFAULT true,
    source          TEXT,
    confidence      NUMERIC(3,2)
);

-- Work experience
CREATE TABLE candidate_experience (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id    UUID REFERENCES candidate(id),
    company         TEXT,
    title           TEXT,
    start_date      DATE,
    end_date        DATE,           -- NULL = current
    is_current      BOOLEAN DEFAULT false,
    description     TEXT,
    location        TEXT,
    employment_type TEXT,           -- full_time, contract, c2c, part_time
    source          TEXT,
    confidence      NUMERIC(3,2)
);

-- Education
CREATE TABLE candidate_education (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id    UUID REFERENCES candidate(id),
    institution     TEXT,
    degree          TEXT,
    field_of_study  TEXT,
    start_year      INT,
    end_year        INT,
    source          TEXT
);

-- Skills (normalized)
CREATE TABLE candidate_skill (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id    UUID REFERENCES candidate(id),
    skill_id        UUID REFERENCES skill(id),
    skill_name      TEXT,           -- normalized name
    raw_name        TEXT,           -- as it appeared in source
    years           NUMERIC(4,1),
    level           TEXT,           -- beginner, intermediate, advanced, expert
    is_inferred     BOOLEAN DEFAULT false,  -- true = derived, not claimed
    source          TEXT[],         -- resume, github, signalhire...
    confidence      NUMERIC(3,2),
    evidence        JSONB           -- {"github_repos": [...], "resume_mentions": 3}
);

-- Certifications
-- Single table for ALL certification types across ALL issuers (ServiceNow, AWS, Microsoft, etc.)
-- cert_code is the stable key: CIS-CSM, CIS-VR, AWS-SAA-C03, AZ-900, etc.
CREATE TABLE candidate_certification (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id        UUID REFERENCES candidate(id),
    certification_id    UUID REFERENCES certification(id),
    cert_code           TEXT,               -- CIS-CSM | CIS-VR | AWS-SAA | etc.
    cert_name           TEXT,
    issuer              TEXT,
    credential_id       TEXT,
    issued_date         DATE,
    expiration_date     DATE,
    is_expired          BOOLEAN GENERATED ALWAYS AS (expiration_date < CURRENT_DATE) STORED,
    verification_url    TEXT,
    verification_status TEXT,       -- discovered | verified | unverified | expired | pending
    source              TEXT,       -- web_search | credly | resume | recruiter_input
    last_verified       TIMESTAMPTZ
);

-- Projects
CREATE TABLE candidate_project (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id    UUID REFERENCES candidate(id),
    name            TEXT,
    description     TEXT,
    skills          TEXT[],
    domain          TEXT,
    github_url      TEXT,
    start_date      DATE,
    end_date        DATE,
    source          TEXT
);

-- External identities (LinkedIn URL, GitHub username, etc.)
CREATE TABLE candidate_external_identity (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id    UUID REFERENCES candidate(id),
    platform        TEXT,           -- linkedin, github, credly, indeed
    profile_url     TEXT,
    external_id     TEXT,
    discovered_from TEXT,           -- google_search, recruiter_input, resume
    discovered_at   TIMESTAMPTZ,
    last_checked    TIMESTAMPTZ,
    is_verified     BOOLEAN DEFAULT false
);

-- Source documents (resumes, applications)
CREATE TABLE candidate_document (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id    UUID REFERENCES candidate(id),
    type            TEXT,           -- resume, cover_letter, portfolio
    s3_key          TEXT,           -- bronze S3 path
    filename        TEXT,
    file_type       TEXT,           -- pdf, docx, doc
    uploaded_at     TIMESTAMPTZ,
    uploaded_by     TEXT,
    is_latest       BOOLEAN DEFAULT true,
    parse_status    TEXT            -- pending, parsed, failed
);

-- Embeddings (pgvector)
CREATE TABLE candidate_embedding (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id    UUID REFERENCES candidate(id),
    embedding_type  TEXT,           -- full_profile, skills, experience, summary
    model           TEXT,           -- titan-embed-v2, all-MiniLM-L6-v2
    vector          vector(1536),
    generated_at    TIMESTAMPTZ,
    source_text     TEXT            -- text that was embedded
);
CREATE INDEX ON candidate_embedding USING ivfflat (vector vector_cosine_ops);
```

---

## Requirement 360

The canonical job requirement record parsed from JDs.

```sql
-- Core requirement
CREATE TABLE requirement (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title               TEXT NOT NULL,
    customer_id         UUID REFERENCES customer(id),
    customer_name       TEXT,
    hiring_manager      TEXT,
    pipeline_id         TEXT,
    source              TEXT,           -- email, manual, ats
    source_ref          TEXT,           -- email_id, ticket_id
    raw_jd_s3_key       TEXT,           -- bronze S3 path
    status              TEXT DEFAULT 'open',  -- open, filled, closed, on_hold
    priority            TEXT DEFAULT 'normal',
    created_at          TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now(),
    filled_at           TIMESTAMPTZ,
    created_by          TEXT
);

-- Parsed requirement details
CREATE TABLE requirement_detail (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    requirement_id      UUID REFERENCES requirement(id),
    role_summary        TEXT,
    employment_type     TEXT,           -- contract, c2c, full_time, part_time
    work_mode           TEXT,           -- remote, hybrid, onsite
    location_city       TEXT,
    location_state      TEXT,
    location_flexible   BOOLEAN DEFAULT false,
    experience_min      INT,
    experience_max      INT,
    rate_min            NUMERIC(8,2),
    rate_max            NUMERIC(8,2),
    rate_type           TEXT,           -- hourly, annual
    work_authorization  TEXT[],         -- USC, GC, H1B...
    start_date          DATE,
    duration_months     INT,
    parsed_at           TIMESTAMPTZ,
    parsed_by           TEXT            -- llm_model name
);

-- Required skills (mandatory)
CREATE TABLE requirement_skill (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    requirement_id      UUID REFERENCES requirement(id),
    skill_id            UUID REFERENCES skill(id),
    skill_name          TEXT,
    is_mandatory        BOOLEAN DEFAULT true,
    years_required      INT,
    notes               TEXT
);

-- Required certifications
CREATE TABLE requirement_certification (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    requirement_id      UUID REFERENCES requirement(id),
    certification_id    UUID REFERENCES certification(id),
    cert_name           TEXT,
    is_mandatory        BOOLEAN DEFAULT true,
    notes               TEXT
);

-- Requirement embeddings
CREATE TABLE requirement_embedding (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    requirement_id  UUID REFERENCES requirement(id),
    embedding_type  TEXT,           -- full_jd, skills, summary
    model           TEXT,
    vector          vector(1536),
    generated_at    TIMESTAMPTZ,
    source_text     TEXT
);
CREATE INDEX ON requirement_embedding USING ivfflat (vector vector_cosine_ops);
```

---

## Skill Knowledge Graph

```sql
-- Canonical skill catalog
CREATE TABLE skill (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT UNIQUE NOT NULL,   -- normalized: "ServiceNow CSM"
    category        TEXT,                   -- platform, language, cloud, framework...
    parent_skill_id UUID REFERENCES skill(id),
    aliases         TEXT[],                 -- ["SNOW CSM", "ServiceNow Customer Service"]
    related_skills  UUID[],
    is_active       BOOLEAN DEFAULT true
);

-- Certification catalog
CREATE TABLE certification (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name                TEXT NOT NULL,
    code                TEXT,               -- "CIS-CSM", "AWS-SAA-C03"
    issuer              TEXT,               -- "ServiceNow", "Amazon Web Services"
    category            TEXT,               -- cloud, platform, agile, security...
    level               TEXT,               -- associate, professional, expert, specialist
    skills              UUID[],             -- skills this cert validates
    validity_months     INT,                -- NULL = no expiry
    verification_url    TEXT,
    credly_org_id       TEXT,
    is_active           BOOLEAN DEFAULT true
);
```

---

## Recruiting Operations

```sql
-- Match record
CREATE TABLE candidate_requirement_match (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id        UUID REFERENCES candidate(id),
    requirement_id      UUID REFERENCES requirement(id),
    overall_score       NUMERIC(4,3),       -- 0.000 - 1.000
    semantic_score      NUMERIC(4,3),
    skill_score         NUMERIC(4,3),
    cert_score          NUMERIC(4,3),
    experience_score    NUMERIC(4,3),
    mandatory_met       BOOLEAN,
    mandatory_gaps      TEXT[],
    match_explanation   TEXT,               -- LLM-generated
    matched_at          TIMESTAMPTZ,
    model_version       TEXT,
    UNIQUE(candidate_id, requirement_id)
);

-- Submission tracking
CREATE TABLE submission (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id        UUID REFERENCES candidate(id),
    requirement_id      UUID REFERENCES requirement(id),
    status              TEXT,   -- selected, contacted, responded, screening,
                                -- submitted, interview, offer, placed, rejected, closed
    submitted_at        TIMESTAMPTZ,
    submitted_by        TEXT,
    rate_submitted      NUMERIC(8,2),
    client_feedback     TEXT,
    rejection_reason    TEXT,
    placed_at           TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ
);

-- Recruiter feedback on AI suggestions
CREATE TABLE recruiter_feedback (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    match_id            UUID REFERENCES candidate_requirement_match(id),
    recruiter_id        TEXT,
    action              TEXT,   -- submitted, rejected, bookmarked, skipped
    rejection_reason    TEXT,   -- wrong_skill, insufficient_exp, wrong_location,
                                -- rate_too_high, cert_missing, duplicate, unavailable
    notes               TEXT,
    created_at          TIMESTAMPTZ DEFAULT now()
);
```
