-- Gold layer DDL for Aurora PostgreSQL 16
-- Run once after Aurora cluster is provisioned:
--   psql $DATABASE_URL -f infra/rds/schema.sql

-- Add embedding_titan column if not exists (Bedrock Titan v2, 1024-dim)
ALTER TABLE candidates ADD COLUMN IF NOT EXISTS embedding_titan vector(1024);
CREATE INDEX IF NOT EXISTS idx_cand_embedding_titan
    ON candidates USING ivfflat (embedding_titan vector_cosine_ops) WITH (lists = 100);

-- ── Extensions ────────────────────────────────────────────────────────────────

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS vector;

-- ── Skill catalog ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS skill (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT UNIQUE NOT NULL,
    category        TEXT,
    parent_skill_id UUID REFERENCES skill(id),
    aliases         TEXT[],
    related_skills  UUID[],
    is_active       BOOLEAN DEFAULT true
);

-- ── Certification catalog ─────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS certification (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name             TEXT NOT NULL,
    code             TEXT UNIQUE,
    issuer           TEXT,
    category         TEXT,
    level            TEXT,
    skills           UUID[],
    validity_months  INT,
    verification_url TEXT,
    credly_org_id    TEXT,
    is_active        BOOLEAN DEFAULT true
);

-- ── Candidate core ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS candidate (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_name      TEXT NOT NULL,
    first_name          TEXT,
    last_name           TEXT,
    current_title       TEXT,
    current_company     TEXT,
    years_experience    NUMERIC(4,1),
    summary             TEXT,
    work_authorization  TEXT,
    availability        TEXT,
    expected_rate_min   NUMERIC(8,2),
    expected_rate_max   NUMERIC(8,2),
    rate_type           TEXT,
    work_mode_pref      TEXT[],
    data_quality_score  NUMERIC(3,2),
    discovery_status    TEXT,           -- discovered | enriched | verified
    openai_confidence   TEXT,           -- high | medium | low
    enriched_by         TEXT,
    source_cert         TEXT,           -- originating cert pipeline
    badge_count         INT,
    is_active           BOOLEAN DEFAULT true,
    created_at          TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_candidate_source_cert    ON candidate(source_cert);
CREATE INDEX IF NOT EXISTS idx_candidate_discovery      ON candidate(discovery_status);
CREATE INDEX IF NOT EXISTS idx_candidate_name           ON candidate(canonical_name);

-- ── Candidate location ────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS candidate_location (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id UUID NOT NULL REFERENCES candidate(id) ON DELETE CASCADE,
    city         TEXT,
    state        TEXT,
    country      TEXT DEFAULT 'US',
    location_raw TEXT,
    is_current   BOOLEAN DEFAULT true,
    source       TEXT,
    confidence   NUMERIC(3,2)
);

CREATE INDEX IF NOT EXISTS idx_candidate_location_cid     ON candidate_location(candidate_id);
CREATE INDEX IF NOT EXISTS idx_candidate_location_country ON candidate_location(country);
CREATE INDEX IF NOT EXISTS idx_candidate_location_state   ON candidate_location(state);

-- ── External identities ───────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS candidate_external_identity (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id    UUID NOT NULL REFERENCES candidate(id) ON DELETE CASCADE,
    platform        TEXT NOT NULL,      -- linkedin | credly | signalhire | github
    profile_url     TEXT,
    external_id     TEXT,
    discovered_from TEXT,
    discovered_at   TIMESTAMPTZ DEFAULT now(),
    last_checked    TIMESTAMPTZ,
    is_verified     BOOLEAN DEFAULT false
);

CREATE INDEX IF NOT EXISTS idx_ext_identity_cid      ON candidate_external_identity(candidate_id);
CREATE INDEX IF NOT EXISTS idx_ext_identity_platform ON candidate_external_identity(platform);
CREATE UNIQUE INDEX IF NOT EXISTS idx_ext_identity_url ON candidate_external_identity(candidate_id, platform);

-- ── Certifications ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS candidate_certification (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id        UUID NOT NULL REFERENCES candidate(id) ON DELETE CASCADE,
    certification_id    UUID REFERENCES certification(id),
    cert_code           TEXT NOT NULL,
    cert_name           TEXT,
    issuer              TEXT,
    credential_id       TEXT,
    issued_date         DATE,
    expiration_date     DATE,
    is_expired          BOOLEAN GENERATED ALWAYS AS (expiration_date IS NOT NULL AND expiration_date < CURRENT_DATE) STORED,
    verification_url    TEXT,
    verification_status TEXT DEFAULT 'verified',
    source              TEXT,
    last_verified       TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cand_cert_cid       ON candidate_certification(candidate_id);
CREATE INDEX IF NOT EXISTS idx_cand_cert_code      ON candidate_certification(cert_code);

-- ── Skills ────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS candidate_skill (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id UUID NOT NULL REFERENCES candidate(id) ON DELETE CASCADE,
    skill_id     UUID REFERENCES skill(id),
    skill_name   TEXT NOT NULL,
    raw_name     TEXT,
    years        NUMERIC(4,1),
    level        TEXT,
    is_inferred  BOOLEAN DEFAULT false,
    sources      TEXT[],
    confidence   NUMERIC(3,2),
    evidence     JSONB
);

CREATE INDEX IF NOT EXISTS idx_cand_skill_cid  ON candidate_skill(candidate_id);
CREATE INDEX IF NOT EXISTS idx_cand_skill_name ON candidate_skill(skill_name);

-- ── Work experience ───────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS candidate_experience (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id UUID NOT NULL REFERENCES candidate(id) ON DELETE CASCADE,
    company      TEXT,
    title        TEXT,
    start_year   INT,
    end_year     INT,
    is_current   BOOLEAN DEFAULT false,
    location     TEXT,
    description  TEXT,
    source       TEXT,
    confidence   NUMERIC(3,2)
);

CREATE INDEX IF NOT EXISTS idx_cand_exp_cid ON candidate_experience(candidate_id);

-- ── Education ─────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS candidate_education (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id UUID NOT NULL REFERENCES candidate(id) ON DELETE CASCADE,
    institution  TEXT,
    degree       TEXT,
    field        TEXT,
    start_year   INT,
    end_year     INT,
    source       TEXT
);

CREATE INDEX IF NOT EXISTS idx_cand_edu_cid ON candidate_education(candidate_id);

-- ── Embeddings (pgvector) ─────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS candidate_embedding (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id   UUID NOT NULL REFERENCES candidate(id) ON DELETE CASCADE,
    embedding_type TEXT,           -- full_profile | skills | experience | summary
    model          TEXT,
    vector         vector(1024),   -- Bedrock Titan Embeddings v2 native dimension
    generated_at   TIMESTAMPTZ DEFAULT now(),
    source_text    TEXT
);

CREATE INDEX IF NOT EXISTS idx_embedding_cid ON candidate_embedding(candidate_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_embedding_unique ON candidate_embedding(candidate_id, embedding_type);
-- Build ivfflat after bulk load: lists = sqrt(total_rows), then REINDEX
CREATE INDEX IF NOT EXISTS idx_embedding_vec ON candidate_embedding USING ivfflat (vector vector_cosine_ops) WITH (lists = 50);

-- ── Requirement tables (for AIR matching) ───────────────────────────────────

CREATE TABLE IF NOT EXISTS requirement (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title               TEXT NOT NULL,
    customer_name       TEXT,
    employment_type     TEXT,
    work_mode           TEXT,
    location_city       TEXT,
    location_state      TEXT,
    experience_min      INT,
    experience_max      INT,
    rate_min            NUMERIC(8,2),
    rate_max            NUMERIC(8,2),
    rate_type           TEXT,
    work_authorization  TEXT[],
    mandatory_skills    TEXT[],
    preferred_skills    TEXT[],
    mandatory_certs     TEXT[],
    raw_jd_s3_key       TEXT,
    status              TEXT DEFAULT 'open',
    created_at          TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS requirement_embedding (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    requirement_id  UUID NOT NULL REFERENCES requirement(id) ON DELETE CASCADE,
    embedding_type  TEXT,           -- full_jd | skills | summary
    model           TEXT,
    vector          vector(1024),
    generated_at    TIMESTAMPTZ DEFAULT now(),
    source_text     TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_req_embedding_unique ON requirement_embedding(requirement_id, embedding_type);
CREATE INDEX IF NOT EXISTS idx_req_embedding_vec ON requirement_embedding USING ivfflat (vector vector_cosine_ops) WITH (lists = 10);

-- ── Match + recruiter ops ─────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS candidate_requirement_match (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id      UUID NOT NULL REFERENCES candidate(id),
    requirement_id    UUID NOT NULL REFERENCES requirement(id),
    overall_score     NUMERIC(4,3),
    semantic_score    NUMERIC(4,3),
    skill_score       NUMERIC(4,3),
    cert_score        NUMERIC(4,3),
    experience_score  NUMERIC(4,3),
    mandatory_met     BOOLEAN,
    mandatory_gaps    TEXT[],
    match_explanation TEXT,
    matched_at        TIMESTAMPTZ DEFAULT now(),
    model_version     TEXT,
    UNIQUE(candidate_id, requirement_id)
);

CREATE TABLE IF NOT EXISTS submission (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id     UUID NOT NULL REFERENCES candidate(id),
    requirement_id   UUID NOT NULL REFERENCES requirement(id),
    status           TEXT,
    submitted_at     TIMESTAMPTZ,
    submitted_by     TEXT,
    rate_submitted   NUMERIC(8,2),
    client_feedback  TEXT,
    rejection_reason TEXT,
    placed_at        TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS recruiter_feedback (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    match_id         UUID REFERENCES candidate_requirement_match(id),
    recruiter_id     TEXT,
    action           TEXT,   -- submitted | rejected | bookmarked | skipped
    rejection_reason TEXT,
    notes            TEXT,
    created_at       TIMESTAMPTZ DEFAULT now()
);

-- ── Recruiting Pipeline ─────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS pipeline_requirement (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       TEXT NOT NULL,
    title           TEXT NOT NULL,
    client_name     TEXT,
    client_id       TEXT,                   -- FK to clients table (future)
    employment_type TEXT DEFAULT 'contract',-- contract | fulltime | c2c
    work_mode       TEXT,                   -- remote | hybrid | onsite
    location        TEXT,
    rate_min        NUMERIC(8,2),
    rate_max        NUMERIC(8,2),
    rate_type       TEXT DEFAULT 'hourly',
    experience_min  INT,
    mandatory_skills TEXT[],
    mandatory_certs  TEXT[],
    raw_jd_text     TEXT,
    job_id          TEXT,                   -- link to staffing jobs table
    priority        TEXT DEFAULT 'normal',  -- urgent | high | normal | low
    due_date        DATE,
    status          TEXT DEFAULT 'open',    -- open | on_hold | closed | filled
    owner           TEXT,                   -- recruiter email
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_req_tenant  ON pipeline_requirement(tenant_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_req_status  ON pipeline_requirement(status);
CREATE INDEX IF NOT EXISTS idx_pipeline_req_owner   ON pipeline_requirement(owner);

-- Stage enum values (enforced in app layer for flexibility):
-- found | shortlisted | contacted | responded | in_play |
-- initial_discussion | submitted | client_feedback |
-- interview | offered | docs_complete | placed
-- Terminal: rejected_by_client | withdrew | not_interested | on_hold

CREATE TABLE IF NOT EXISTS pipeline_candidate (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    requirement_id      UUID NOT NULL REFERENCES pipeline_requirement(id) ON DELETE CASCADE,
    candidate_id        UUID REFERENCES candidate(id),  -- NULL if external (SignalHire only)
    name                TEXT NOT NULL,
    title               TEXT,
    company             TEXT,
    location            TEXT,
    linkedin_url        TEXT,
    email               TEXT,
    phone               TEXT,
    match_score         NUMERIC(4,3),
    source              TEXT,               -- pgvector | signalhire | manual
    stage               TEXT NOT NULL DEFAULT 'found',
    stage_history       JSONB DEFAULT '[]', -- [{stage, ts, note, by}]
    next_action         TEXT,               -- recruiter-facing prompt
    next_action_due     DATE,
    notes               TEXT,
    is_stale            BOOLEAN DEFAULT false,
    added_by            TEXT,
    added_at            TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_cand_req    ON pipeline_candidate(requirement_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_cand_stage  ON pipeline_candidate(stage);
CREATE INDEX IF NOT EXISTS idx_pipeline_cand_cid    ON pipeline_candidate(candidate_id);

-- Funnel counts view — one row per requirement with counts per stage
CREATE OR REPLACE VIEW v_pipeline_funnel AS
SELECT
    r.id                                                        AS requirement_id,
    r.title,
    r.client_name,
    r.priority,
    r.due_date,
    r.status,
    r.owner,
    COUNT(*) FILTER (WHERE c.stage = 'found')                   AS found,
    COUNT(*) FILTER (WHERE c.stage = 'shortlisted')             AS shortlisted,
    COUNT(*) FILTER (WHERE c.stage = 'contacted')               AS contacted,
    COUNT(*) FILTER (WHERE c.stage = 'responded')               AS responded,
    COUNT(*) FILTER (WHERE c.stage = 'in_play')                 AS in_play,
    COUNT(*) FILTER (WHERE c.stage = 'initial_discussion')      AS initial_discussion,
    COUNT(*) FILTER (WHERE c.stage = 'submitted')               AS submitted,
    COUNT(*) FILTER (WHERE c.stage = 'client_feedback')         AS client_feedback,
    COUNT(*) FILTER (WHERE c.stage = 'interview')               AS interview,
    COUNT(*) FILTER (WHERE c.stage = 'offered')                 AS offered,
    COUNT(*) FILTER (WHERE c.stage = 'docs_complete')           AS docs_complete,
    COUNT(*) FILTER (WHERE c.stage = 'placed')                  AS placed,
    COUNT(*) FILTER (WHERE c.stage IN ('rejected_by_client','withdrew','not_interested')) AS dropped,
    COUNT(*)                                                    AS total
FROM pipeline_requirement r
LEFT JOIN pipeline_candidate c ON c.requirement_id = r.id
GROUP BY r.id, r.title, r.client_name, r.priority, r.due_date, r.status, r.owner;

-- ── Useful views ──────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW v_candidate_us AS
SELECT
    c.id,
    c.canonical_name,
    c.current_title,
    c.current_company,
    c.years_experience,
    c.summary,
    c.source_cert,
    c.discovery_status,
    c.openai_confidence,
    c.data_quality_score,
    l.city,
    l.state,
    l.country,
    l.location_raw,
    li.profile_url  AS linkedin_url,
    cr.profile_url  AS credly_url,
    c.created_at
FROM candidate c
LEFT JOIN candidate_location l          ON l.candidate_id = c.id AND l.is_current = true
LEFT JOIN candidate_external_identity li ON li.candidate_id = c.id AND li.platform = 'linkedin'
LEFT JOIN candidate_external_identity cr ON cr.candidate_id = c.id AND cr.platform = 'credly'
WHERE l.country = 'US';

CREATE OR REPLACE VIEW v_candidate_full AS
SELECT
    c.id,
    c.canonical_name,
    c.current_title,
    c.current_company,
    c.years_experience,
    c.summary,
    c.source_cert,
    c.badge_count,
    c.discovery_status,
    c.openai_confidence,
    c.data_quality_score,
    l.city,
    l.state,
    l.country,
    l.location_raw,
    li.profile_url  AS linkedin_url,
    cr.profile_url  AS credly_url,
    sh.profile_url  AS signalhire_url,
    (SELECT COUNT(*) FROM candidate_skill    sk WHERE sk.candidate_id = c.id) AS skill_count,
    (SELECT COUNT(*) FROM candidate_experience ex WHERE ex.candidate_id = c.id) AS experience_count,
    (SELECT COUNT(*) FROM candidate_certification cc WHERE cc.candidate_id = c.id) AS cert_count,
    c.created_at
FROM candidate c
LEFT JOIN candidate_location l           ON l.candidate_id = c.id AND l.is_current = true
LEFT JOIN candidate_external_identity li ON li.candidate_id = c.id AND li.platform = 'linkedin'
LEFT JOIN candidate_external_identity cr ON cr.candidate_id = c.id AND cr.platform = 'credly'
LEFT JOIN candidate_external_identity sh ON sh.candidate_id = c.id AND sh.platform = 'signalhire';
