# ── Shared job defaults ───────────────────────────────────────────────────────

locals {
  common_args = {
    "--BUCKET"                    = var.bucket
    "--AWS_REGION"                = var.aws_region
    "--PGVECTOR_HOST"             = var.pgvector_host
    "--PGVECTOR_DB"               = var.pgvector_db
    "--PGVECTOR_USER"             = var.pgvector_user
    "--PGVECTOR_PASSWORD"         = var.pgvector_password
    "--extra-py-files"            = "s3://${var.bucket}/glue-scripts/common.py"
    "--additional-python-modules" = "requests,psycopg2-binary,boto3"
    "--job-language"              = "python"
    "--TempDir"                   = "s3://${var.bucket}/glue-tmp/"
    "--enable-job-insights"       = "true"
  }

  brightdata_args = merge(local.common_args, {
    "--BRIGHTDATA_BUCKET"         = var.brightdata_bucket
    "--additional-python-modules" = "psycopg2-binary,boto3,pyarrow,ijson"
  })
}

# ── Job 1: Bronze — Scrape all 19 certs from Credly → one file per cert ──────
# Runs first. Incremental — only fetches new earners since last run.
# Output: s3://prajnov-talent-lake/bronze/discovery/credly/certs/{slug}/consolidated/all.json

resource "aws_glue_job" "credly_bronze_ingest" {
  name         = "prajnov-credly-bronze-ingest"
  role_arn     = aws_iam_role.glue.arn
  description  = "Scrape all 19 SN certs from Credly directory → one consolidated S3 file per cert"
  glue_version = "4.0"

  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "s3://${var.bucket}/glue-scripts/credly_bronze_ingest.py"
  }

  default_arguments = merge(local.common_args, {
    "--CERTS" = join(",", var.certs)
  })

  max_capacity = 0.0625
  timeout      = 180   # 3 hours — 19 certs × ~10 min each

  tags = {
    project = "prajnov-talent-lake"
    stage   = "bronze"
    layer   = "ingestion"
  }
}

# ── Job 2: Silver — Match Credly records against BrightData, enrich candidates

resource "aws_glue_job" "credly_silver_enrich" {
  name         = "prajnov-credly-silver-enrich"
  role_arn     = aws_iam_role.glue.arn
  description  = "Match Credly bronze → BrightData candidates, enrich verified_certifications"
  glue_version = "4.0"

  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "s3://${var.bucket}/glue-scripts/credly_silver_enrich.py"
  }

  default_arguments = local.common_args

  max_capacity = 0.0625
  timeout      = 60

  tags = {
    project = "prajnov-talent-lake"
    stage   = "silver"
    layer   = "enrichment"
  }
}

# ── Job 3: DQ Fix — Normalize dirty certs, dedup, fix locations ──────────────

resource "aws_glue_job" "shared_dq_fix" {
  name         = "prajnov-shared-dq-fix"
  role_arn     = aws_iam_role.glue.arn
  description  = "Normalize dirty cert codes, dedup by linkedin_id, fix locations"
  glue_version = "4.0"

  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "s3://${var.bucket}/glue-scripts/shared_dq_fix.py"
  }

  default_arguments = local.common_args

  max_capacity = 0.0625
  timeout      = 60

  tags = {
    project = "prajnov-talent-lake"
    stage   = "silver"
    layer   = "dq"
  }
}

# ── Job 4: Gold Merge — Entity resolution → gold candidates table ─────────────

resource "aws_glue_job" "shared_gold_merge" {
  name         = "prajnov-shared-gold-merge"
  role_arn     = aws_iam_role.glue.arn
  description  = "Entity resolution + merge all sources into gold candidates table"
  glue_version = "4.0"

  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "s3://${var.bucket}/glue-scripts/shared_gold_merge.py"
  }

  default_arguments = local.common_args

  max_capacity = 0.0625
  timeout      = 60

  tags = {
    project = "prajnov-talent-lake"
    stage   = "gold"
    layer   = "merge"
  }
}

# ── Job 8: Shared Embed Upsert — Bedrock Titan v2 → embedding_titan ─────────

resource "aws_glue_job" "shared_embed_upsert" {
  name         = "prajnov-shared-embed-upsert"
  role_arn     = aws_iam_role.glue.arn
  description  = "Assemble candidate profile text → Bedrock Titan v2 → embedding_titan (1024-dim)"
  glue_version = "4.0"

  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "s3://${var.bucket}/glue-scripts/shared_embed_upsert.py"
  }

  default_arguments = local.common_args

  max_capacity = 0.0625
  timeout      = 120   # 2 hours — 90k candidates × Bedrock latency

  tags = {
    project = "prajnov-talent-lake"
    stage   = "gold"
    layer   = "embedding"
  }
}

# ── BrightData pipeline ───────────────────────────────────────────────────────
#
#  Job 5: Bronze — Scan prajnov-brightdata-datasets, stream any new dataset
#                  files into bronze JSONL batches. Incremental via ETag watermark.
#  Job 6: Silver — Normalize bronze JSONL → 5 silver Parquet tables.
#  Job 7: Gold   — Upsert silver into candidates DB. Entity resolution by linkedin_id.

resource "aws_glue_job" "brightdata_bronze_ingest" {
  name         = "prajnov-brightdata-bronze-ingest"
  role_arn     = aws_iam_role.glue.arn
  description  = "Scan BrightData datasets bucket → bronze JSONL batches (incremental by ETag)"
  glue_version = "4.0"

  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "s3://${var.bucket}/glue-scripts/brightdata_bronze_ingest.py"
  }

  default_arguments = local.brightdata_args

  max_capacity = 0.0625
  timeout      = 120   # 2 hours — 1.8 GB JSON file streams slowly

  tags = {
    project = "prajnov-talent-lake"
    stage   = "bronze"
    layer   = "ingestion"
    source  = "brightdata"
  }
}

resource "aws_glue_job" "brightdata_silver_normalize" {
  name         = "prajnov-brightdata-silver-normalize"
  role_arn     = aws_iam_role.glue.arn
  description  = "Normalize BrightData bronze JSONL → 5 silver Parquet tables"
  glue_version = "4.0"

  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "s3://${var.bucket}/glue-scripts/brightdata_silver_normalize.py"
  }

  default_arguments = local.brightdata_args

  max_capacity = 0.0625
  timeout      = 60

  tags = {
    project = "prajnov-talent-lake"
    stage   = "silver"
    layer   = "normalize"
    source  = "brightdata"
  }
}

resource "aws_glue_job" "brightdata_gold_upsert" {
  name         = "prajnov-brightdata-gold-upsert"
  role_arn     = aws_iam_role.glue.arn
  description  = "Upsert BrightData silver into candidates DB — entity resolution by linkedin_id"
  glue_version = "4.0"

  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "s3://${var.bucket}/glue-scripts/brightdata_gold_upsert.py"
  }

  default_arguments = local.brightdata_args

  max_capacity = 0.0625
  timeout      = 60

  tags = {
    project = "prajnov-talent-lake"
    stage   = "gold"
    layer   = "upsert"
    source  = "brightdata"
  }
}
