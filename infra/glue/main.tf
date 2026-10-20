terraform {
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
}

provider "aws" {
  region = var.aws_region
  # credentials from env vars: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN
  # set via: eval $(aws configure export-credentials --profile 201727487159_AdministratorAccess --format env)
}

# ── IAM role shared by all Glue jobs ─────────────────────────────────────────

resource "aws_iam_role" "glue" {
  name = "prajnov-glue-talent-lake"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3_rds" {
  name = "prajnov-glue-s3-rds"
  role = aws_iam_role.glue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [
          "arn:aws:s3:::${var.bucket}",
          "arn:aws:s3:::${var.bucket}/*"
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = ["arn:aws:secretsmanager:${var.aws_region}:*:secret:prajnov/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricData", "logs:CreateLogGroup",
                    "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["bedrock:InvokeModel"]
        Resource = "arn:aws:bedrock:${var.aws_region}::foundation-model/amazon.titan-embed-text-v2:0"
      }
    ]
  })
}

# ── Upload all Glue job scripts to S3 ────────────────────────────────────────

locals {
  scripts = {
    "credly_bronze_ingest"       = "jobs/credly_bronze_ingest.py"
    "credly_silver_enrich"       = "jobs/credly_silver_enrich.py"
    "shared_dq_fix"              = "jobs/shared_dq_fix.py"
    "shared_gold_merge"          = "jobs/shared_gold_merge.py"
    "brightdata_bronze_ingest"   = "jobs/brightdata_bronze_ingest.py"
    "brightdata_silver_normalize" = "jobs/brightdata_silver_normalize.py"
    "brightdata_gold_upsert"     = "jobs/brightdata_gold_upsert.py"
    "shared_embed_upsert"         = "jobs/shared_embed_upsert.py"
  }
  common_py = "../../ingestion/common.py"
}

resource "aws_s3_object" "glue_scripts" {
  for_each = local.scripts
  bucket   = var.bucket
  key      = "glue-scripts/${each.key}.py"
  source   = "${path.module}/${each.value}"
  etag     = filemd5("${path.module}/${each.value}")
}

resource "aws_s3_object" "common_py" {
  bucket = var.bucket
  key    = "glue-scripts/common.py"
  source = "${path.module}/${local.common_py}"
  etag   = filemd5("${path.module}/${local.common_py}")
}

# ── Glue catalog databases ────────────────────────────────────────────────────

resource "aws_glue_catalog_database" "bronze" {
  name        = "prajnov_bronze"
  description = "Prajnov Talent Lake — Bronze layer (raw Credly + BrightData)"
}

resource "aws_glue_catalog_database" "silver" {
  name        = "prajnov_silver"
  description = "Prajnov Talent Lake — Silver layer (normalized candidates)"
}

resource "aws_glue_catalog_database" "gold" {
  name        = "prajnov_gold"
  description = "Prajnov Talent Lake — Gold layer (Candidate 360)"
}

# ── Athena workgroup ──────────────────────────────────────────────────────────

resource "aws_athena_workgroup" "prajnov" {
  name = "prajnov"

  configuration {
    result_configuration {
      output_location = "s3://${var.bucket}/athena-results/"
    }
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = false
  }
}
