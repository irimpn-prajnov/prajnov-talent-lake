# ── IAM role for Step Functions ──────────────────────────────────────────────

resource "aws_iam_role" "sfn" {
  name = "prajnov-sfn-talent-lake"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "sfn_glue" {
  name = "prajnov-sfn-glue"
  role = aws_iam_role.sfn.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "glue:StartJobRun",
        "glue:GetJobRun",
        "glue:GetJobRuns",
        "glue:BatchStopJobRun"
      ]
      Resource = "*"
    }]
  })
}

# ── Step Functions — 4-step pipeline ─────────────────────────────────────────
#
#  Step 1: Bronze  — Scrape all 19 certs from Credly (one file per cert)
#  Step 2: Silver  — Match + enrich BrightData candidates with Credly certs
#  Step 3: DQ Fix  — Normalize dirty certs, dedup, fix locations
#  Step 4: Gold    — Entity resolution + merge into gold candidates table

resource "aws_sfn_state_machine" "pipeline" {
  name     = "prajnov-talent-lake-pipeline"
  role_arn = aws_iam_role.sfn.arn

  definition = jsonencode({
    Comment = "Prajnov Talent Lake — weekly pipeline: Bronze → Silver → DQ → Gold"
    StartAt = "BronzeIngest"

    States = {

      # Step 1: Scrape all 19 certs from Credly → one consolidated file per cert
      BronzeIngest = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.credly_bronze_ingest.name
          Arguments = {
            "--CERTS" = join(",", var.certs)
          }
        }
        Retry = [{
          ErrorEquals     = ["States.TaskFailed"]
          IntervalSeconds = 60
          MaxAttempts     = 2
          BackoffRate     = 2
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "PipelineFailed"
          ResultPath  = "$.error"
        }]
        Next = "SilverEnrich"
      }

      # Step 2: Match Credly records against BrightData, enrich verified_certifications
      SilverEnrich = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.credly_silver_enrich.name
        }
        Retry = [{
          ErrorEquals     = ["States.TaskFailed"]
          IntervalSeconds = 30
          MaxAttempts     = 2
          BackoffRate     = 2
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "PipelineFailed"
          ResultPath  = "$.error"
        }]
        Next = "DQFix"
      }

      # Step 3: Normalize dirty certs, dedup, fix locations
      DQFix = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.shared_dq_fix.name
        }
        Retry = [{
          ErrorEquals     = ["States.TaskFailed"]
          IntervalSeconds = 30
          MaxAttempts     = 2
          BackoffRate     = 2
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "PipelineFailed"
          ResultPath  = "$.error"
        }]
        Next = "GoldMerge"
      }

      # Step 4: Entity resolution + merge into gold candidates table
      GoldMerge = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.shared_gold_merge.name
        }
        Retry = [{
          ErrorEquals     = ["States.TaskFailed"]
          IntervalSeconds = 30
          MaxAttempts     = 2
          BackoffRate     = 2
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "PipelineFailed"
          ResultPath  = "$.error"
        }]
        Next = "EmbedUpsert"
      }

      # Step 5: Bedrock Titan v2 embeddings → embedding_titan column
      EmbedUpsert = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.shared_embed_upsert.name
        }
        Retry = [{
          ErrorEquals     = ["States.TaskFailed"]
          IntervalSeconds = 30
          MaxAttempts     = 2
          BackoffRate     = 2
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "PipelineFailed"
          ResultPath  = "$.error"
        }]
        End = true
      }

      PipelineFailed = {
        Type  = "Fail"
        Error = "PipelineFailed"
        Cause = "One or more Glue jobs failed. Check CloudWatch logs."
      }
    }
  })

  tags = {
    project = "prajnov-talent-lake"
  }
}

# ── IAM role for EventBridge Scheduler ───────────────────────────────────────

resource "aws_iam_role" "scheduler" {
  name = "prajnov-scheduler-talent-lake"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "scheduler_sfn" {
  name = "prajnov-scheduler-sfn"
  role = aws_iam_role.scheduler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["states:StartExecution"]
      Resource = [aws_sfn_state_machine.pipeline.arn]
    }]
  })
}

# ── EventBridge Scheduler — every Sunday 01:00 UTC ───────────────────────────

resource "aws_scheduler_schedule" "weekly" {
  name       = "prajnov-talent-lake-weekly"
  group_name = "default"

  flexible_time_window { mode = "OFF" }

  schedule_expression          = "cron(0 1 ? * SUN *)"
  schedule_expression_timezone = "UTC"

  target {
    arn      = aws_sfn_state_machine.pipeline.arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({ trigger = "weekly_schedule" })
  }
}

# ── IAM: allow Glue to read from BrightData source bucket ───────────────────

resource "aws_iam_role_policy" "glue_brightdata_src" {
  name = "prajnov-glue-brightdata-src"
  role = aws_iam_role.glue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:ListBucket"]
      Resource = [
        "arn:aws:s3:::${var.brightdata_bucket}",
        "arn:aws:s3:::${var.brightdata_bucket}/*"
      ]
    }]
  })
}

# ── Step Functions — BrightData 3-step pipeline ───────────────────────────────
#
#  Step 1: Bronze  — Scan BrightData datasets bucket → bronze JSONL batches
#  Step 2: Silver  — Normalize bronze → 5 silver Parquet tables
#  Step 3: Gold    — Upsert silver into candidates DB (entity resolution)

resource "aws_sfn_state_machine" "brightdata_pipeline" {
  name     = "prajnov-brightdata-pipeline"
  role_arn = aws_iam_role.sfn.arn

  definition = jsonencode({
    Comment = "Prajnov BrightData pipeline: Bronze → Silver → Gold"
    StartAt = "BronzeIngest"

    States = {

      BronzeIngest = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.brightdata_bronze_ingest.name
        }
        Retry = [{
          ErrorEquals     = ["States.TaskFailed"]
          IntervalSeconds = 60
          MaxAttempts     = 2
          BackoffRate     = 2
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "PipelineFailed"
          ResultPath  = "$.error"
        }]
        Next = "SilverNormalize"
      }

      SilverNormalize = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.brightdata_silver_normalize.name
        }
        Retry = [{
          ErrorEquals     = ["States.TaskFailed"]
          IntervalSeconds = 30
          MaxAttempts     = 2
          BackoffRate     = 2
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "PipelineFailed"
          ResultPath  = "$.error"
        }]
        Next = "GoldUpsert"
      }

      GoldUpsert = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.brightdata_gold_upsert.name
        }
        Retry = [{
          ErrorEquals     = ["States.TaskFailed"]
          IntervalSeconds = 30
          MaxAttempts     = 2
          BackoffRate     = 2
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "PipelineFailed"
          ResultPath  = "$.error"
        }]
        Next = "EmbedUpsert"
      }

      # Step 4: Bedrock Titan v2 embeddings → embedding_titan column
      EmbedUpsert = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.shared_embed_upsert.name
        }
        Retry = [{
          ErrorEquals     = ["States.TaskFailed"]
          IntervalSeconds = 30
          MaxAttempts     = 2
          BackoffRate     = 2
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "PipelineFailed"
          ResultPath  = "$.error"
        }]
        End = true
      }

      PipelineFailed = {
        Type  = "Fail"
        Error = "PipelineFailed"
        Cause = "One or more BrightData Glue jobs failed. Check CloudWatch logs."
      }
    }
  })

  tags = {
    project = "prajnov-talent-lake"
    source  = "brightdata"
  }
}

# ── EventBridge Scheduler — BrightData pipeline every Sunday 02:00 UTC ───────
# Runs 1 hour after the Credly pipeline (01:00 UTC) to avoid DB contention.

resource "aws_scheduler_schedule" "brightdata_weekly" {
  name       = "prajnov-brightdata-pipeline-weekly"
  group_name = "default"

  flexible_time_window { mode = "OFF" }

  schedule_expression          = "cron(0 2 ? * SUN *)"
  schedule_expression_timezone = "UTC"

  target {
    arn      = aws_sfn_state_machine.brightdata_pipeline.arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({ trigger = "weekly_schedule" })
  }
}

# ── CloudWatch alarm — BrightData pipeline failure ────────────────────────────

resource "aws_cloudwatch_metric_alarm" "brightdata_pipeline_failure" {
  alarm_name          = "prajnov-brightdata-pipeline-failure"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ExecutionsFailed"
  namespace           = "AWS/States"
  period              = 86400
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Prajnov BrightData weekly pipeline failed"

  dimensions = {
    StateMachineArn = aws_sfn_state_machine.brightdata_pipeline.arn
  }
}

resource "aws_cloudwatch_metric_alarm" "pipeline_failure" {
  alarm_name          = "prajnov-talent-lake-pipeline-failure"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ExecutionsFailed"
  namespace           = "AWS/States"
  period              = 86400
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Prajnov talent lake weekly pipeline failed"

  dimensions = {
    StateMachineArn = aws_sfn_state_machine.pipeline.arn
  }
}
