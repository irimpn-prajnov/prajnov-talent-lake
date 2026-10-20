output "glue_jobs" {
  value = {
    bronze_ingest  = aws_glue_job.credly_bronze_ingest.name
    silver_enrich  = aws_glue_job.credly_silver_enrich.name
    dq_fix         = aws_glue_job.shared_dq_fix.name
    gold_merge     = aws_glue_job.shared_gold_merge.name
  }
}

output "pipeline_arn" {
  value = aws_sfn_state_machine.pipeline.arn
}

output "weekly_schedule" {
  value = "Every Sunday 01:00 UTC — cron(0 1 ? * SUN *)"
}

output "glue_role_arn" {
  value = aws_iam_role.glue.arn
}

output "pipeline_stages" {
  value = "Bronze (Credly scrape) → Silver (enrich candidates) → DQ Fix → Gold (merge)"
}
