terraform {
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
}

provider "aws" {
  region = var.aws_region
}

variable "aws_region"   { default = "ap-south-1" }
variable "db_name"      { default = "prajnov" }
variable "db_username"  { default = "prajnov_admin" }
variable "db_password"  {
  description = "Aurora master password — set via TF_VAR_db_password env var"
  sensitive   = true
}

# ── Networking — use default VPC ──────────────────────────────────────────────

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

resource "aws_db_subnet_group" "prajnov" {
  name       = "prajnov-aurora"
  subnet_ids = data.aws_subnets.default.ids
}

resource "aws_security_group" "aurora" {
  name   = "prajnov-aurora-sg"
  vpc_id = data.aws_vpc.default.id

  # Allow inbound PostgreSQL from within VPC + Lambda/Glue
  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [data.aws_vpc.default.cidr_block]
  }

  # Allow inbound from anywhere for local dev (restrict in prod)
  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "local dev access — restrict to office IP in prod"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ── Aurora PostgreSQL Serverless v2 ──────────────────────────────────────────

resource "aws_rds_cluster" "prajnov" {
  cluster_identifier      = "prajnov-talent-lake"
  engine                  = "aurora-postgresql"
  engine_mode             = "provisioned"
  engine_version          = "16.4"
  database_name           = var.db_name
  master_username         = var.db_username
  master_password         = var.db_password
  db_subnet_group_name    = aws_db_subnet_group.prajnov.name
  vpc_security_group_ids  = [aws_security_group.aurora.id]

  serverlessv2_scaling_configuration {
    min_capacity = 0.5   # ~$0.06/hr when idle
    max_capacity = 4.0   # scales up for bulk writes
  }

  enable_http_endpoint         = true   # Data API — no VPC tunnel needed for scripts
  skip_final_snapshot          = true
  deletion_protection          = false
  backup_retention_period      = 7
  preferred_backup_window      = "02:00-03:00"
  preferred_maintenance_window = "sun:04:00-sun:05:00"

  tags = {
    project = "prajnov-talent-lake"
    layer   = "gold"
  }
}

resource "aws_rds_cluster_instance" "prajnov" {
  identifier         = "prajnov-talent-lake-1"
  cluster_identifier = aws_rds_cluster.prajnov.id
  instance_class     = "db.serverless"
  engine             = aws_rds_cluster.prajnov.engine
  engine_version     = aws_rds_cluster.prajnov.engine_version

  publicly_accessible = true   # needed for local pipeline scripts
}

# ── Secrets Manager — store DB credentials ────────────────────────────────────

resource "aws_secretsmanager_secret" "aurora_creds" {
  name                    = "prajnov/aurora/credentials"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "aurora_creds" {
  secret_id = aws_secretsmanager_secret.aurora_creds.id
  secret_string = jsonencode({
    host     = aws_rds_cluster.prajnov.endpoint
    port     = 5432
    dbname   = var.db_name
    username = var.db_username
    password = var.db_password
  })
}

# ── Outputs ───────────────────────────────────────────────────────────────────

output "aurora_endpoint" {
  value = aws_rds_cluster.prajnov.endpoint
}

output "aurora_reader_endpoint" {
  value = aws_rds_cluster.prajnov.reader_endpoint
}

output "aurora_cluster_arn" {
  value = aws_rds_cluster.prajnov.arn
}

output "secret_arn" {
  value = aws_secretsmanager_secret.aurora_creds.arn
}

output "connection_string" {
  value     = "postgresql://${var.db_username}:<password>@${aws_rds_cluster.prajnov.endpoint}:5432/${var.db_name}"
  sensitive = false
}
