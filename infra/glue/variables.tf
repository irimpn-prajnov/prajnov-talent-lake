variable "aws_region" {
  default = "ap-south-1"
}

variable "aws_profile" {
  default = "201727487159_AdministratorAccess"
}

variable "bucket" {
  default = "prajnov-talent-lake"
}

variable "pgvector_host" {
  default = "ai-recruiter.c7q8sg00wqte.ap-south-1.rds.amazonaws.com"
}

variable "pgvector_db" {
  default = "recruiter"
}

variable "pgvector_user" {
  default = "prajnov"
}

variable "pgvector_secret_arn" {
  description = "ARN of the prajnov/recruiter Secrets Manager secret"
  default     = "arn:aws:secretsmanager:ap-south-1:201727487159:secret:prajnov/recruiter-F8sc3X"
}

variable "signalhire_api_key" {
  sensitive = true
  default   = ""
}

variable "brightdata_bucket" {
  default = "prajnov-brightdata-datasets"
}

# All ServiceNow certs — drives both Glue jobs and Step Functions branches
variable "certs" {
  type = list(string)
  default = [
    # Core
    "CSA", "CAD", "CTA", "CMA",
    # CIS
    "CIS-CSM",  "CIS-DF",   "CIS-DISCO", "CIS-EM",  "CIS-FSM",
    "CIS-HAM",  "CIS-HR",   "CIS-ITSM",  "CIS-PA",  "CIS-RC",
    "CIS-SAM",  "CIS-SIR",  "CIS-SM",    "CIS-SP",  "CIS-SPM",
    "CIS-TPRM", "CIS-VR",   "CIS-APM",   "CIS-CPG",
    # Suite
    "SUITE-CSM",  "SUITE-DF",    "SUITE-EA",    "SUITE-HR",    "SUITE-ITSM",
    "SUITE-ITOM-V", "SUITE-ITOM-P", "SUITE-ITOM-E",
    "SUITE-LSD",  "SUITE-PI",    "SUITE-PSDS",  "SUITE-WSD",
    "SUITE-FSO-B", "SUITE-FSO-I", "SUITE-APO",   "SUITE-SPO",   "SUITE-SLO", "SUITE-TSM",
    # Platform Owner
    "CPOA", "CPOP", "CPOE",
  ]
}
