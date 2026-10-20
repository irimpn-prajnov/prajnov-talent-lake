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

variable "pgvector_password" {
  sensitive = true
}

variable "signalhire_api_key" {
  sensitive = true
  default   = ""
}

variable "brightdata_bucket" {
  default = "prajnov-brightdata-datasets"
}

# All 19 ServiceNow certs — drives both Glue jobs and Step Functions branches
variable "certs" {
  type = list(string)
  default = [
    "CIS-VR", "CIS-CSM", "CIS-ITSM", "CIS-HR", "CIS-DISCO",
    "CIS-HAM", "CIS-SAM", "CIS-EM",  "CIS-SPM", "CIS-RC",
    "CIS-SM", "CIS-SIR", "CIS-FSM",  "CIS-PA",  "CIS-TPRM",
    "CIS-SP", "CSA",     "CAD",       "CMA"
  ]
}
