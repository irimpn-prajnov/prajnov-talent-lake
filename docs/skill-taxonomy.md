# Skill Taxonomy & Certification Catalog

## Skill Categories

```
SKILLS
├── platforms/
│   ├── ServiceNow
│   │   ├── ITSM
│   │   ├── ITOM
│   │   │   ├── Discovery
│   │   │   ├── Service Mapping
│   │   │   └── ACC (Agent Client Collector)
│   │   ├── CSM (Customer Service Management)
│   │   ├── FSM (Field Service Management)
│   │   ├── SPM (Strategic Portfolio Management)
│   │   ├── SecOps
│   │   ├── GRC
│   │   ├── HRSD
│   │   └── App Engine / Flow Designer
│   ├── Salesforce
│   ├── SAP
│   └── Workday
│
├── cloud/
│   ├── AWS
│   │   ├── EC2, S3, RDS, Lambda
│   │   ├── Bedrock
│   │   ├── SageMaker
│   │   ├── Glue, Athena, Lake Formation
│   │   └── EKS, ECS, Fargate
│   ├── Azure
│   │   ├── Azure OpenAI
│   │   └── Azure Data Factory
│   └── GCP
│       ├── BigQuery
│       └── Vertex AI
│
├── ai_ml/
│   ├── Generative AI
│   │   ├── RAG (Retrieval-Augmented Generation)
│   │   ├── LLM Fine-tuning
│   │   ├── Prompt Engineering
│   │   ├── Agentic AI
│   │   ├── LangChain
│   │   ├── LlamaIndex
│   │   └── Vector Databases
│   ├── Machine Learning
│   │   ├── scikit-learn
│   │   ├── XGBoost
│   │   └── PyTorch / TensorFlow
│   └── Data Science
│       ├── Pandas, NumPy
│       └── Spark / PySpark
│
├── languages/
│   ├── Python
│   ├── JavaScript / TypeScript
│   ├── Java
│   ├── Go
│   └── SQL
│
├── data/
│   ├── Data Engineering
│   │   ├── Apache Spark
│   │   ├── Apache Kafka
│   │   ├── dbt
│   │   └── Airflow
│   ├── Databases
│   │   ├── PostgreSQL
│   │   ├── MySQL
│   │   ├── MongoDB
│   │   ├── Snowflake
│   │   └── Databricks
│   └── BI / Analytics
│       ├── Tableau
│       ├── Power BI
│       └── Looker
│
├── devops/
│   ├── Kubernetes (K8s)
│   ├── Docker
│   ├── Terraform
│   ├── CI/CD (GitHub Actions, Jenkins)
│   └── Ansible
│
└── frameworks/
    ├── React / Next.js
    ├── FastAPI
    ├── Spring Boot
    └── Django / Flask
```

---

## Skill Aliases (Normalization Map)

| Raw Name | Canonical Name |
|---|---|
| SNOW | ServiceNow |
| Gen AI, GenAI | Generative AI |
| Amazon Bedrock, AWS Bedrock | Bedrock |
| Postgres, PostgreSQL | PostgreSQL |
| K8s | Kubernetes |
| JS | JavaScript |
| TS | TypeScript |
| ML | Machine Learning |
| NLP | Natural Language Processing |
| LLM | Large Language Models |
| RAG | Retrieval-Augmented Generation |
| SN CSM | ServiceNow CSM |
| ITSM | IT Service Management |
| IaC | Infrastructure as Code |
| CI/CD | Continuous Integration/Continuous Deployment |

---

## Certification Catalog

### ServiceNow
| Code | Name | Level | Skills |
|---|---|---|---|
| CIS-ITSM | Certified Implementation Specialist – ITSM | Specialist | ServiceNow, ITSM |
| CIS-CSM | Certified Implementation Specialist – CSM | Specialist | ServiceNow, CSM |
| CIS-FSM | Certified Implementation Specialist – FSM | Specialist | ServiceNow, FSM |
| CIS-ITOM | Certified Implementation Specialist – ITOM | Specialist | ServiceNow, ITOM |
| CIS-SecOps | Certified Implementation Specialist – SecOps | Specialist | ServiceNow, SecOps |
| CIS-SPM | Certified Implementation Specialist – SPM | Specialist | ServiceNow, SPM |
| CSA | Certified System Administrator | Associate | ServiceNow |
| CAD | Certified Application Developer | Professional | ServiceNow, App Engine |

### AWS
| Code | Name | Level | Skills |
|---|---|---|---|
| AWS-CCP | Cloud Practitioner | Foundational | AWS |
| AWS-SAA | Solutions Architect – Associate | Associate | AWS, Architecture |
| AWS-SAP | Solutions Architect – Professional | Professional | AWS, Architecture |
| AWS-DEA | Data Engineer – Associate | Associate | AWS, Data Engineering |
| AWS-MLA | Machine Learning Engineer – Associate | Associate | AWS, ML |
| AWS-ANS | Advanced Networking – Specialty | Specialty | AWS, Networking |
| AWS-SCS | Security – Specialty | Specialty | AWS, Security |

### Microsoft Azure
| Code | Name | Level | Skills |
|---|---|---|---|
| AZ-900 | Azure Fundamentals | Foundational | Azure |
| AZ-104 | Azure Administrator | Associate | Azure |
| AZ-204 | Azure Developer | Associate | Azure, Development |
| AZ-305 | Azure Solutions Architect Expert | Expert | Azure, Architecture |
| DP-203 | Azure Data Engineer | Associate | Azure, Data Engineering |
| AI-102 | Azure AI Engineer | Associate | Azure, AI |

### Google Cloud
| Code | Name | Level | Skills |
|---|---|---|---|
| GCP-ACE | Associate Cloud Engineer | Associate | GCP |
| GCP-PCA | Professional Cloud Architect | Professional | GCP, Architecture |
| GCP-PDE | Professional Data Engineer | Professional | GCP, Data Engineering |
| GCP-PMLE | Professional ML Engineer | Professional | GCP, ML |

### Databricks
| Code | Name | Level | Skills |
|---|---|---|---|
| DB-DECA | Databricks Certified Associate Developer | Associate | Databricks, Spark |
| DB-DECP | Databricks Certified Professional Developer | Professional | Databricks, Spark |
| DB-MLPA | Databricks ML Practitioner | Associate | Databricks, ML |

### Kubernetes / CNCF
| Code | Name | Level | Skills |
|---|---|---|---|
| CKA | Certified Kubernetes Administrator | Professional | Kubernetes |
| CKAD | Certified Kubernetes Application Developer | Professional | Kubernetes |
| CKS | Certified Kubernetes Security Specialist | Specialist | Kubernetes, Security |

### Agile / SAFe
| Code | Name | Level | Skills |
|---|---|---|---|
| CSM | Certified Scrum Master | Practitioner | Scrum, Agile |
| SAFe-SA | SAFe Agilist | Practitioner | SAFe, Agile |
| SAFe-SPC | SAFe Program Consultant | Advanced | SAFe, Agile |

---

## Certification → Skill Inference

When a candidate holds a certification, we can infer skills with high confidence:

```
CIS-CSM  →  ServiceNow (expert), CSM (expert), ITSM (intermediate)
AWS-SAP  →  AWS (expert), Cloud Architecture (expert), EC2/S3/RDS (advanced)
CKA      →  Kubernetes (expert), Docker (advanced), Linux (intermediate)
DB-DECP  →  Databricks (expert), Spark (expert), Python (advanced), SQL (advanced)
```

These inferences are stored as `candidate_skill.is_inferred = true` with
`evidence = {"certification": "CIS-CSM", "confidence": 0.95}`.
