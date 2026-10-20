# Data Sources — APIs, Access Models, Rate Limits

## Source Tier Summary

| Source | Tier | Access Model | Cost | Priority |
|---|---|---|---|---|
| Own resumes / recruiter uploads | 1 | Direct | Free | R1 |
| GitHub REST API | 2 | OAuth / token | Free (5K req/hr auth) | R4 |
| Indeed Partner API | 2 | Partner agreement | TBD | R5 |
| SignalHire API | 2 | Paid per reveal | Per plan | R4 |
| Credly public badges | 2 | Public API | Free | R4 |
| AWS cert verification | 2 | Public lookup | Free | R4 |
| Google Custom Search | 3 | API key | 100/day free | R4 |

---

## 1. Own Data (Tier 1 — R1)

**What**: Every resume, application, referral, previous submission that Prajnov already has.

**Ingestion**:
- Email attachment listener (Gmail IMAP → S3 Bronze)
- Recruiter upload UI → S3 Bronze
- Bulk import from existing S3 buckets (`prajnov-email-reader-data`, `prajnov-ai-recuiter-candidate-profiles`)

**Bronze path**: `bronze/candidates/resumes/YYYY/MM/DD/{uuid}.{pdf|docx|json}`

**Notes**: This is the most valuable and safest data source. Start here.

---

## 2. GitHub REST API (Tier 2 — R4)

**Docs**: https://docs.github.com/en/rest

**What we can get**:
- Public user profile: name, bio, location, company, blog/website
- Repositories: languages, topics, description, stars, forks
- Contribution activity: commit frequency, languages used
- Pinned repos, README content

**Access**:
- Unauthenticated: 60 req/hr
- Authenticated (PAT): 5,000 req/hr
- GitHub App: 15,000 req/hr (recommended for production)

**Cost**: Free

**Key endpoints**:
```
GET /users/{username}                    # Profile
GET /users/{username}/repos              # Repositories
GET /repos/{owner}/{repo}/languages      # Language breakdown
GET /users/{username}/events/public      # Activity
```

**What to derive**:
```
github_profile
  ├── username
  ├── name, bio, location, company
  ├── public_repos_count
  ├── followers
  └── repos[]
        ├── name, description
        ├── languages{}          # {"Python": 12400, "TypeScript": 3200}
        ├── topics[]             # ["langchain", "aws", "bedrock"]
        ├── stars, forks
        └── last_pushed
```

**Skill inference**: Language + topic → skill evidence
```
Python + langchain + bedrock → GenAI/Python/Bedrock evidence
TypeScript + nextjs → React/Next.js evidence
terraform + aws → AWS/IaC evidence
```

**Bronze path**: `bronze/github/users/{username}/profile.json`
                 `bronze/github/users/{username}/repos.json`

---

## 3. Indeed Partner API (Tier 2 — R5)

**Docs**: https://developer.indeed.com/

**What's available**:
- Job Posting API: post/manage jobs
- Candidate Retrieval API: retrieve applicants + resumes (requires employer agreement)
- Application Sync: sync applications into your ATS

**Access model**:
- Requires Indeed developer account + app registration
- Candidate data requires employer/partner agreement
- Not a general-purpose resume search API

**Action**: Register at https://developer.indeed.com and apply for Candidate API access.

**Alternative — Indeed Resume Search**:
- Indeed Resume (employer product) has a web UI for resume search
- No public API for resume search — requires employer subscription
- Scraping is against ToS

**Recommendation**: Apply for Indeed Partner API. In the meantime, use resume uploads + SignalHire as primary candidate sources.

---

## 4. SignalHire API (Tier 2 — R4)

**Docs**: https://www.signalhire.com/api

**What we can get**:
- Candidate search by title, skills, location, experience
- Contact reveal: email, phone
- LinkedIn profile data (where available)
- Company data

**Access**: Paid per reveal / per search. Already integrated in prajnov-recruiter.

**Bronze path**: `bronze/enrichment/signalhire/{candidate_id}/reveal.json`

**Notes**: Use for enrichment of candidates already in Silver, not as primary discovery source.

---

## 5. Credly / Digital Credentials (Tier 2 — R4)

**Docs**: https://www.credly.com/docs/api

**What we can get**:
- Public badge lookup by badge URL or credential ID
- Badge metadata: certification name, issuer, issued date, expiry, skills
- Earner public profile (if public)

**Access**:
- Public badge data: no auth required for public badges
- Credly API (v1): requires API key for bulk/programmatic access
- Contact Credly for partner/integration access

**Key endpoints**:
```
GET /v1/badges/{badge_id}               # Badge details
GET /v1/organizations/{org_id}/badges   # All badges from an org (e.g. AWS)
```

**What to extract**:
```
certification_evidence
  ├── credential_id
  ├── certification_name
  ├── issuer (AWS, Microsoft, ServiceNow...)
  ├── issued_date
  ├── expiration_date
  ├── badge_url
  ├── skills[]
  └── verification_url
```

**Bronze path**: `bronze/certifications/credly/{credential_id}.json`

---

## 6. AWS Certification Verification (Tier 2 — R4)

**What**: AWS provides a public verification URL for each certification.

**Verification URL format**:
```
https://aws.amazon.com/verification
```
Candidates provide their certification ID → verifiable via the AWS portal.

**Approach**: Store candidate-provided cert IDs, verify via AWS portal, store result.

**Other cert providers to add later**:
| Provider | Verification |
|---|---|
| Microsoft (Azure/M365) | https://learn.microsoft.com/en-us/credentials/certifications/verify |
| Google Cloud | https://www.credential.net |
| Databricks | Credly badges |
| Snowflake | Credly badges |
| ServiceNow | https://nowlearning.servicenow.com |
| Kubernetes/CNCF | https://www.cncf.io/certification/verify |
| Salesforce | https://trailhead.salesforce.com/credentials/verification |

---

## 7. Google Custom Search — LinkedIn URL Discovery (Tier 3 — R4)

**What**: Use Google Custom Search to find public LinkedIn profile URLs for known candidates.

**Not**: Scraping LinkedIn profiles. Only discovering the URL.

**Approach**:
```
Query: site:linkedin.com/in "{name}" "{title}" "{location}"
Result: linkedin.com/in/john-smith-aws-architect-123abc
```

Store the URL as `candidate_external_identity.profile_url` — not the profile content.

**Access**: Google Custom Search API
- 100 queries/day free
- $5 per 1,000 queries beyond free tier

**Important**: Do not store LinkedIn profile content. Store only the discovered URL as a pointer. Respect LinkedIn's terms of service and robots.txt.

**Bronze path**: `bronze/discovery/web_search/{candidate_id}/linkedin_discovery.json`

---

## Source Priority Matrix

| Use Case | Best Source |
|---|---|
| Initial candidate pool | Own resumes + recruiter uploads |
| Technical skill evidence | GitHub |
| Contact enrichment | SignalHire |
| Certification verification | Credly + provider portals |
| New applicants | Indeed Partner API (R5) |
| LinkedIn URL discovery | Google Custom Search (pointer only) |
| Bulk candidate search | SignalHire |

---

## What NOT to Build (and Why)

| Approach | Why Not |
|---|---|
| LinkedIn profile scraping | Against ToS, legal risk, IP blocks |
| LinkedIn API (general) | Restricted to approved Talent Solutions partners |
| Indeed resume scraping | Against ToS |
| Automated Credly scraping | Use official API instead |
| Storing scraped PII without consent | Privacy/legal risk |
