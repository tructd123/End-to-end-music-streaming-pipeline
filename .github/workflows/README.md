# GitHub Actions CI/CD Setup for dbt

## Overview

This project uses GitHub Actions to automate dbt testing and deployment.

## Workflow Triggers

| Event | Action |
|-------|--------|
| PR to `main` | Lint → Test (dev dataset) |
| Push to `main` | Lint → Test → Deploy (prod) → Generate Docs |

## Required GitHub Secrets

Add these secrets in **Settings > Secrets and variables > Actions**:

| Secret | Description | Example |
|--------|-------------|---------|
| `GCP_PROJECT_ID` | GCP Project ID | `graphic-boulder-483814-g7` |
| `GCP_SA_KEY` | Service Account JSON (base64) | See below |
| `BQ_LOCATION` | BigQuery region | `asia-southeast1` |

### Encoding Service Account Key

```bash
# Encode your service account key to base64
cat credentials/dbt-sa-key.json | base64 -w 0 > sa-key-base64.txt

# Copy content of sa-key-base64.txt to GitHub Secret GCP_SA_KEY
```

**Windows PowerShell:**
```powershell
[Convert]::ToBase64String([IO.File]::ReadAllBytes("credentials\dbt-sa-key.json")) | Set-Clipboard
# Paste directly to GitHub Secret
```

## Workflow Jobs

### 1. 🔍 Lint & Compile
- Installs dbt and SQLFluff
- Tests BigQuery connection
- Compiles dbt models
- Runs SQL linting (optional, continues on error)

### 2. 🧪 Run Tests
- Builds models on `dev` dataset
- Runs all dbt tests
- Uploads test results as artifacts

### 3. 🚀 Deploy to Production
- Only runs on push to `main`
- Requires manual approval (production environment)
- Runs `dbt run` and `dbt test` on prod
- Uploads deployment artifacts

### 4. 📚 Generate Docs
- Generates dbt documentation
- Uploads as downloadable artifact

## Environment Protection

For production safety, set up environment protection:

1. Go to **Settings > Environments**
2. Create `production` environment
3. Enable **Required reviewers**
4. Add branch protection for `main`

## Local Testing

Before pushing, test locally:

```bash
cd dbt

# Test dev target
dbt debug --target dev
dbt compile --target dev
dbt test --target dev

# Test prod target
dbt debug --target prod
dbt compile --target prod
```

## Troubleshooting

### Common Issues

**1. Authentication Failed**
```
Check GCP_SA_KEY is properly base64 encoded
Verify service account has BigQuery permissions
```

**2. Dataset Not Found**
```
Ensure staging_dev dataset exists for dev target
Run terraform to create datasets
```

**3. Permissions Error**
```
Service account needs:
- BigQuery Data Editor
- BigQuery Job User
```

## File Structure

```
.github/
└── workflows/
    └── dbt-ci.yml      # Main CI/CD workflow

dbt/
├── profiles.yml        # Connection profiles (local/dev/prod)
├── dbt_project.yml     # Project config
└── models/
    ├── staging/
    ├── intermediate/
    └── marts/
```

## Monitoring

After workflow runs:
1. Check **Actions** tab for logs
2. Download artifacts for detailed results
3. Review BigQuery for model changes

## Cost Considerations

- Dev builds use `staging_dev` dataset (isolated)
- Prod builds use `staging` dataset  
- Both have `maximum_bytes_billed` limits
- Failed tests won't deploy to production
