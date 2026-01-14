# ==============================================================================
# SoundFlow - GCP Deployment Script (PowerShell)
# Deploys infrastructure and applications to Google Cloud Platform
# ==============================================================================

param(
    [switch]$SkipTerraform,
    [switch]$SkipDocker,
    [switch]$SkipDbt,
    [switch]$TerraformOnly,
    [switch]$Help
)

$ErrorActionPreference = "Stop"

# Configuration
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
$TerraformDir = Join-Path $ProjectRoot "terraform"
$SparkDir = Join-Path $ProjectRoot "spark_streaming"
$DbtDir = Join-Path $ProjectRoot "dbt"

# Load environment variables if .env.gcp exists
$EnvFile = Join-Path $ProjectRoot ".env.gcp"
if (Test-Path $EnvFile) {
    Get-Content $EnvFile | ForEach-Object {
        if ($_ -match '^([^=]+)=(.*)$') {
            [Environment]::SetEnvironmentVariable($matches[1], $matches[2], "Process")
        }
    }
}

function Write-Header {
    param([string]$Message)
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Blue
    Write-Host $Message -ForegroundColor Blue
    Write-Host "========================================" -ForegroundColor Blue
    Write-Host ""
}

function Write-Success {
    param([string]$Message)
    Write-Host "✓ $Message" -ForegroundColor Green
}

function Write-Warning {
    param([string]$Message)
    Write-Host "⚠ $Message" -ForegroundColor Yellow
}

function Write-Error {
    param([string]$Message)
    Write-Host "✗ $Message" -ForegroundColor Red
}

function Test-Prerequisites {
    Write-Header "Checking Prerequisites"
    
    # Check gcloud
    if (-not (Get-Command gcloud -ErrorAction SilentlyContinue)) {
        Write-Error "gcloud CLI not found. Please install: https://cloud.google.com/sdk/docs/install"
        exit 1
    }
    Write-Success "gcloud CLI found"
    
    # Check terraform
    if (-not (Get-Command terraform -ErrorAction SilentlyContinue)) {
        Write-Error "Terraform not found. Please install: https://www.terraform.io/downloads"
        exit 1
    }
    $tfVersion = terraform version -json | ConvertFrom-Json
    Write-Success "Terraform found: $($tfVersion.terraform_version)"
    
    # Check docker
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        Write-Error "Docker not found. Please install Docker"
        exit 1
    }
    Write-Success "Docker found"
    
    # Check gcloud auth
    $authList = gcloud auth list --filter="status:ACTIVE" --format="value(account)" 2>$null
    if (-not $authList) {
        Write-Error "Not authenticated with gcloud. Run: gcloud auth login"
        exit 1
    }
    Write-Success "gcloud authenticated as: $($authList | Select-Object -First 1)"
    
    # Check project is set
    $script:GcpProject = $env:GCP_PROJECT
    if (-not $script:GcpProject) {
        $script:GcpProject = gcloud config get-value project 2>$null
    }
    
    if (-not $script:GcpProject) {
        Write-Error "GCP_PROJECT not set. Set it in .env.gcp or run: gcloud config set project YOUR_PROJECT"
        exit 1
    }
    Write-Success "GCP Project: $script:GcpProject"
}

function Initialize-Terraform {
    Write-Header "Initializing Terraform"
    
    Set-Location $TerraformDir
    
    # Create terraform.tfvars if not exists
    if (-not (Test-Path "terraform.tfvars")) {
        Write-Warning "terraform.tfvars not found. Creating from example..."
        Copy-Item "terraform.tfvars.example" "terraform.tfvars"
        Write-Host ""
        Write-Warning "Please edit terraform/terraform.tfvars with your configuration:"
        Write-Host "  - project_id"
        Write-Host "  - region (default: asia-southeast1)"
        Write-Host "  - environment (default: dev)"
        Write-Host ""
        Read-Host "Press Enter after editing terraform.tfvars"
    }
    
    terraform init
    Write-Success "Terraform initialized"
}

function Show-TerraformPlan {
    Write-Header "Planning Infrastructure"
    
    Set-Location $TerraformDir
    terraform plan -out=tfplan
    
    Write-Host ""
    $response = Read-Host "Review the plan above. Continue with apply? (y/n)"
    
    if ($response -ne 'y' -and $response -ne 'Y') {
        Write-Warning "Deployment cancelled"
        exit 0
    }
}

function Invoke-TerraformApply {
    Write-Header "Applying Infrastructure"
    
    Set-Location $TerraformDir
    terraform apply tfplan
    
    Write-Success "Infrastructure deployed"
    
    # Save outputs for later use
    terraform output -json | Out-File -FilePath (Join-Path $ProjectRoot ".terraform-outputs.json") -Encoding UTF8
    Write-Success "Terraform outputs saved to .terraform-outputs.json"
}

function Build-DockerImages {
    Write-Header "Building Docker Images"
    
    $GcrHost = "gcr.io/$script:GcpProject"
    
    # Build Spark image
    Write-Host "Building Spark streaming image..."
    Set-Location $SparkDir
    docker build -f Dockerfile.gcp -t "$GcrHost/soundflow-spark:latest" .
    Write-Success "Spark image built"
    
    # Push to GCR
    Write-Host "Pushing images to GCR..."
    docker push "$GcrHost/soundflow-spark:latest"
    Write-Success "Images pushed to GCR"
}

function Initialize-Dbt {
    Write-Header "Setting up dbt for BigQuery"
    
    Set-Location $DbtDir
    
    $BqLocation = if ($env:BQ_LOCATION) { $env:BQ_LOCATION } else { "asia-southeast1" }
    $CredentialsPath = Join-Path $TerraformDir "credentials\dbt-sa-key.json"
    
    # Create .env file for dbt
    @"
DBT_TARGET=prod
GCP_PROJECT=$script:GcpProject
BQ_LOCATION=$BqLocation
GOOGLE_APPLICATION_CREDENTIALS=$CredentialsPath
"@ | Out-File -FilePath ".env.gcp" -Encoding UTF8
    
    Write-Success "dbt environment file created: .env.gcp"
    
    # Set environment variables
    $env:DBT_TARGET = "prod"
    $env:GCP_PROJECT = $script:GcpProject
    $env:BQ_LOCATION = $BqLocation
    $env:GOOGLE_APPLICATION_CREDENTIALS = $CredentialsPath
    
    # Test dbt connection
    Write-Host "Testing dbt connection..."
    try {
        dbt debug --target prod
        Write-Success "dbt connection successful"
    } catch {
        Write-Warning "dbt connection failed. Check credentials and BigQuery access."
    }
}

function Invoke-DbtBuild {
    Write-Header "Running dbt Models"
    
    Set-Location $DbtDir
    
    Write-Host "Running dbt build..."
    dbt build --target prod
    
    Write-Success "dbt models deployed to BigQuery"
}

function Show-Summary {
    Write-Header "Deployment Summary"
    
    Set-Location $TerraformDir
    
    Write-Host "GCS Bucket: $(terraform output -raw gcs_bucket_url)"
    Write-Host ""
    Write-Host "BigQuery Datasets:"
    $datasets = terraform output -json bigquery_datasets | ConvertFrom-Json
    $datasets.PSObject.Properties | ForEach-Object { Write-Host "  - $($_.Name): $($_.Value)" }
    Write-Host ""
    Write-Host "Pub/Sub Topics:"
    $topics = terraform output -json pubsub_topics | ConvertFrom-Json
    $topics.PSObject.Properties | ForEach-Object { Write-Host "  - $($_.Name): $($_.Value)" }
    Write-Host ""
    Write-Host "Service Accounts:"
    $sas = terraform output -json service_accounts | ConvertFrom-Json
    $sas.PSObject.Properties | ForEach-Object { Write-Host "  - $($_.Name): $($_.Value)" }
    Write-Host ""
    
    Write-Success "Deployment complete!"
    
    Write-Host ""
    Write-Host "Next steps:"
    Write-Host "  1. Configure EventSim to publish to Pub/Sub topics"
    Write-Host "  2. Deploy Spark streaming job to Cloud Run or GKE"
    Write-Host "  3. Run dbt to create mart tables: cd dbt; dbt run --target prod"
    Write-Host "  4. Set up Dagster for orchestration"
}

# Show help
if ($Help) {
    Write-Host "Usage: .\deploy_gcp.ps1 [options]"
    Write-Host ""
    Write-Host "Options:"
    Write-Host "  -SkipTerraform    Skip Terraform deployment"
    Write-Host "  -SkipDocker       Skip Docker image build and push"
    Write-Host "  -SkipDbt          Skip dbt setup and run"
    Write-Host "  -TerraformOnly    Only run Terraform deployment"
    Write-Host "  -Help             Show this help message"
    exit 0
}

# Handle TerraformOnly flag
if ($TerraformOnly) {
    $SkipDocker = $true
    $SkipDbt = $true
}

# Main deployment flow
Write-Header "SoundFlow GCP Deployment"

Test-Prerequisites

if (-not $SkipTerraform) {
    Initialize-Terraform
    Show-TerraformPlan
    Invoke-TerraformApply
}

if (-not $SkipDocker) {
    Build-DockerImages
}

if (-not $SkipDbt) {
    Initialize-Dbt
    
    $response = Read-Host "Run dbt models now? (y/n)"
    if ($response -eq 'y' -or $response -eq 'Y') {
        Invoke-DbtBuild
    }
}

Show-Summary
