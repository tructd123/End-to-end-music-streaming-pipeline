#!/bin/bash
# ==============================================================================
# SoundFlow - GCP Deployment Script
# Deploys infrastructure and applications to Google Cloud Platform
# ==============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
TERRAFORM_DIR="$PROJECT_ROOT/terraform"
SPARK_DIR="$PROJECT_ROOT/spark_streaming"
DBT_DIR="$PROJECT_ROOT/dbt"

# Load environment variables if .env exists
if [ -f "$PROJECT_ROOT/.env.gcp" ]; then
    source "$PROJECT_ROOT/.env.gcp"
fi

print_header() {
    echo -e "\n${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}\n"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

check_prerequisites() {
    print_header "Checking Prerequisites"
    
    # Check gcloud
    if ! command -v gcloud &> /dev/null; then
        print_error "gcloud CLI not found. Please install: https://cloud.google.com/sdk/docs/install"
        exit 1
    fi
    print_success "gcloud CLI found"
    
    # Check terraform
    if ! command -v terraform &> /dev/null; then
        print_error "Terraform not found. Please install: https://www.terraform.io/downloads"
        exit 1
    fi
    print_success "Terraform found: $(terraform version -json | jq -r '.terraform_version')"
    
    # Check docker
    if ! command -v docker &> /dev/null; then
        print_error "Docker not found. Please install Docker"
        exit 1
    fi
    print_success "Docker found"
    
    # Check gcloud auth
    if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | head -n1 &> /dev/null; then
        print_error "Not authenticated with gcloud. Run: gcloud auth login"
        exit 1
    fi
    print_success "gcloud authenticated as: $(gcloud auth list --filter=status:ACTIVE --format='value(account)' | head -n1)"
    
    # Check project is set
    if [ -z "$GCP_PROJECT" ]; then
        GCP_PROJECT=$(gcloud config get-value project 2>/dev/null)
    fi
    
    if [ -z "$GCP_PROJECT" ]; then
        print_error "GCP_PROJECT not set. Set it in .env.gcp or run: gcloud config set project YOUR_PROJECT"
        exit 1
    fi
    print_success "GCP Project: $GCP_PROJECT"
}

init_terraform() {
    print_header "Initializing Terraform"
    
    cd "$TERRAFORM_DIR"
    
    # Create terraform.tfvars if not exists
    if [ ! -f "terraform.tfvars" ]; then
        print_warning "terraform.tfvars not found. Creating from example..."
        cp terraform.tfvars.example terraform.tfvars
        echo ""
        print_warning "Please edit terraform/terraform.tfvars with your configuration:"
        echo "  - project_id"
        echo "  - region (default: asia-southeast1)"
        echo "  - environment (default: dev)"
        echo ""
        read -p "Press Enter after editing terraform.tfvars..."
    fi
    
    terraform init
    print_success "Terraform initialized"
}

plan_infrastructure() {
    print_header "Planning Infrastructure"
    
    cd "$TERRAFORM_DIR"
    terraform plan -out=tfplan
    
    echo ""
    read -p "Review the plan above. Continue with apply? (y/n): " -n 1 -r
    echo ""
    
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_warning "Deployment cancelled"
        exit 0
    fi
}

apply_infrastructure() {
    print_header "Applying Infrastructure"
    
    cd "$TERRAFORM_DIR"
    terraform apply tfplan
    
    print_success "Infrastructure deployed"
    
    # Save outputs for later use
    terraform output -json > "$PROJECT_ROOT/.terraform-outputs.json"
    print_success "Terraform outputs saved to .terraform-outputs.json"
}

build_docker_images() {
    print_header "Building Docker Images"
    
    # Get GCR hostname from terraform outputs
    GCR_HOST="gcr.io/$GCP_PROJECT"
    
    # Build Spark image
    echo "Building Spark streaming image..."
    cd "$SPARK_DIR"
    docker build -f Dockerfile.gcp -t "$GCR_HOST/soundflow-spark:latest" .
    print_success "Spark image built"
    
    # Push to GCR
    echo "Pushing images to GCR..."
    docker push "$GCR_HOST/soundflow-spark:latest"
    print_success "Images pushed to GCR"
}

setup_dbt() {
    print_header "Setting up dbt for BigQuery"
    
    cd "$DBT_DIR"
    
    # Create .env file for dbt
    cat > .env.gcp << EOF
DBT_TARGET=prod
GCP_PROJECT=$GCP_PROJECT
BQ_LOCATION=${BQ_LOCATION:-asia-southeast1}
GOOGLE_APPLICATION_CREDENTIALS=$PROJECT_ROOT/terraform/credentials/dbt-sa-key.json
EOF
    
    print_success "dbt environment file created: .env.gcp"
    
    # Test dbt connection
    echo "Testing dbt connection..."
    source .env.gcp
    
    if dbt debug --target prod; then
        print_success "dbt connection successful"
    else
        print_warning "dbt connection failed. Check credentials and BigQuery access."
    fi
}

run_dbt() {
    print_header "Running dbt Models"
    
    cd "$DBT_DIR"
    source .env.gcp
    
    echo "Running dbt build..."
    dbt build --target prod
    
    print_success "dbt models deployed to BigQuery"
}

print_summary() {
    print_header "Deployment Summary"
    
    cd "$TERRAFORM_DIR"
    
    echo "GCS Bucket: $(terraform output -raw gcs_bucket_url)"
    echo ""
    echo "BigQuery Datasets:"
    terraform output -json bigquery_datasets | jq -r 'to_entries[] | "  - \(.key): \(.value)"'
    echo ""
    echo "Pub/Sub Topics:"
    terraform output -json pubsub_topics | jq -r 'to_entries[] | "  - \(.key): \(.value)"'
    echo ""
    echo "Service Accounts:"
    terraform output -json service_accounts | jq -r 'to_entries[] | "  - \(.key): \(.value)"'
    echo ""
    
    print_success "Deployment complete!"
    
    echo ""
    echo "Next steps:"
    echo "  1. Configure EventSim to publish to Pub/Sub topics"
    echo "  2. Deploy Spark streaming job to Cloud Run or GKE"
    echo "  3. Run dbt to create mart tables: cd dbt && dbt run --target prod"
    echo "  4. Set up Dagster for orchestration"
}

# Main deployment flow
main() {
    print_header "SoundFlow GCP Deployment"
    
    check_prerequisites
    
    # Parse arguments
    SKIP_TERRAFORM=false
    SKIP_DOCKER=false
    SKIP_DBT=false
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            --skip-terraform)
                SKIP_TERRAFORM=true
                shift
                ;;
            --skip-docker)
                SKIP_DOCKER=true
                shift
                ;;
            --skip-dbt)
                SKIP_DBT=true
                shift
                ;;
            --terraform-only)
                SKIP_DOCKER=true
                SKIP_DBT=true
                shift
                ;;
            --help)
                echo "Usage: $0 [options]"
                echo ""
                echo "Options:"
                echo "  --skip-terraform    Skip Terraform deployment"
                echo "  --skip-docker       Skip Docker image build and push"
                echo "  --skip-dbt          Skip dbt setup and run"
                echo "  --terraform-only    Only run Terraform deployment"
                echo "  --help              Show this help message"
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                exit 1
                ;;
        esac
    done
    
    # Run deployment steps
    if [ "$SKIP_TERRAFORM" = false ]; then
        init_terraform
        plan_infrastructure
        apply_infrastructure
    fi
    
    if [ "$SKIP_DOCKER" = false ]; then
        build_docker_images
    fi
    
    if [ "$SKIP_DBT" = false ]; then
        setup_dbt
        
        read -p "Run dbt models now? (y/n): " -n 1 -r
        echo ""
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            run_dbt
        fi
    fi
    
    print_summary
}

# Run main function
main "$@"
