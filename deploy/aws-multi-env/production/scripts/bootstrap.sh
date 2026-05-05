#!/bin/bash
set -euo pipefail

# Bootstrap SchemaBot AWS infrastructure
# Usage: ./bootstrap.sh [-y]
#
# Options:
#   -y    Auto-approve all terraform applies (no interactive prompts)
#
# Idempotent — safe to re-run. Skips steps that are already done.
#
# Phases:
# 1. ECR repository
# 2. Docker image (skips if current commit already pushed)
# 3. Infrastructure (RDS, Bastion, Secrets Manager, VPC, etc.)
# 4. App Runner service (deploys the image)

REGION="us-west-2"
export AWS_PROFILE="${AWS_PROFILE:-schemabot-deployer}"

AUTO_APPROVE=""
while getopts "y" opt; do
    case $opt in
        y) AUTO_APPROVE="-auto-approve" ;;
        *) echo "Usage: $0 [-y]"; exit 1 ;;
    esac
done

echo "🚀 SchemaBot AWS Bootstrap"
echo "============================="
echo ""

# Compute absolute paths once — avoids fragile relative cd chains
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TF_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$TF_DIR/../../.." && pwd)"
cd "$TF_DIR"

# Check prerequisites
echo "🔍 Checking prerequisites..."

for cmd in terraform docker aws jq mysql nc session-manager-plugin; do
    if ! command -v "$cmd" &> /dev/null; then
        echo "❌ $cmd not found"
        exit 1
    fi
done

if ! aws sts get-caller-identity &> /dev/null; then
    echo "❌ AWS credentials not configured for profile: $AWS_PROFILE"
    exit 1
fi

echo "   ✅ All prerequisites met"
echo ""

# Resolve AWS account ID for naming S3 state bucket
ACCOUNT_ID=$(aws sts get-caller-identity --query 'Account' --output text)
STATE_BUCKET="schemabot-terraform-state-${ACCOUNT_ID}"
LOCK_TABLE="schemabot-terraform-lock"

# Ensure S3 state bucket exists (idempotent)
if ! aws s3api head-bucket --bucket "$STATE_BUCKET" 2>/dev/null; then
    echo "   Creating S3 state bucket: $STATE_BUCKET"
    aws s3api create-bucket \
        --bucket "$STATE_BUCKET" \
        --region "$REGION" \
        --create-bucket-configuration LocationConstraint="$REGION"
    aws s3api put-bucket-versioning \
        --bucket "$STATE_BUCKET" \
        --versioning-configuration Status=Enabled
fi

# Ensure DynamoDB lock table exists (idempotent)
if ! aws dynamodb describe-table --table-name "$LOCK_TABLE" --region "$REGION" > /dev/null 2>&1; then
    echo "   Creating DynamoDB lock table: $LOCK_TABLE"
    aws dynamodb create-table \
        --table-name "$LOCK_TABLE" \
        --attribute-definitions AttributeName=LockID,AttributeType=S \
        --key-schema AttributeName=LockID,KeyType=HASH \
        --billing-mode PAY_PER_REQUEST \
        --region "$REGION"
    aws dynamodb wait table-exists --table-name "$LOCK_TABLE" --region "$REGION"
fi

# Initialize Terraform
echo "📦 Initializing Terraform..."
terraform init -upgrade \
    -backend-config="bucket=$STATE_BUCKET" \
    -backend-config="dynamodb_table=$LOCK_TABLE"

# Phase 1: Ensure ECR repository exists (App Runner needs an image to start)
echo ""
echo "🏗️  Phase 1: ECR repository..."
terraform apply -target=aws_ecr_repository.schemabot $AUTO_APPROVE

if [ $? -ne 0 ]; then
    echo "❌ Terraform apply failed (ECR)"
    exit 1
fi

# Phase 2: Build and push Docker image (skips if image for current commit exists)
echo ""
echo "🐳 Phase 2: Docker image..."

ECR_URL=$(terraform output -raw ecr_repository_url)
GIT_SHA=$(git -C "$REPO_ROOT" rev-parse --short HEAD)

# Check if image for this commit already exists in ECR
IMAGE_EXISTS=$(aws ecr describe-images \
    --repository-name "$(basename "$ECR_URL")" \
    --image-ids imageTag="$GIT_SHA" \
    --region "$REGION" \
    --output text 2>/dev/null || true)

if [ -n "$IMAGE_EXISTS" ]; then
    echo "   ✅ Image for $GIT_SHA already exists, skipping build"
else
    echo "   Building and pushing image for $GIT_SHA..."
    echo ""

    aws ecr get-login-password --region "$REGION" | \
        docker login --username AWS --password-stdin "$ECR_URL"

    cd "$REPO_ROOT"
    docker build --platform linux/amd64 -t "$ECR_URL:$GIT_SHA" -t "$ECR_URL:latest" -f deploy/Dockerfile .
    docker push "$ECR_URL:$GIT_SHA"
    docker push "$ECR_URL:latest"
    cd "$TF_DIR"
fi

# Phase 3: Create infrastructure (everything except App Runner)
# App Runner is created separately so infra is ready before the first deploy.
echo ""
echo "🏗️  Phase 3: Infrastructure (RDS, Bastion, Secrets Manager, etc.)..."

# Target everything except App Runner service and VPC connector.
# VPC connector must be applied with App Runner (Phase 4) because
# the old connector can't be deleted while App Runner references it.
INFRA_TARGETS=(
    -target=aws_db_parameter_group.schemabot
    -target=aws_db_instance.schemabot
    -target=aws_db_instance.testapp_staging
    -target=aws_db_instance.testapp_production
    -target=aws_instance.bastion
    -target=aws_iam_role_policy_attachment.bastion_ssm
    -target=aws_secretsmanager_secret.storage_dsn
    -target=aws_secretsmanager_secret.testapp_staging_dsn
    -target=aws_secretsmanager_secret.testapp_production_dsn
    -target=aws_secretsmanager_secret.github_app
    -target=aws_secretsmanager_secret_version.storage_dsn
    -target=aws_secretsmanager_secret_version.testapp_staging_dsn
    -target=aws_secretsmanager_secret_version.testapp_production_dsn
    -target=aws_secretsmanager_secret_version.github_app
    -target=aws_vpc_endpoint.secretsmanager
)

# Check if infra needs changes
if terraform plan -detailed-exitcode "${INFRA_TARGETS[@]}" > /dev/null 2>&1; then
    echo "   ✅ No infrastructure changes needed"
else
    terraform apply $AUTO_APPROVE "${INFRA_TARGETS[@]}"
    if [ $? -ne 0 ]; then
        echo "❌ Terraform apply failed (infrastructure)"
        exit 1
    fi
fi

# Phase 3.5: Set up SchemaBot MySQL user on target RDS instances
echo ""
echo "🔑 Phase 3.5: Setting up SchemaBot MySQL user..."

TF_OUTPUT=$(terraform output -json) || {
    echo "❌ Could not get terraform outputs. Run: terraform apply"
    exit 1
}
BASTION_ID=$(echo "$TF_OUTPUT" | jq -r '.bastion_instance_id.value // empty')

if [ -z "$BASTION_ID" ] || [ "$BASTION_ID" = "null" ]; then
    echo "   ⚠️  Bastion not available, skipping MySQL user setup"
    echo "   Run deploy/setup-db-user.sh manually after infrastructure is ready"
else
    # Generate a single password for the schemabot user (shared across RDS instances).
    # Written to a temp file — never printed to terminal.
    SB_PASSWORD_FILE=$(mktemp)
    chmod 600 "$SB_PASSWORD_FILE"
    openssl rand -hex 16 > "$SB_PASSWORD_FILE"

    cleanup_password_files() {
        rm -f "$SB_PASSWORD_FILE" 2>/dev/null || true
    }
    trap cleanup_password_files EXIT

    # Helper: run setup-db-user.sh against an RDS instance via SSM tunnel
    setup_rds_user() {
        local label="$1" endpoint="$2" dsn="$3" target_db="$4" sb_user="${5:-schemabot}"
        local host username password local_port

        host=$(echo "$endpoint" | cut -d: -f1)
        username=$(echo "$dsn" | sed 's/:.*//')
        password=$(echo "$dsn" | sed 's/[^:]*://' | sed 's/@.*//')

        # Find a free port
        local_port=$((49152 + RANDOM % 16384))
        while nc -z localhost "$local_port" 2>/dev/null; do
            local_port=$((49152 + RANDOM % 16384))
        done

        echo "   Setting up user on $label ($host via port $local_port)..."

        # Start SSM tunnel in background
        aws ssm start-session \
            --target "$BASTION_ID" \
            --region "$REGION" \
            --document-name AWS-StartPortForwardingSessionToRemoteHost \
            --parameters "{\"host\":[\"$host\"],\"portNumber\":[\"3306\"],\"localPortNumber\":[\"$local_port\"]}" \
            > /dev/null 2>&1 &
        local ssm_pid=$!

        # Wait for tunnel
        local attempts=0
        while ! nc -z localhost "$local_port" 2>/dev/null; do
            if [ $attempts -ge 30 ]; then
                echo "   ❌ Tunnel to $label failed"
                kill $ssm_pid 2>/dev/null || true
                return 1
            fi
            sleep 1
            attempts=$((attempts + 1))
        done

        # Run setup script (admin password via env, schemabot password via file)
        MYSQL_PWD="$password" "$REPO_ROOT/deploy/setup-db-user.sh" \
            -h 127.0.0.1 -P "$local_port" \
            -u "$username" \
            -d "$target_db" \
            --sb-user "$sb_user" \
            --sb-password-file "$SB_PASSWORD_FILE"

        kill $ssm_pid 2>/dev/null || true
        wait $ssm_pid 2>/dev/null || true
    }

    # All RDS instances use "schemabot_spirit" as the dedicated user to avoid
    # conflicting with the "schemabot" RDS master user on each instance.
    STORAGE_ENDPOINT=$(echo "$TF_OUTPUT" | jq -r '.schemabot_endpoint.value // empty')
    STORAGE_DSN=$(echo "$TF_OUTPUT" | jq -r '.schemabot_dsn.value // empty')
    setup_rds_user "storage" "$STORAGE_ENDPOINT" "$STORAGE_DSN" "schemabot" "schemabot_spirit"

    STAGING_ENDPOINT=$(echo "$TF_OUTPUT" | jq -r '.testapp_staging_endpoint.value // empty')
    STAGING_DSN=$(echo "$TF_OUTPUT" | jq -r '.testapp_dsns.value.staging // empty')
    setup_rds_user "staging" "$STAGING_ENDPOINT" "$STAGING_DSN" "testapp" "schemabot_spirit"

    PROD_ENDPOINT=$(echo "$TF_OUTPUT" | jq -r '.testapp_production_endpoint.value // empty')
    PROD_DSN=$(echo "$TF_OUTPUT" | jq -r '.testapp_dsns.value.production // empty')
    setup_rds_user "production" "$PROD_ENDPOINT" "$PROD_DSN" "testapp" "schemabot_spirit"

    # Update Secrets Manager DSNs to use the dedicated Spirit user
    echo ""
    echo "   Updating Secrets Manager DSNs..."

    SB_PASS=$(cat "$SB_PASSWORD_FILE")

    STORAGE_SECRET=$(echo "$TF_OUTPUT" | jq -r '.storage_dsn_secret_id.value // empty')
    STAGING_SECRET=$(echo "$TF_OUTPUT" | jq -r '.testapp_staging_dsn_secret_id.value // empty')
    PROD_SECRET=$(echo "$TF_OUTPUT" | jq -r '.testapp_production_dsn_secret_id.value // empty')

    aws secretsmanager put-secret-value \
        --secret-id "$STORAGE_SECRET" \
        --secret-string "$(jq -n --arg dsn "schemabot_spirit:${SB_PASS}@tcp(${STORAGE_ENDPOINT})/schemabot?parseTime=true&multiStatements=true&tls=true" '{dsn: $dsn}')" \
        --region "$REGION"

    aws secretsmanager put-secret-value \
        --secret-id "$STAGING_SECRET" \
        --secret-string "$(jq -n --arg dsn "schemabot_spirit:${SB_PASS}@tcp(${STAGING_ENDPOINT})/testapp?parseTime=true&tls=true" '{dsn: $dsn}')" \
        --region "$REGION"

    aws secretsmanager put-secret-value \
        --secret-id "$PROD_SECRET" \
        --secret-string "$(jq -n --arg dsn "schemabot_spirit:${SB_PASS}@tcp(${PROD_ENDPOINT})/testapp?parseTime=true&tls=true" '{dsn: $dsn}')" \
        --region "$REGION"

    echo "   ✅ MySQL user setup and secrets update complete"
fi

# Phase 4: Apply full Terraform (creates/updates App Runner service)
echo ""
echo "🏗️  Phase 4: App Runner service..."

if terraform plan -detailed-exitcode > /dev/null 2>&1; then
    echo "   ✅ No changes needed"
else
    terraform apply $AUTO_APPROVE
    if [ $? -ne 0 ]; then
        echo "❌ Terraform apply failed"
        exit 1
    fi
fi

echo ""
echo "✅ Bootstrap complete!"
echo ""
echo "📍 Service URL: $(terraform output -raw service_url)"
echo "🔗 Webhook URL: $(terraform output -raw webhook_url)"
