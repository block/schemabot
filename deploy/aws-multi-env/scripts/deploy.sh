#!/bin/bash
set -euo pipefail

# Build and deploy SchemaBot to AWS App Runner
# Usage: cd deploy/aws-multi-env/staging && ../scripts/deploy.sh [--skip-build]
#
# Options:
#   --skip-build    Skip Docker build, just trigger deployment

REGION="us-west-2"
export AWS_PROFILE="${AWS_PROFILE:-schemabot-deployer}"

SKIP_BUILD=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-build)
            SKIP_BUILD=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: ../scripts/deploy.sh [--skip-build]"
            exit 1
            ;;
    esac
done

echo "🚀 SchemaBot Deployment"
echo "=========================="

# CWD is the environment directory (staging/ or production/).
TF_DIR="$(pwd)"
REPO_ROOT="$(git -C "$TF_DIR" rev-parse --show-toplevel)"

# Ensure working tree is clean (image is tagged by commit SHA)
if [ "$SKIP_BUILD" = false ] && ! git -C "$REPO_ROOT" diff --quiet HEAD 2>/dev/null; then
    echo "❌ Uncommitted changes detected. Commit first — the image is tagged by commit SHA."
    exit 1
fi

# Get terraform outputs (auto-init if needed, e.g. in a fresh worktree)
cd "$TF_DIR"

if ! terraform output -json > /dev/null 2>&1; then
    echo "🔧 Initializing Terraform backend..."
    ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
    terraform init -input=false \
        -backend-config="bucket=schemabot-terraform-state-${ACCOUNT_ID}" \
        -backend-config="dynamodb_table=schemabot-terraform-lock" > /dev/null 2>&1 || {
        echo "❌ Could not initialize Terraform"
        echo "   Run: ../scripts/bootstrap.sh"
        exit 1
    }
fi

TF_OUTPUT=$(terraform output -json 2>/dev/null) || {
    echo "❌ Could not get terraform output"
    echo "   Run: ../scripts/bootstrap.sh"
    exit 1
}

ECR_URL=$(echo "$TF_OUTPUT" | jq -r '.ecr_repository_url.value')
SERVICE_URL=$(echo "$TF_OUTPUT" | jq -r '.service_url.value')

# Extract service ARN from deploy_commands
SERVICE_ARN=$(echo "$TF_OUTPUT" | jq -r '.deploy_commands.value' | grep "service-arn" | sed 's/.*--service-arn //' | tr -d ' ')

echo "   ECR: $ECR_URL"
echo "   Service: $SERVICE_URL"
echo "   Build context: $REPO_ROOT"
echo "   Terraform dir: $TF_DIR"
echo ""

if [ "$SKIP_BUILD" = false ]; then
    GIT_SHA=$(git -C "$REPO_ROOT" rev-parse --short HEAD)

    # Check if image for this commit already exists in ECR
    REPO_NAME=$(basename "$ECR_URL")
    IMAGE_EXISTS=$(aws ecr describe-images \
        --repository-name "$REPO_NAME" \
        --image-ids imageTag="$GIT_SHA" \
        --region "$REGION" \
        --output text 2>/dev/null || true)

    if [ -n "$IMAGE_EXISTS" ]; then
        echo "   ✅ Image for $GIT_SHA already exists, skipping build"
    else
        # Login to ECR
        echo "🔐 Logging in to ECR..."
        aws ecr get-login-password --region "$REGION" | \
            docker login --username AWS --password-stdin "$ECR_URL" 2>/dev/null

        # Build the image
        GIT_FULL_SHA=$(git -C "$REPO_ROOT" rev-parse HEAD)
        BUILD_DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ)

        echo ""
        echo "🔨 Building Docker image ($GIT_SHA)..."
        cd "$REPO_ROOT"
        docker build --platform linux/amd64 \
            --build-arg VERSION="$GIT_SHA" \
            --build-arg COMMIT="$GIT_FULL_SHA" \
            --build-arg BUILD_DATE="$BUILD_DATE" \
            -t "$ECR_URL:$GIT_SHA" -t "$ECR_URL:latest" -f deploy/Dockerfile .

        # Push to ECR
        echo ""
        echo "📤 Pushing to ECR..."
        docker push "$ECR_URL:$GIT_SHA"
        docker push "$ECR_URL:latest"

        cd "$TF_DIR"
    fi
fi

# Trigger deployment
echo ""
echo "🔄 Triggering App Runner deployment..."
OPERATION_ID=$(aws apprunner start-deployment \
    --service-arn "$SERVICE_ARN" \
    --region "$REGION" \
    --query 'OperationId' \
    --output text 2>/dev/null)

echo "   Operation ID: $OPERATION_ID"
echo ""

# Wait for deployment
echo "⏳ Waiting for deployment to complete..."
echo ""

START_TIME=$(date +%s)
MAX_WAIT=600  # 10 minutes

while true; do
    ELAPSED=$(( $(date +%s) - START_TIME ))
    if [ "$ELAPSED" -ge "$MAX_WAIT" ]; then
        echo ""
        echo "❌ Deployment timed out after $(( ELAPSED / 60 ))m$(( ELAPSED % 60 ))s"
        echo "   Check status: aws apprunner list-operations --service-arn $SERVICE_ARN --region $REGION"
        exit 1
    fi

    STATUS=$(aws apprunner list-operations \
        --service-arn "$SERVICE_ARN" \
        --region "$REGION" \
        --query "OperationSummaryList[?Id=='$OPERATION_ID'].Status" \
        --output text 2>/dev/null)

    ELAPSED_FMT="$(( ELAPSED / 60 ))m$(( ELAPSED % 60 ))s"

    case "$STATUS" in
        SUCCEEDED)
            echo ""
            echo "✅ Deployment succeeded! ($ELAPSED_FMT)"
            break
            ;;
        FAILED)
            echo ""
            echo "❌ Deployment failed! ($ELAPSED_FMT)"
            echo "   Check logs: ../scripts/logs.sh --all 10m"
            exit 1
            ;;
        ROLLBACK_SUCCEEDED)
            echo ""
            echo "❌ Deployment failed and rolled back. ($ELAPSED_FMT)"
            echo "   Check logs: ../scripts/logs.sh --all 10m"
            exit 1
            ;;
        ROLLBACK_FAILED)
            echo ""
            echo "❌ Deployment failed and rollback also failed! ($ELAPSED_FMT)"
            echo "   Check logs: ../scripts/logs.sh --all 10m"
            exit 1
            ;;
        ROLLBACK_IN_PROGRESS)
            printf "\r   Rolling back... %s" "$ELAPSED_FMT"
            sleep 10
            ;;
        IN_PROGRESS|PENDING)
            printf "\r   Deploying... %s" "$ELAPSED_FMT"
            sleep 10
            ;;
        *)
            printf "\r   %s... %s" "$STATUS" "$ELAPSED_FMT"
            sleep 10
            ;;
    esac
done

echo ""
echo "🏁 Deployment complete!"
echo ""

# Health check
echo "🔍 Running health check..."
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$SERVICE_URL/health" 2>/dev/null || echo "000")

if [ "$HTTP_CODE" = "200" ]; then
    echo "   ✅ Health check passed"
    curl -s "$SERVICE_URL/health" | jq . 2>/dev/null || curl -s "$SERVICE_URL/health"
else
    echo "   ⚠️  Health check returned: $HTTP_CODE"
fi

echo ""
echo "📍 Service URL: $SERVICE_URL"
