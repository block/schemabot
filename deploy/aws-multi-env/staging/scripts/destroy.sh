#!/bin/bash
set -euo pipefail

# Destroy all SchemaBot AWS infrastructure
# Usage: ./destroy.sh
#
# Always requires interactive confirmation — no auto-approve flag.

REGION="us-west-2"
export AWS_PROFILE="${AWS_PROFILE:-schemabot-deployer}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TF_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$TF_DIR"

echo "🗑️  SchemaBot AWS Destroy"
echo "=========================="
echo ""

# Show what exists
RESOURCE_COUNT=$(terraform state list 2>/dev/null | wc -l | tr -d ' ')
if [ "$RESOURCE_COUNT" = "0" ]; then
    echo "   No resources in terraform state. Nothing to destroy."
    exit 0
fi

echo "   $RESOURCE_COUNT resources in terraform state"
echo ""
echo "⚠️  This will destroy ALL SchemaBot infrastructure:"
echo "   - RDS instances (storage, staging, production)"
echo "   - App Runner service"
echo "   - ECR repository and images"
echo "   - Bastion host"
echo "   - Secrets Manager secrets"
echo "   - VPC endpoints, subnets, security groups"
echo ""
read -rp "Type 'destroy' to confirm: " CONFIRM
if [ "$CONFIRM" != "destroy" ]; then
    echo "Aborted."
    exit 1
fi

echo ""
echo "🔥 Destroying infrastructure..."
terraform destroy

echo ""
echo "✅ All infrastructure destroyed."
