#!/bin/bash
set -euo pipefail

# Bootstrap a PlanetScale database for SchemaBot with sharded + unsharded keyspaces.
# Creates the database, credentials, and stores them in AWS Secrets Manager.
#
# Prerequisites:
#   - pscale CLI installed: brew install planetscale/tap/pscale
#   - Authenticated: pscale auth login
#   - AWS CLI configured (for Secrets Manager)
#   - Terraform state initialized (run bootstrap.sh first)
#
# Usage:
#   ../scripts/bootstrap-planetscale.sh --org <org> --env staging create
#   ../scripts/bootstrap-planetscale.sh --org <org> --env staging delete
#   ../scripts/bootstrap-planetscale.sh --org <org> status
#
# Run from the environment directory (e.g., deploy/aws-multi-env/staging/).

# ============================================================================
# Defaults
# ============================================================================

PS_ORG=""
PS_DATABASE="commerce"
PS_REGION="us-west"
PS_CLUSTER_SIZE="PS-10"
PS_SHARDED_SHARD_COUNT=2
PS_COST_PER_SHARD=39
PS_ENV=""
AWS_REGION="${AWS_DEFAULT_REGION:-us-west-2}"

# ============================================================================
# Parse flags
# ============================================================================

while [[ $# -gt 0 ]]; do
    case $1 in
        --help|-h)
            COMMAND="--help"
            break
            ;;
        --org)
            PS_ORG="$2"
            shift 2
            ;;
        --env)
            PS_ENV="$2"
            shift 2
            ;;
        --aws-profile)
            export AWS_PROFILE="$2"
            shift 2
            ;;
        --database)
            PS_DATABASE="$2"
            shift 2
            ;;
        --region)
            PS_REGION="$2"
            shift 2
            ;;
        --cluster-size)
            PS_CLUSTER_SIZE="$2"
            shift 2
            ;;
        --shards)
            PS_SHARDED_SHARD_COUNT="$2"
            shift 2
            ;;
        *)
            break
            ;;
    esac
done

COMMAND="${1:-}"
PS_SHARDED_KEYSPACE="${PS_DATABASE}_sharded"

if [ -z "$PS_ORG" ] && [ "$COMMAND" != "--help" ]; then
    echo "Error: --org <org-name> is required"
    echo "Usage: $0 --org <org-name> --env <environment> [options] <command>"
    echo "Run '$0 --help' for details."
    exit 1
fi

if [ -z "$COMMAND" ]; then
    echo "Error: command required (create, status, delete)"
    echo "Usage: $0 --org <org-name> --env <environment> [options] <command>"
    exit 1
fi

# --env is required for create and delete (Secrets Manager access)
if [ -z "$PS_ENV" ] && [ "$COMMAND" != "status" ] && [ "$COMMAND" != "--help" ] && [ "$COMMAND" != "-h" ]; then
    echo "Error: --env <environment> is required for $COMMAND"
    echo "Usage: $0 --org <org-name> --env <environment> $COMMAND"
    exit 1
fi

if ! command -v pscale &> /dev/null; then
    echo "Error: pscale CLI not installed"
    echo "Install: brew install planetscale/tap/pscale"
    echo "Then:    pscale auth login"
    exit 1
fi

ORG_FLAG="--org $PS_ORG"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MULTI_ENV_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

log()     { echo -e "${BLUE}[INFO]${NC} $1"; }
success() { echo -e "${GREEN}[OK]${NC} $1"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $1"; }
error()   { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

# ============================================================================
# Secrets Manager helpers
# ============================================================================

# Secrets Manager prefix follows the naming convention: schemabot-<env>.
get_sm_prefix() {
    echo "schemabot-${PS_ENV}"
}

create_or_update_secret() {
    local secret_id="$1"
    local secret_value="$2"
    local description="$3"

    log "Checking if secret exists: $secret_id (region: $AWS_REGION, profile: ${AWS_PROFILE:-default})"
    if aws secretsmanager describe-secret --region "$AWS_REGION" --secret-id "$secret_id" > /dev/null 2>&1; then
        log "Updating existing secret: $secret_id"
        if ! aws secretsmanager update-secret \
            --region "$AWS_REGION" \
            --secret-id "$secret_id" \
            --secret-string "$secret_value"; then
            error "Failed to update secret: $secret_id"
        fi
        success "Secret updated: $secret_id"
    else
        log "Creating new secret: $secret_id"
        if ! aws secretsmanager create-secret \
            --region "$AWS_REGION" \
            --name "$secret_id" \
            --description "$description" \
            --secret-string "$secret_value"; then
            error "Failed to create secret: $secret_id"
        fi
        success "Secret created: $secret_id"
    fi
}

store_credentials_in_sm() {
    local service_token="$1"
    local vtgate_dsn="$2"

    local prefix
    prefix=$(get_sm_prefix)

    log "Secrets Manager prefix: $prefix"
    log "AWS profile: ${AWS_PROFILE:-default}"
    log "AWS region: $AWS_REGION"

    local token_value
    token_value=$(jq -n --arg token "$service_token" '{"token": $token}')
    create_or_update_secret "$prefix/planetscale-token" "$token_value" "PlanetScale service token for SchemaBot"

    local vtgate_value
    vtgate_value=$(jq -n --arg dsn "$vtgate_dsn" '{"dsn": $dsn}')
    create_or_update_secret "$prefix/planetscale-vtgate" "$vtgate_value" "PlanetScale vtgate credentials for SchemaBot"
}

# ============================================================================
# create
# ============================================================================

cmd_create() {
    log "Creating PlanetScale database '$PS_DATABASE' in org '$PS_ORG'..."
    echo ""

    if pscale database show "$PS_DATABASE" $ORG_FLAG &>/dev/null; then
        error "Database '$PS_DATABASE' already exists in org '$PS_ORG'"
    fi

    # Step 1: Create database (creates default unsharded keyspace with same name)
    log "Step 1/7: Creating database '$PS_DATABASE' (unsharded keyspace, $PS_CLUSTER_SIZE)..."
    pscale database create "$PS_DATABASE" \
        --region "$PS_REGION" \
        --cluster-size "$PS_CLUSTER_SIZE" \
        --wait \
        $ORG_FLAG
    success "Database created with unsharded keyspace '$PS_DATABASE'"

    # Step 2: Create sharded keyspace
    log "Step 2/7: Creating sharded keyspace '$PS_SHARDED_KEYSPACE' ($PS_SHARDED_SHARD_COUNT shards, $PS_CLUSTER_SIZE each)..."
    pscale keyspace create "$PS_DATABASE" main "$PS_SHARDED_KEYSPACE" \
        --shards "$PS_SHARDED_SHARD_COUNT" \
        --cluster-size "$PS_CLUSTER_SIZE" \
        --wait \
        $ORG_FLAG
    success "Sharded keyspace created"

    # Step 3: Enable safe migrations on main branch (required for deploy requests)
    log "Step 3/7: Enabling safe migrations on main branch..."
    pscale branch safe-migrations enable "$PS_DATABASE" main $ORG_FLAG
    success "Safe migrations enabled"

    # Step 4: Create service token
    local token_name="schemabot-${PS_DATABASE}"
    log "Step 4/7: Creating service token '$token_name'..."
    local token_json
    token_json=$(pscale service-token create --name "$token_name" --format json $ORG_FLAG)
    local token_id token_secret
    token_id=$(echo "$token_json" | jq -re '.id') || error "Failed to parse service token ID from output"
    token_secret=$(echo "$token_json" | jq -re '.token') || error "Failed to parse service token secret from output"
    success "Service token created: $token_id"

    # Step 5: Grant permissions
    log "Step 5/7: Granting database permissions..."
    pscale service-token add-access "$token_id" \
        approve_deploy_request \
        connect_branch \
        create_branch \
        create_comment \
        create_deploy_request \
        delete_branch_password \
        read_branch \
        read_comment \
        read_database \
        read_deploy_request \
        write_branch_vschema \
        --database "$PS_DATABASE" \
        $ORG_FLAG
    success "Permissions granted (11 access types)"

    # Step 6: Create vtgate password for progress polling (SHOW VITESS_MIGRATIONS)
    local vtgate_name="schemabot-${PS_DATABASE}-vtgate"
    log "Step 6/7: Creating vtgate password '$vtgate_name'..."
    local vtgate_json
    vtgate_json=$(pscale password create "$PS_DATABASE" main "$vtgate_name" --role reader --format json $ORG_FLAG)
    local vtgate_host vtgate_user vtgate_pass
    vtgate_host=$(echo "$vtgate_json" | jq -re '.access_host_url') || error "Failed to parse vtgate host from output"
    vtgate_user=$(echo "$vtgate_json" | jq -re '.username') || error "Failed to parse vtgate username from output"
    vtgate_pass=$(echo "$vtgate_json" | jq -re '.plain_text') || error "Failed to parse vtgate password from output"
    success "Vtgate password created"

    local service_token="${token_id}:${token_secret}"
    local vtgate_dsn="${vtgate_user}:${vtgate_pass}@tcp(${vtgate_host}:3306)/"

    # Step 7: Store credentials in Secrets Manager
    log "Step 7/7: Storing credentials in AWS Secrets Manager..."
    store_credentials_in_sm "$service_token" "$vtgate_dsn"
    success "Credentials stored in Secrets Manager"

    # Save credentials to env directory as backup (gitignored)
    local creds_file="$MULTI_ENV_DIR/$PS_ENV/.planetscale-credentials"
    cat > "$creds_file" <<CREDS
# Generated by bootstrap-planetscale.sh — do not commit this file.
TOKEN='${service_token}'
VTGATE_DSN='${vtgate_dsn}'
CREDS

    # Summary
    local total_shards=$((1 + PS_SHARDED_SHARD_COUNT))
    local total_cost=$((total_shards * PS_COST_PER_SHARD))

    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo "  PlanetScale Setup Complete"
    echo "═══════════════════════════════════════════════════════════════"
    echo ""
    echo "  Database:     $PS_DATABASE"
    echo "  Organization: $PS_ORG"
    echo "  Region:       $PS_REGION"
    echo ""
    echo "  Keyspaces:"
    echo "    - $PS_DATABASE (unsharded, 1 shard)"
    echo "    - $PS_SHARDED_KEYSPACE (sharded, $PS_SHARDED_SHARD_COUNT shards)"
    echo ""
    echo "  Cost: ~\$$total_cost/mo ($total_shards shards × \$$PS_COST_PER_SHARD)"
    echo ""
    echo "  Credentials: stored in Secrets Manager"
    echo "  Backup:      .planetscale-credentials (gitignored)"
    echo ""
    echo "  Next steps:"
    echo "    1. Update config.yaml organization field:"
    echo "       organization: \"$PS_ORG\""
    echo ""
    echo "    2. Redeploy SchemaBot:"
    echo "       ../scripts/deploy.sh"
    echo ""
    echo "═══════════════════════════════════════════════════════════════"
}

# ============================================================================
# status
# ============================================================================

cmd_status() {
    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo "  PlanetScale Status"
    echo "═══════════════════════════════════════════════════════════════"
    echo ""

    local total_shards=$((1 + PS_SHARDED_SHARD_COUNT))
    local total_cost=$((total_shards * PS_COST_PER_SHARD))

    local ps_status
    ps_status=$(pscale database show "$PS_DATABASE" --format json $ORG_FLAG 2>/dev/null | jq -r '.state' || echo "not-found")

    echo -e "  Database:     $PS_DATABASE"
    echo -e "  Organization: $PS_ORG"
    echo ""

    case "$ps_status" in
        ready)
            echo -e "  Status: ${GREEN}running${NC}"
            echo ""
            echo "  Keyspaces:"
            echo "    - $PS_DATABASE (unsharded, 1 shard)"
            echo "    - $PS_SHARDED_KEYSPACE (sharded, $PS_SHARDED_SHARD_COUNT shards)"
            echo ""
            echo "  Cost: ~\$$total_cost/mo ($total_shards shards × \$$PS_COST_PER_SHARD)"
            ;;
        not-found)
            echo -e "  Status: ${YELLOW}not found${NC}"
            echo ""
            echo "  Run '$0 --org $PS_ORG --env <env> create' to create the database"
            ;;
        *)
            echo "  Status: $ps_status"
            ;;
    esac

    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo ""
}

# ============================================================================
# delete
# ============================================================================

cmd_delete() {
    if ! pscale database show "$PS_DATABASE" $ORG_FLAG &>/dev/null; then
        error "Database '$PS_DATABASE' not found in org '$PS_ORG'"
    fi

    local total_shards=$((1 + PS_SHARDED_SHARD_COUNT))
    local total_cost=$((total_shards * PS_COST_PER_SHARD))

    echo ""
    warn "This will DELETE the PlanetScale database: $PS_DATABASE"
    warn "This includes all service tokens with access to this database."
    warn "Organization: $PS_ORG"
    warn "Monthly savings: ~\$$total_cost"
    echo ""
    read -p "Type the database name to confirm: " confirm

    if [ "$confirm" != "$PS_DATABASE" ]; then
        error "Cancelled — input did not match database name"
    fi

    # Delete service token created by the create command.
    log "Cleaning up service tokens..."
    local tokens
    tokens=$(pscale service-token list --format json $ORG_FLAG 2>/dev/null || echo '[]')
    local token_name="schemabot-${PS_DATABASE}"
    local tid
    tid=$(echo "$tokens" | jq -r ".[] | select(.name == \"$token_name\") | .id")
    if [ -n "$tid" ]; then
        log "Deleting service token '$token_name' ($tid)..."
        pscale service-token delete "$tid" $ORG_FLAG
        success "Service token deleted"
    else
        warn "Service token '$token_name' not found, skipping"
    fi

    log "Deleting database $PS_DATABASE..."
    pscale database delete "$PS_DATABASE" --force $ORG_FLAG
    success "Database deleted"

    # Clean up Secrets Manager entries
    log "Cleaning up Secrets Manager secrets..."
    local prefix
    prefix=$(get_sm_prefix)
    for secret_suffix in planetscale-token planetscale-vtgate; do
        local secret_id="$prefix/$secret_suffix"
        if aws secretsmanager describe-secret --region "$AWS_REGION" --secret-id "$secret_id" > /dev/null 2>&1; then
            log "Deleting secret: $secret_id"
            aws secretsmanager delete-secret --region "$AWS_REGION" --secret-id "$secret_id" --force-delete-without-recovery > /dev/null
            success "Secret deleted: $secret_id"
        else
            warn "Secret not found: $secret_id (skipping)"
        fi
    done

    # Clean up local credentials file
    local creds_file="$MULTI_ENV_DIR/$PS_ENV/.planetscale-credentials"
    if [ -f "$creds_file" ]; then
        rm "$creds_file"
        success "Removed $creds_file"
    fi
}

# ============================================================================
# Main
# ============================================================================

case "$COMMAND" in
    create) cmd_create ;;
    status) cmd_status ;;
    delete) cmd_delete ;;
    --help|-h)
        echo "Bootstrap a PlanetScale database for SchemaBot"
        echo ""
        echo "Usage: $0 --org <org> --env <environment> [options] <command>"
        echo ""
        echo "Run from the environment directory (e.g., deploy/aws-multi-env/staging/)."
        echo ""
        echo "Commands:"
        echo "  create    Create database, credentials, and store in Secrets Manager"
        echo "  status    Show database status and cost estimate"
        echo "  delete    Delete database and service tokens (with confirmation)"
        echo ""
        echo "Required:"
        echo "  --org <org>            PlanetScale organization"
        echo "  --env <environment>    Target environment (staging, production)"
        echo ""
        echo "Options:"
        echo "  --aws-profile <name>   AWS CLI profile (default: \$AWS_PROFILE)"
        echo "  --database <name>      Database name (default: commerce)"
        echo "  --region <region>      PlanetScale region (default: us-west)"
        echo "  --cluster-size <size>  Cluster size (default: PS-10, \$${PS_COST_PER_SHARD}/shard/mo)"
        echo "  --shards <n>           Shards for sharded keyspace (default: 2)"
        echo ""
        echo "Prerequisites:"
        echo "  brew install planetscale/tap/pscale"
        echo "  pscale auth login"
        echo ""
        echo "Database structure:"
        echo "  <database>          — unsharded keyspace (1 shard)"
        echo "  <database>_sharded  — sharded keyspace (<n> shards)"
        echo ""
        exit 0
        ;;
    *)
        echo "Unknown command: $COMMAND"
        echo "Usage: $0 --org <org> --env <environment> [options] <command>"
        echo "Commands: create, status, delete"
        exit 1
        ;;
esac
