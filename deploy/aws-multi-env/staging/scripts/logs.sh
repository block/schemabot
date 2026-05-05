#!/bin/bash
set -euo pipefail

# View App Runner logs for SchemaBot
# Usage: ./logs.sh [options] [duration] [filter]
#
# Examples:
#   ./logs.sh               # Application logs, last 30 minutes
#   ./logs.sh 1h            # Application logs, last 1 hour
#   ./logs.sh 10m error     # Application logs, last 10 minutes, filter for "error"
#   ./logs.sh -f            # Follow application logs in real-time
#   ./logs.sh --service     # App Runner service events (deploys, health checks, crashes)
#   ./logs.sh --all         # Both application and service logs interleaved

REGION="us-west-2"
export AWS_PROFILE="${AWS_PROFILE:-schemabot-deployer}"

# Defaults
DURATION="30m"
FILTER=""
FOLLOW=false
LOG_TYPE="application"  # application, service, or all

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -f|--follow)
            FOLLOW=true
            shift
            ;;
        --service)
            LOG_TYPE="service"
            shift
            ;;
        --all)
            LOG_TYPE="all"
            shift
            ;;
        --app|--application)
            LOG_TYPE="application"
            shift
            ;;
        -h|--help)
            echo "Usage: ./logs.sh [options] [duration] [filter]"
            echo ""
            echo "Options:"
            echo "  --app, --application  Application logs (default)"
            echo "  --service             App Runner service events (deploys, health checks)"
            echo "  --all                 Both application and service logs"
            echo "  -f, --follow          Follow logs in real-time"
            echo "  -h, --help            Show this help"
            echo ""
            echo "Duration: 5m, 30m, 1h, 2h, etc. Default: 30m"
            echo "Filter: text string to filter log messages"
            echo ""
            echo "Examples:"
            echo "  ./logs.sh                     # App logs, last 30 minutes"
            echo "  ./logs.sh --service           # Service events (deploy failures, health checks)"
            echo "  ./logs.sh --all 1h            # All logs, last 1 hour"
            echo "  ./logs.sh 10m error           # App logs with 'error' filter"
            echo "  ./logs.sh -f                  # Follow app logs in real-time"
            exit 0
            ;;
        [0-9]*m|[0-9]*h)
            DURATION="$1"
            shift
            ;;
        *)
            FILTER="$1"
            shift
            ;;
    esac
done

# Go to parent dir where main.tf lives
cd "$(dirname "$0")/.."

# Find log groups (sort by creation time, pick the most recent)
APP_LOG_GROUP=$(aws logs describe-log-groups \
    --log-group-name-prefix "/aws/apprunner/schemabot-example" \
    --region "$REGION" \
    --query 'sort_by(logGroups[?contains(logGroupName, `/application`)], &creationTime) | [-1].logGroupName' \
    --output text 2>/dev/null)

SVC_LOG_GROUP=$(aws logs describe-log-groups \
    --log-group-name-prefix "/aws/apprunner/schemabot-example" \
    --region "$REGION" \
    --query 'sort_by(logGroups[?contains(logGroupName, `/service`)], &creationTime) | [-1].logGroupName' \
    --output text 2>/dev/null)

# Determine which log groups to query
LOG_GROUPS=()
case "$LOG_TYPE" in
    application)
        if [ -n "$APP_LOG_GROUP" ] && [ "$APP_LOG_GROUP" != "None" ]; then
            LOG_GROUPS+=("$APP_LOG_GROUP")
        else
            echo "⚠️  No application logs found (service may not have started)"
            echo "   Try: ./logs.sh --service"
            exit 1
        fi
        ;;
    service)
        if [ -n "$SVC_LOG_GROUP" ] && [ "$SVC_LOG_GROUP" != "None" ]; then
            LOG_GROUPS+=("$SVC_LOG_GROUP")
        else
            echo "❌ No service logs found"
            exit 1
        fi
        ;;
    all)
        if [ -n "$APP_LOG_GROUP" ] && [ "$APP_LOG_GROUP" != "None" ]; then
            LOG_GROUPS+=("$APP_LOG_GROUP")
        fi
        if [ -n "$SVC_LOG_GROUP" ] && [ "$SVC_LOG_GROUP" != "None" ]; then
            LOG_GROUPS+=("$SVC_LOG_GROUP")
        fi
        if [ ${#LOG_GROUPS[@]} -eq 0 ]; then
            echo "❌ No log groups found"
            exit 1
        fi
        ;;
esac

echo "📋 SchemaBot Logs"
for lg in "${LOG_GROUPS[@]}"; do
    echo "   Log group: $lg"
done
echo "   Duration: $DURATION"
[ -n "$FILTER" ] && echo "   Filter: $FILTER"
echo ""

# Convert duration to milliseconds
case "$DURATION" in
    *m) DURATION_MS=$((${DURATION%m} * 60 * 1000)) ;;
    *h) DURATION_MS=$((${DURATION%h} * 60 * 60 * 1000)) ;;
    *)  DURATION_MS=$((30 * 60 * 1000)) ;;
esac

START_TIME=$(($(date +%s) * 1000 - DURATION_MS))

# Fetch and display logs
fetch_logs() {
    local log_group="$1"
    local label="$2"

    if [ "$FOLLOW" = true ]; then
        echo "📡 Following $label (Ctrl+C to stop)..."
        echo ""
        if [ -n "$FILTER" ]; then
            aws logs tail "$log_group" \
                --region "$REGION" \
                --follow \
                --filter-pattern "$FILTER" \
                --format short
        else
            aws logs tail "$log_group" \
                --region "$REGION" \
                --follow \
                --format short
        fi
    else
        local events
        if [ -n "$FILTER" ]; then
            events=$(aws logs filter-log-events \
                --log-group-name "$log_group" \
                --region "$REGION" \
                --start-time "$START_TIME" \
                --filter-pattern "$FILTER" \
                --query 'events[*].[timestamp,message]' \
                --output text 2>/dev/null)
        else
            events=$(aws logs filter-log-events \
                --log-group-name "$log_group" \
                --region "$REGION" \
                --start-time "$START_TIME" \
                --query 'events[*].[timestamp,message]' \
                --output text 2>/dev/null)
        fi

        if [ -z "$events" ]; then
            echo "   (no $label in the last $DURATION)"
            return
        fi

        echo "$events" | while IFS=$'\t' read -r ts msg; do
            if [ -n "$ts" ] && [ "$ts" != "None" ]; then
                echo "$(date -r $((ts/1000)) '+%H:%M:%S') $msg"
            fi
        done
    fi
}

# For --all mode, show service logs first (deploy events), then application logs
if [ "$LOG_TYPE" = "all" ]; then
    if [ -n "$SVC_LOG_GROUP" ] && [ "$SVC_LOG_GROUP" != "None" ]; then
        echo "── Service Events ──────────────────────────────"
        fetch_logs "$SVC_LOG_GROUP" "service events"
        echo ""
    fi
    if [ -n "$APP_LOG_GROUP" ] && [ "$APP_LOG_GROUP" != "None" ]; then
        echo "── Application Logs ────────────────────────────"
        fetch_logs "$APP_LOG_GROUP" "application logs"
    fi
elif [ "$FOLLOW" = true ] && [ ${#LOG_GROUPS[@]} -gt 1 ]; then
    # Follow only works with one log group at a time
    echo "⚠️  Follow mode only supports one log group. Use --app or --service with -f."
    exit 1
else
    fetch_logs "${LOG_GROUPS[0]}" "$LOG_TYPE logs"
fi
