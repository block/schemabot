#!/usr/bin/env bash
#
# Obtain an OIDC token from the local Dex instance for testing.
#
# This uses the OAuth2 authorization code flow with a temporary HTTP server
# to capture the callback. It opens the browser for login, the user signs in
# with static credentials, and the script exchanges the code for a token.
#
# Usage:
#   ./scripts/get-oidc-token.sh                    # prints the access token
#   TOKEN=$(./scripts/get-oidc-token.sh)            # capture for use
#   curl -H "Authorization: Bearer $TOKEN" ...      # use with API
#
# Prerequisites:
#   - Local Dex running: docker compose --profile oidc up
#   - curl and python3 available
#
# Test credentials (from deploy/local/config/dex.yaml):
#   Admin:  admin@example.com  / password   (group: admins — read+write)
#   Viewer: viewer@example.com / password   (group: viewers — read only)
#
set -euo pipefail

DEX_URL="${DEX_URL:-http://localhost:5556/dex}"
CLIENT_ID="${CLIENT_ID:-schemabot}"
REDIRECT_PORT="${REDIRECT_PORT:-5555}"
REDIRECT_URI="http://localhost:${REDIRECT_PORT}/callback"
SCOPES="openid email groups profile offline_access"

# Verify Dex is running.
if ! curl -sf "${DEX_URL}/.well-known/openid-configuration" > /dev/null 2>&1; then
    echo "Error: Dex is not running at ${DEX_URL}" >&2
    echo "Start it with: docker compose --profile oidc up" >&2
    exit 1
fi

# Generate a random state parameter for CSRF protection.
STATE=$(python3 -c "import secrets; print(secrets.token_urlsafe(16))")

# Build the authorization URL.
AUTH_URL="${DEX_URL}/auth?client_id=${CLIENT_ID}&redirect_uri=${REDIRECT_URI}&response_type=code&scope=$(echo "${SCOPES}" | tr ' ' '+')&state=${STATE}"

echo "Opening browser for Dex login..." >&2
echo "Sign in with: admin@example.com / password" >&2
echo "" >&2

# Open the browser (macOS: open, Linux: xdg-open).
if command -v open > /dev/null 2>&1; then
    open "${AUTH_URL}"
elif command -v xdg-open > /dev/null 2>&1; then
    xdg-open "${AUTH_URL}"
else
    echo "Open this URL in your browser:" >&2
    echo "${AUTH_URL}" >&2
fi

# Start a temporary HTTP server to capture the OAuth callback.
# Python serves exactly one request, extracts the authorization code, and exits.
RESPONSE=$(python3 -c "
import http.server
import urllib.parse
import sys

class CallbackHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)

        if 'code' not in params:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b'No authorization code received.')
            print('ERROR:no_code', file=sys.stderr)
            return

        code = params['code'][0]
        state = params.get('state', [''])[0]

        self.send_response(200)
        self.send_header('Content-Type', 'text/html')
        self.end_headers()
        self.wfile.write(b'<html><body><h2>Login successful!</h2><p>You can close this tab.</p></body></html>')

        # Write code to stdout for the parent script to capture.
        print(f'{code}|{state}', flush=True)

    def log_message(self, format, *args):
        pass  # Suppress HTTP server logs.

server = http.server.HTTPServer(('localhost', ${REDIRECT_PORT}), CallbackHandler)
# Handle exactly one request.
server.handle_request()
server.server_close()
" 2>/dev/null)

# Parse the response.
AUTH_CODE=$(echo "${RESPONSE}" | cut -d'|' -f1)
RETURNED_STATE=$(echo "${RESPONSE}" | cut -d'|' -f2)

if [ -z "${AUTH_CODE}" ]; then
    echo "Error: No authorization code received." >&2
    exit 1
fi

if [ "${RETURNED_STATE}" != "${STATE}" ]; then
    echo "Error: State mismatch (possible CSRF)." >&2
    exit 1
fi

# Exchange the authorization code for tokens.
if ! TOKEN_RESPONSE=$(curl -sf -X POST "${DEX_URL}/token" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "grant_type=authorization_code" \
    -d "code=${AUTH_CODE}" \
    -d "client_id=${CLIENT_ID}" \
    -d "redirect_uri=${REDIRECT_URI}"); then
    echo "Error: Token exchange failed." >&2
    exit 1
fi

# Extract the ID token (Dex returns id_token for OIDC flows).
ID_TOKEN=$(echo "${TOKEN_RESPONSE}" | python3 -c "import sys, json; print(json.load(sys.stdin)['id_token'])")

if [ -z "${ID_TOKEN}" ]; then
    echo "Error: No id_token in response." >&2
    exit 1
fi

# Print token claims to stderr for visibility.
echo "" >&2
echo "Token claims:" >&2
echo "${ID_TOKEN}" | cut -d'.' -f2 | python3 -c "
import sys, base64, json
payload = sys.stdin.read().strip()
# Add padding if needed.
payload += '=' * (4 - len(payload) % 4)
claims = json.loads(base64.urlsafe_b64decode(payload))
for key in ('sub', 'email', 'groups', 'exp'):
    if key in claims:
        print(f'  {key}: {claims[key]}', file=sys.stderr)
" 2>&1 >&2

# Print only the token to stdout so it can be captured with $().
echo "${ID_TOKEN}"
