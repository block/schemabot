# SchemaBot Docker Image
#
# Build:
#   docker build -t schemabot -f deploy/Dockerfile .
#
# Run:
#   docker run -p 8080:8080 \
#     -e SCHEMABOT_CONFIG_FILE=/config/schemabot-server-config.yaml \
#     -v ./config:/config \
#     schemabot

# Build stage
FROM golang:1.27-alpine AS builder

# The Alpine CDN intermittently answers "temporary error (try again later)" and a
# single failed fetch fails the whole build, so every apk add in this repo's
# Alpine images retries with backoff instead of failing on the first miss. This
# is the canonical copy of the loop and its rationale; the other Dockerfiles
# point here rather than restating it.
RUN for attempt in 1 2 3 4 5; do \
        apk add --no-cache git ca-certificates && break; \
        echo "apk add git ca-certificates failed (attempt ${attempt}/5)" >&2; \
        [ "$attempt" -lt 5 ] || exit 1; \
        echo "retrying in $((attempt * 5))s" >&2; \
        sleep $((attempt * 5)); \
    done

WORKDIR /src

# Copy go mod files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary (version set via build args)
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=${BUILD_DATE}" \
    -o /schemabot ./pkg/cmd

# Runtime stage
FROM alpine:3.24

# Retried for the reason given on the builder stage's apk add above.
RUN for attempt in 1 2 3 4 5; do \
        apk add --no-cache ca-certificates tzdata && break; \
        echo "apk add ca-certificates tzdata failed (attempt ${attempt}/5)" >&2; \
        [ "$attempt" -lt 5 ] || exit 1; \
        echo "retrying in $((attempt * 5))s" >&2; \
        sleep $((attempt * 5)); \
    done

# Add AWS RDS CA certificates to the system trust store so tls=true works in DSNs
ADD https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem \
    /usr/local/share/ca-certificates/aws-rds-global-bundle.crt
RUN update-ca-certificates

# Create non-root user
RUN adduser -D -u 1000 schemabot
USER schemabot

WORKDIR /app

# Copy binary from builder
COPY --from=builder /schemabot /app/schemabot

# Copy configs into the image (config.yaml is gitignored but present locally;
# CI creates it from config.yaml.example before building).
COPY --chown=schemabot:schemabot deploy/aws/config.yaml /config/aws-example.yaml
COPY --chown=schemabot:schemabot deploy/aws-multi-env/staging/config.yaml /config/aws-multi-env-staging.yaml
COPY --chown=schemabot:schemabot deploy/aws-multi-env/production/config.yaml /config/aws-multi-env-production.yaml

# Default port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

ENTRYPOINT ["/app/schemabot"]
CMD ["serve"]
