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
FROM golang:1.26-alpine AS builder

RUN apk add --no-cache git ca-certificates

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
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata

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

# Copy AWS config (for App Runner deployments where volumes can't be mounted)
COPY --chown=schemabot:schemabot deploy/aws/config.yaml /config/aws-example.yaml

# Default port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

ENTRYPOINT ["/app/schemabot"]
CMD ["serve"]
