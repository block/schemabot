# Etre entity-registry server for the k8s e2e suite.
#
# Etre publishes no image, so the server is built from source at the same module
# and version as the client dependency in go.mod (github.com/square/etre). The
# server config is mounted from a ConfigMap at runtime rather than baked in, so
# the same image works for any datasource.
#
# Each apk add is retried with backoff; deploy/Dockerfile explains why.
FROM golang:1.27.1-alpine AS builder
WORKDIR /app
RUN for attempt in 1 2 3 4 5; do \
        apk add --no-cache git && break; \
        echo "apk add git failed (attempt ${attempt}/5)" >&2; \
        [ "$attempt" -lt 5 ] || exit 1; \
        echo "retrying in $((attempt * 5))s" >&2; \
        sleep $((attempt * 5)); \
    done
RUN git clone --depth 1 --branch v0.15.0 https://github.com/square/etre.git .
RUN go build -o etre ./bin/etre

FROM alpine:3.24
RUN for attempt in 1 2 3 4 5; do \
        apk add --no-cache ca-certificates && break; \
        echo "apk add ca-certificates failed (attempt ${attempt}/5)" >&2; \
        [ "$attempt" -lt 5 ] || exit 1; \
        echo "retrying in $((attempt * 5))s" >&2; \
        sleep $((attempt * 5)); \
    done
WORKDIR /app
COPY --from=builder /app/etre .
EXPOSE 8080
CMD ["./etre", "-config", "/etc/etre/config.yaml"]
