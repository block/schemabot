# Etre entity-registry server for the k8s e2e suite.
#
# Etre publishes no image, so the server is built from source at the same module
# and version as the client dependency in go.mod (github.com/square/etre). The
# server config is mounted from a ConfigMap at runtime rather than baked in, so
# the same image works for any datasource.
#
# The Alpine CDN intermittently answers "temporary error (try again later)" and a
# single failed fetch takes the whole e2e job with it, so each apk add retries
# with backoff instead of failing on the first miss.
FROM golang:1.26.3-alpine AS builder
WORKDIR /app
RUN for attempt in 1 2 3 4 5; do \
        apk add --no-cache git && break; \
        [ "$attempt" -lt 5 ] || exit 1; \
        echo "apk add failed (attempt ${attempt}/5); retrying" >&2; \
        sleep $((attempt * 5)); \
    done
RUN git clone --depth 1 --branch v0.15.0 https://github.com/square/etre.git .
RUN go build -o etre ./bin/etre

FROM alpine:3.19
RUN for attempt in 1 2 3 4 5; do \
        apk add --no-cache ca-certificates && break; \
        [ "$attempt" -lt 5 ] || exit 1; \
        echo "apk add failed (attempt ${attempt}/5); retrying" >&2; \
        sleep $((attempt * 5)); \
    done
WORKDIR /app
COPY --from=builder /app/etre .
EXPOSE 8080
CMD ["./etre", "-config", "/etc/etre/config.yaml"]
