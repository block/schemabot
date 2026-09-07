FROM golang:1.27-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o /schemabot ./pkg/cmd

FROM alpine:3.24
# Retried with backoff; deploy/Dockerfile explains why.
RUN for attempt in 1 2 3 4 5; do \
        apk add --no-cache ca-certificates && break; \
        echo "apk add ca-certificates failed (attempt ${attempt}/5)" >&2; \
        [ "$attempt" -lt 5 ] || exit 1; \
        echo "retrying in $((attempt * 5))s" >&2; \
        sleep $((attempt * 5)); \
    done
COPY --from=builder /schemabot /schemabot
ENTRYPOINT ["/schemabot"]
CMD ["serve"]
