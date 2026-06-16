# Etre entity-registry server for the k8s e2e suite.
#
# Etre publishes no image, so the server is built from source at the same module
# and version as the client dependency in go.mod (github.com/square/etre). The
# server config is mounted from a ConfigMap at runtime rather than baked in, so
# the same image works for any datasource.
FROM golang:1.26.3-alpine AS builder
WORKDIR /app
RUN apk add --no-cache git
RUN git clone --depth 1 --branch v0.15.0 https://github.com/square/etre.git .
RUN go build -o etre ./bin/etre

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=builder /app/etre .
EXPOSE 8080
CMD ["./etre", "-config", "/etc/etre/config.yaml"]
