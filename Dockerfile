FROM golang:1.26-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o /schemabot ./pkg/cmd

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=builder /schemabot /schemabot
ENTRYPOINT ["/schemabot"]
CMD ["serve"]
