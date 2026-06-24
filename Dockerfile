FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o server .

FROM alpine:3.21
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /app/server .

# Run as an unprivileged user (SOC 2 CC6.8: containers must not run as root).
RUN addgroup -S appuser && adduser -S -G appuser -u 1001 appuser \
    && chown -R appuser:appuser /app
USER appuser

EXPOSE 8080
CMD ["./server"]
