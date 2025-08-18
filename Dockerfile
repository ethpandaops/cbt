# Build stage
FROM golang:1.24-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git make

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o cbt main.go

# Final stage
FROM alpine:latest

# Install ca-certificates for HTTPS, Python for exec scripts, and wget for health checks
RUN apk --no-cache add ca-certificates python3 wget

# Create non-root user
RUN addgroup -g 1000 -S cbt && \
    adduser -u 1000 -S cbt -G cbt

# Set working directory
WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/cbt /app/cbt

# Create scripts directory
RUN mkdir -p /app/scripts

# Set ownership
RUN chown -R cbt:cbt /app

# Switch to non-root user
USER cbt

# Expose ports
EXPOSE 9090 8081

# Set entrypoint
ENTRYPOINT ["/app/cbt"]

# Default command (can be overridden)
CMD ["--help"]