FROM node:22 AS frontend-builder
WORKDIR /src
COPY frontend/package.json frontend/pnpm-lock.yaml frontend/pnpm-workspace.yaml ./
RUN corepack enable && corepack prepare pnpm@latest --activate
RUN pnpm install --frozen-lockfile
COPY frontend/ ./
RUN pnpm run build

FROM golang:1.25 AS builder
WORKDIR /src
COPY go.sum go.mod ./
RUN go mod download
COPY . .
COPY --from=frontend-builder /src/build ./frontend/build
RUN go build -o /bin/app .

FROM ubuntu:latest
RUN apt-get update && apt-get -y upgrade && apt-get install -y --no-install-recommends \
  libssl-dev \
  ca-certificates \
  python3 \
  wget \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /bin/app /cbt
ENTRYPOINT ["/cbt"]
