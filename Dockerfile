FROM node:24 AS frontend-builder
WORKDIR /src
COPY frontend/package.json frontend/pnpm-lock.yaml frontend/pnpm-workspace.yaml ./
RUN corepack enable && corepack prepare pnpm@10.30.1 --activate
RUN pnpm install --frozen-lockfile
COPY frontend/ ./
ARG VITE_API_URL=http://localhost:8888/api/v1
ENV VITE_API_URL=${VITE_API_URL}
RUN pnpm run build

FROM golang:1.26 AS builder
WORKDIR /src
COPY go.sum go.mod ./
RUN go mod download
COPY . .
COPY --from=frontend-builder /src/build ./frontend/build
# Version info wired into cmd/version.go (same ldflags as .goreleaser.yaml)
ARG RELEASE=dev
ARG GIT_COMMIT=none
RUN go build -ldflags "-s -w -X github.com/ethpandaops/cbt/cmd.Release=${RELEASE} -X github.com/ethpandaops/cbt/cmd.GitCommit=${GIT_COMMIT}" -o /bin/app .

FROM ubuntu:24.04
RUN apt-get update && apt-get -y upgrade && apt-get install -y --no-install-recommends \
  libssl-dev \
  ca-certificates \
  python3 \
  wget \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
RUN useradd --system --uid 10001 --create-home --shell /usr/sbin/nologin cbt
WORKDIR /app
RUN chown cbt:cbt /app
COPY --from=builder /bin/app /cbt
USER cbt
ENTRYPOINT ["/cbt"]
