// Package testutil provides test utilities for CBT, including:
//   - ClickHouse container helpers for integration tests (clickhouse.go)
//   - Redis container helpers for integration tests (redis.go)
//   - Miniredis helpers for unit tests (miniredis.go)
//
// Integration test utilities require Docker and are gated behind the "integration"
// build tag. To run integration tests:
//
//	go test -tags=integration ./...
//
// Unit test helpers (miniredis) do not require Docker and work with regular tests.
package testutil
