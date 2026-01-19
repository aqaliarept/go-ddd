#!/bin/bash
set -euo pipefail

echo "Running tests with coverage..."

rm -f coverage.out coverage_mongo.out coverage_postgres.out coverage_redis.out coverage_core.out

go test ./internal/test/mongo -race -coverprofile=coverage_mongo.out -covermode=atomic \
	-coverpkg=github.com/aqaliarept/go-ddd-kit/pkg/core/...,github.com/aqaliarept/go-ddd-kit/pkg/mongo/...

go test ./internal/test/postgres -race -coverprofile=coverage_postgres.out -covermode=atomic \
	-coverpkg=github.com/aqaliarept/go-ddd-kit/pkg/core/...,github.com/aqaliarept/go-ddd-kit/pkg/postgres/...

go test ./internal/test/redis -race -coverprofile=coverage_redis.out -covermode=atomic \
	-coverpkg=github.com/aqaliarept/go-ddd-kit/pkg/core/...,github.com/aqaliarept/go-ddd-kit/pkg/redis/...

go test ./internal/test/sqlite -race -coverprofile=coverage_sqlite.out -covermode=atomic \
	-coverpkg=github.com/aqaliarept/go-ddd-kit/pkg/core/...,github.com/aqaliarept/go-ddd-kit/pkg/sqlite/...

go test ./pkg/core -race -coverprofile=coverage_core.out -covermode=atomic \
	-coverpkg=github.com/aqaliarept/go-ddd-kit/pkg/core/...

echo "Merging coverage profiles..."
echo "mode: atomic" > coverage.out
grep -h -v "^mode:" coverage_mongo.out coverage_postgres.out coverage_redis.out coverage_sqlite.out coverage_core.out >> coverage.out 2>/dev/null || true

go tool cover -html=coverage.out -o coverage.html

echo ""
echo "Coverage report generated: coverage.html"
echo ""
