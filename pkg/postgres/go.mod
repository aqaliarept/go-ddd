module github.com/aqaliarept/go-ddd-kit/pkg/postgres

replace github.com/aqaliarept/go-ddd-kit/pkg/core => ../core

replace github.com/aqaliarept/go-ddd-kit/internal/test => ../../internal/test

go 1.25.5

require (
	github.com/aqaliarept/go-ddd-kit/pkg/core v0.0.0
	github.com/jackc/pgconn v1.14.3
	github.com/jackc/pgx/v4 v4.18.3
)

require (
	github.com/avast/retry-go/v4 v4.7.0 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgproto3/v2 v2.3.3 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/pgtype v1.14.0 // indirect
	github.com/jackc/puddle v1.3.0 // indirect
	github.com/lib/pq v1.10.9 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	golang.org/x/crypto v0.44.0 // indirect
	golang.org/x/text v0.32.0 // indirect
)
