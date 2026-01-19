module github.com/aqaliarept/go-ddd-kit/pkg/sqlite

replace github.com/aqaliarept/go-ddd-kit/pkg/core => ../core

go 1.25.5

require (
	github.com/aqaliarept/go-ddd-kit/pkg/core v0.0.0
	modernc.org/sqlite v1.34.0
)
