## `go-ddd-kit`: toolkit for Domain-Driven Design in Go

[![codecov](https://codecov.io/gh/aqaliarept/go-ddd-kit/branch/main/graph/badge.svg)](https://codecov.io/gh/aqaliarept/go-ddd-kit)
[![Go Report Card](https://goreportcard.com/badge/github.com/aqaliarept/go-ddd-kit/pkg/core)](https://goreportcard.com/report/github.com/aqaliarept/go-ddd-kit/pkg/core)
[![Go Report Card](https://goreportcard.com/badge/github.com/aqaliarept/go-ddd-kit/pkg/mongo)](https://goreportcard.com/report/github.com/aqaliarept/go-ddd-kit/pkg/mongo)
[![Go Report Card](https://goreportcard.com/badge/github.com/aqaliarept/go-ddd-kit/pkg/postgres)](https://goreportcard.com/report/github.com/aqaliarept/go-ddd-kit/pkg/postgres)
[![Go Report Card](https://goreportcard.com/badge/github.com/aqaliarept/go-ddd-kit/pkg/redis)](https://goreportcard.com/report/github.com/aqaliarept/go-ddd-kit/pkg/redis)

`go-ddd-kit` making it easier to build maintainable applications with aggregates, events, and repositories.

## Goals

After building several production applications in Go using Domain-Driven Design, the same problems were encountered repeatedly: how to structure aggregates, handle events, manage persistence, and deal with concurrency. When exploring the frameworks available in the Go ecosystem, many were found to focus on different aspects—some emphasized architectural patterns, others focused on specific storage backends, but a simple, cohesive approach to the core DDD concerns was not found.

The frameworks explored for Go each had their strengths, but setting up aggregates with proper persistence and transaction handling was often found to require more configuration and boilerplate than desired. Some frameworks introduced layers of abstraction that, while powerful, added complexity for common use cases.

`go-ddd-kit` was created to address these gaps. It's built from the ground up with production needs in mind, focusing on what actually matters when building real applications.

### Simple, Not Simplistic

A framework should make common tasks easy, not make simple tasks complicated. `go-ddd-kit` provides the essential building blocks—aggregates, events, and repositories—without forcing you through layers of indirection or requiring extensive configuration. You can get started quickly, but the framework scales with your application's complexity. It doesn't try to solve every possible problem upfront; instead, it gives you solid foundations and gets out of your way.

### Domain Logic That Fits Naturally

One of the biggest challenges in DDD is keeping your domain layer clean while still being able to integrate it with the rest of your application. Your aggregates shouldn't need to know about HTTP, gRPC, or message queues, but you also shouldn't have to write mountains of adapter code just to use them. `go-ddd-kit` is designed so that your domain logic stays independent and testable, while still being convenient to use from HTTP handlers, background workers, or any other part of your system. The framework bridges the gap between your domain model and your application infrastructure without forcing compromises.

### Transactions and Concurrency Done Right

Production applications need to handle concurrency safely. You need transactions that work correctly, retries when things go wrong, and protection against concurrent modifications. In many frameworks, these concerns are left to the application layer or must be implemented manually, which can be error-prone and time-consuming. `go-ddd-kit` puts transaction management, optimistic concurrency control, and automatic retries front and center. The `ConcurrentScope` handles the complexity of managing transactions across multiple aggregates, retrying on transient errors, and tracking changes—so you can focus on your business logic instead of infrastructure concerns.

### Migration as a Core Feature, Not an Afterthought

Data schemas evolve. Fields get renamed, structures change, and you need to handle old data gracefully. In many frameworks, migration is treated as something done separately, often requiring downtime or complex migration scripts. In `go-ddd-kit`, state migration is a first-class feature. Your aggregates can handle multiple schema versions, automatically migrating old data when it's loaded. This means you can deploy schema changes without downtime, and your application gracefully handles data in any version it encounters. The framework provides comprehensive support for runtime migrations, so schema evolution becomes a natural part of your development process rather than a special operation.

## Installation

```bash
go get github.com/aqaliarept/go-ddd-kit/pkg/core
go get github.com/aqaliarept/go-ddd-kit/pkg/redis
go get github.com/aqaliarept/go-ddd-kit/pkg/mongo
go get github.com/aqaliarept/go-ddd-kit/pkg/postgres
```

## At a Glance

### Define an Aggregate

```go
type SessionState struct {
    Status      SessionStatus
    AccessToken string
}

func (s *SessionState) Apply(event core.Event) {
    switch e := event.(type) {
    case SessionCreated:
        s.Status = StatusPending
    case TokensReceived:
        s.AccessToken = e.AccessToken
        s.Status = StatusAuthenticated
    default:
        core.PanicUnsupportedEvent(event)
    }
}

type Session struct {
    core.Aggregate[SessionState]
}

func NewSession(id core.ID) *Session {
    agg := &Session{}
    agg.Initialize(id, SessionCreated{})
    return agg
}
```

### Use Commands

```go
func (s *Session) CompleteAuthorizationCodeFlow(token string) (core.EventPack, error) {
    return s.ProcessCommand(func(state *SessionState, er core.EventRiser) error {
        er.Raise(TokensReceived{AccessToken: token})
        return nil
    })
}
```

### Work with Repositories

```go
repoFactory := redis.NewRepositoryFactory(redisClient)
scope := core.NewConcurrentScope(repoFactory)

_, err := scope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
    session := &Session{}
    err := repo.Load(ctx, sessionID, session)
    if err != nil {
        return err
    }
    
    events, err := session.CompleteAuthorizationCodeFlow(token)
    if err != nil {
        return err
    }
    
    return repo.Save(ctx, session, redis.WithExpiration(24*time.Hour))
})
```

## Features

- **Aggregates**: Encapsulate business logic through commands and manage state through events
- **Event-Driven State**: State changes are driven by domain events, ensuring consistency
- **Multiple Storage Backends**: Support for Redis, MongoDB, and PostgreSQL
- **Concurrent Scopes**: Automatic transaction management, retries, and change tracking
- **Optimistic Concurrency**: Built-in versioning for concurrent modification detection
- **State Migration**: Schema versioning support with automatic migration when loading aggregates
- **Type-Safe**: Leverages Go generics for compile-time safety

## Core Concepts

### Aggregates

Aggregates are the core building blocks that encapsulate business logic. They use commands to process operations and events to mutate state.

### Events

Domain events represent meaningful business occurrences. State changes happen exclusively through event application.

### Repositories

Repositories abstract persistence, supporting multiple storage backends. Always use repositories through `ConcurrentScope` for automatic transaction management and retries.

### Concurrent Scope

`ConcurrentScope` provides:
- Automatic retries on transient errors
- Transaction management for transactional repositories
- Change tracking for all modified aggregates
- Proper rollback handling on errors

### State Migration

The framework supports schema versioning and migration through the `StateRestorer` and `StateStorer` interfaces. States can implement these interfaces to handle loading from different schema versions and migrate them to the current schema.

```go
type SessionStateV2 struct {
    AccessToken string
}

func (s *SessionStateV2) Restore(schemaVersion core.SchemaVersion, restoreFunc func(state core.StatePtr) error) error {
    if schemaVersion == 1 {
        var v1State SessionStateV1
        if err := restoreFunc(&v1State); err != nil {
            return err
        }
        s.AccessToken = v1State.Token
        return nil
    }
    return restoreFunc(s)
}

func (s *SessionStateV2) Store(storeFunc func(state core.StatePtr, schemaVersion core.SchemaVersion) error) error {
    return storeFunc(s, core.SchemaVersion(2))
}
```

## Examples

See the [OAuth Server example](examples/oauth-server/README.md) for a complete implementation demonstrating aggregates, events, repositories, and concurrent scopes.

## Documentation

For detailed API documentation, visit [pkg.go.dev](https://pkg.go.dev/github.com/aqaliarept/go-ddd-kit/pkg/core).
