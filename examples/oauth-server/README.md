# OAuth Server Example

This example demonstrates how to build an OAuth2 authorization server using the go-ddd-kit framework. It showcases key DDD concepts including aggregates, domain events, and repository patterns.

## Overview

The OAuth server implements a complete OAuth2 authorization code flow with automatic token refresh. It acts as a reverse proxy that handles authentication and forwards authenticated requests to a backend service.

**Key takeaways:**

1. **Aggregates encapsulate business logic, follow *Tell, Don't Ask***: The `Session` aggregate manages the entire OAuth session lifecycle through commands and events. State changes are driven by events applied via the `Apply` method.

2. **Domain events enable decoupling**: The `RefreshQueued` event decouples the API handler from the refresh worker, allowing asynchronous processing.

3. **Value objects ensure correctness**: Type-safe value objects prevent invalid data from entering the domain and make the code more expressive.

4. **Repository abstraction**: The framework's repository pattern allows switching storage backends (Redis, MongoDB, PostgreSQL) without changing domain code.

5. **Concurrent scopes manage transactions and retries**: Always use repositories through `ConcurrentScope.Run()`. The scope handles transactions, retries on transient errors, and tracks changes automatically.

6. **Initialization pattern**: Aggregates must call `Initialize(id, initialEvent)` in their constructor to set up the initial state with the first domain event.

This example demonstrates how go-ddd-kit enables building maintainable, testable applications following Domain-Driven Design principles.

## Architecture

> **Note**: This example intentionally does not follow Hexagonal/Onion/Clean architecture layout. The code is organized in a simplified structure to focus on demonstrating the go-ddd-kit framework concepts rather than architectural patterns. In production applications, you would typically separate concerns into distinct layers (domain, application, infrastructure/adapters) with proper dependency inversion.

### Components

1. **Domain layer** (`domain/`)
   - `Session` aggregate representing an OAuth session
   The `Session` aggregate exposes the following commands:
        - `CompleteAuthorizationCodeFlow(expectedNonce, accessToken, refreshToken, tokenExpiry, sessionExpiration, now)`: Completes the OAuth authorization code flow by validating the nonce and storing the received tokens. Raises `TokensReceived` event and transitions session to authenticated state.

        - `RefreshTokens(accessToken, refreshToken, tokenExpiry, sessionExpiration, now)`: Updates the session with new tokens after a refresh operation. Raises `TokensReceived` event.

        - `ProcessRequest(now)`: Processes an API request by returning the current access token. Automatically raises `RefreshQueued` event if token refresh is needed based on timing. Returns the access token and any events raised.
   - Value objects: `Nonce`, `AccessToken`, `RefreshToken`, `TokenExpiry`, etc.
   - Domain events: `SessionCreated`, `TokensReceived`, `RefreshQueued`

2. **Application Layer** (`server.go`, `worker.go`)
   - HTTP server handling OAuth flow endpoints
   - Background worker for token refresh
   - Reverse proxy to backend service

3. **Adapters Layer**
   - Redis repository for session persistence
   - OAuth2 client integration

## Key Framework Concepts Demonstrated

### 1. Aggregates

Aggregates are the core building blocks in go-ddd-kit. To implement an aggregate, you must:

1. Create a type and embed `core.Aggregate[State]`

```go
type Session struct {
    core.Aggregate[SessionState]
}

func (s *Session) StorageOptions() []core.StorageOption {
	return []core.StorageOption{redis.WithNamespace("sessions")}
}
```
2. Implement **Storage Options**: aggregates specify storage configuration via `StorageOptions()` method, such as namespace for Redis or collection/table names for MongoDB/PostgreSQL.

In the example storage option defines that sessions will be stored in Redis with keys preffixed by `sessions:`

#### Aggregate state 
The aggregate state is a struct, containing runtime aggregate data. By default state stuct is serialized into storage.

**State with Apply Method**: 

It must implement the `core.EventApplier` interface by providing an `Apply(event Event)` method. This method handles all domain events and mutates the state accordingly.

```go
type SessionState struct {
    // ... fields
}

func (s *SessionState) Apply(event core.Event) {
    switch e := event.(type) {
    case SessionCreated:
        // update state from event
    case TokensReceived:
        // update state from event
    case core.Tombstone:
        // Special type of event, risen when aggregate is removed
    default:
        core.PanicUnsupportedEvent(event)
    }
}
```

**IMPORTANT**:
State should be mutated only via events propagation: events not only an abstraction, but also used for change tracking. Repository will skip aggregate saving if there are no events.

**Domain Events**: Events are plain structs that represent state changes. Each event should correspond to a meaningful business occurrence.

#### Constructor ####

It's good practice to create a dedicated constructor function for aggregates. `Initialize` function should be called to setup initial state via event propagation.

**Note**: Sometimes it make sence to return events from the constructuror func for further analysis

```go
func NewSession(...) *Session {
    agg := &Session{}
    agg.Initialize(sessionID, SessionCreated{...})
    return agg
}
```

#### Commands ####

Business logic is encapsulated in command methods that use `ProcessCommand`. Commands receive the current state and an `EventRiser` to raise events.

**Note**: Unlike many other DDD frameworks `go-ddd-kit` doesn't enforce usage of command handlers pattern, but if needed this pattern could be easily implemented with simple type `switch` atop of the famework `ProcesssCommand`

`ProcessCommand` handle all of the framework's internal logic, event propagation and state mutation.

`state` should be used only for queries. Events are propagated via `core.EventRiser` and mutate `state`

**IMPORTANT** If `ProcessCommand` lambda returns an error, then aggregate is marked as corrupted and can't be used anymore.

```go
func (s *Session) CompleteAuthorizationCodeFlow(...) (core.EventPack, error) {
    return s.ProcessCommand(func(state *SessionState, er core.EventRiser) error {
        // validate business rules
        // raise events via er.Raise(event)
        return nil
    })
}
```

It's good practice to return events generated by the command. Application layer could use such events for handling specific policies.

### 2. Concurrent Scope

**IMPORTANT**: Repositories should **never** be used directly. They must **only** be accessed through `ConcurrentScope`. Direct repository usage bypasses essential features like automatic retries, transaction management, and change tracking.

**Configuring Concurrent Scope**: Follow these steps to set up and configure a concurrent scope:

1. **Create Repository Factory**: First, create a repository factory from your storage connection:
   ```go
   repoFactory := redispkg.NewRepositoryFactory(redisClient)
   // or for MongoDB: mongopkg.NewRepositoryFactory(database)
   // or for PostgreSQL: postgrespkg.NewRepositoryFactory(pool)
   ```

2. **Create Scope with Default Options**: Create a scope with default retry behavior:
   ```go
   scope := core.NewConcurrentScope(repoFactory)
   ```

3. **Configure Rollback Timeout (Optional)**: Set custom timeout for transaction rollback:
   ```go
   scope := core.NewConcurrentScope(repoFactory,
       core.WithRollbackTimeout(10*time.Second),
   )
   ```

4. **Use Scope for All Repository Operations**: Always use `scope.Run()` for any repository operations:
   ```go
   _, err := scope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
       // Use repo here - never use repository factory directly
       return repo.Load(ctx, id, aggregate)
   })
   ```

The `ConcurrentScope` provides essential features:

- **Automatic Retries**: Automatically retries operations on `ErrTransient` or `ErrConcurrentModification` errors
- **Transaction Management**: Automatically handles transactions for repositories that implement `Transactional` interface
- **Change Tracking**: Tracks all aggregates modified during a scope execution
- **Error Handling**: Properly rolls back transactions on errors with configurable timeout

**Usage Pattern**: All repository operations must be wrapped in `scope.Run()`:

```go
scope := core.NewConcurrentScope(repoFactory)

_, err := scope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
    session := &domain.Session{}
    err := repo.Load(ctx, sessionID, session)
    if err != nil {
        return err
    }
    
    // process commands, modify aggregate
    events, err := session.ProcessRequest(now)
    if err != nil {
        return err
    }
    
    return repo.Save(ctx, session, redis.WithExpiration(...))
})
```

**IMPORTANT**: Always reuse the `ctx` parameter from the lambda function for all operations inside the `Run` block. The context is enriched with scope-specific data (such as transaction references for transactional repositories). Using the original context instead of the lambda's context will cause operations to execute outside the scope's transaction, leading to incorrect behavior.

The scope automatically:
- Creates a new repository instance for each execution
- Begins a transaction if the repository supports it
- Retries on transient or concurrent modification errors
- Commits on success or rolls back on error
- Tracks all changes made during execution

**Retry Configuration**: You can configure retry behavior when creating the scope:

```go
scope := core.NewConcurrentScope(factory,
    core.WithRetryOptions(
        retry.Attempts(5),
        retry.Delay(100*time.Millisecond),
    ),
)
```

### 3. Repository Pattern

The framework provides repository abstractions that support multiple storage backends (Redis, MongoDB, PostgreSQL). Repositories are created via factory functions specific to each backend.

**Repository Factory**: Create a factory from your storage connection:

```go
repoFactory := redispkg.NewRepositoryFactory(redisClient)
// or for MongoDB: mongopkg.NewRepositoryFactory(database)
// or for PostgreSQL: postgrespkg.NewRepositoryFactory(pool)
```

**Load and Save Operations**: Repositories provide two main operations for aggregate persistence:

- **`Load(ctx, id, aggregate)`**: Loads an aggregate by its ID into the provided aggregate instance. The aggregate must implement the `Restorer` interface. Returns `ErrAggregateNotFound` if the aggregate doesn't exist.

- **`Save(ctx, aggregate, options...)`**: Saves an aggregate to storage. The aggregate must implement the `Storer` interface. Both `Load` and `Save` accept repository-specific options as variadic parameters.

**Repository-Specific Options**: Each repository implementation provides its own option types. For example, Redis repository supports expiration options:

```go
// Save with Redis expiration
err := repo.Save(ctx, session, redis.WithExpiration(24*time.Hour))
```

## OAuth Flow

### 1. Login (`/login`)

- Creates a new `Session` aggregate with a nonce via `NewSession()`
- Saves session using `scope.Run()` with repository
- Sets session cookie
- Redirects to OAuth provider

### 2. Callback (`/callback`)

- Loads session from repository within `scope.Run()`
- Exchanges authorization code for tokens
- Updates session via `CompleteAuthorizationCodeFlow()` command
- Saves session and redirects to original URL

### 3. API Proxy (`/api`)

- Loads session within `scope.Run()`
- Processes request via `ProcessRequest()` command
- Command automatically raises `RefreshQueued` event if token refresh is needed
- Extracts events from scope execution and queues refresh if needed
- Adds access token to request header and proxies to backend

## Token Refresh Worker

The background worker processes token refresh requests asynchronously. It uses `scope.Run()` to:
- Load the session
- Refresh tokens via OAuth provider
- Update session via `RefreshTokens()` command
- Save the updated session

This demonstrates how domain events (like `RefreshQueued`) enable decoupling between different parts of the application.

## Configuration

The server is configured via environment variables:

- `OAUTH_CLIENT_ID`: OAuth client ID
- `OAUTH_CLIENT_SECRET`: OAuth client secret
- `OAUTH_AUTH_URL`: OAuth authorization URL
- `OAUTH_TOKEN_URL`: OAuth token exchange URL
- `OAUTH_REDIRECT_URL`: OAuth redirect callback URL
- `BACKEND_URL`: Backend service URL to proxy requests to
- `REDIS_ADDR`: Redis address (default: `localhost:6379`)
- `SESSION_COOKIE_NAME`: Session cookie name (default: `session_id`)
- `SESSION_EXPIRATION`: Session expiration duration (default: `24h`)
- `REFRESH_WORKER_CHANNEL_SIZE`: Refresh worker queue size (default: `100`)
- `REFRESH_TIMEOUT`: Refresh operation timeout (default: `5m`)
- `SERVER_PORT`: HTTP server port (default: `8080`)

## Running the Example

1. Start Redis:
```bash
redis-server
```

2. Set environment variables:
```bash
export OAUTH_CLIENT_ID=your_client_id
export OAUTH_CLIENT_SECRET=your_client_secret
export OAUTH_AUTH_URL=https://oauth-provider.com/auth
export OAUTH_TOKEN_URL=https://oauth-provider.com/token
export OAUTH_REDIRECT_URL=http://localhost:8080/callback
export BACKEND_URL=http://localhost:3000
```

3. Run the server:
```bash
go run main.go config.go server.go worker.go oauth.go
```

