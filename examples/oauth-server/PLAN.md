# OAuth Server Implementation Plan

## Overview

Implement a complete OAuth session management server with Session aggregate, OAuth2 flow, token refresh worker, and integration tests using the DDD kit patterns. Follow DDD best practices with proper encapsulation, command/query separation, and domain-driven design principles.

## Architecture

```mermaid
flowchart TD
    Client[Client] -->|1. GET /login?redirect_url=...| LoginEndpoint[/login]
    LoginEndpoint -->|2. Create Session| SessionAgg[Session Aggregate]
    SessionAgg -->|3. Store in Redis| Redis[(Redis)]
    LoginEndpoint -->|4. Redirect with state=nonce| OAuthProvider[OAuth Provider]
    OAuthProvider -->|5. Callback with code| CallbackEndpoint[/callback]
    CallbackEndpoint -->|6. Load Session| Redis
    CallbackEndpoint -->|7. Exchange code for tokens| OAuthProvider
    CallbackEndpoint -->|8. Store tokens| SessionAgg
    SessionAgg -->|9. Save| Redis
    Client -->|10. GET /api| ApiEndpoint[/api]
    ApiEndpoint -->|11. Load Session| Redis
    ApiEndpoint -->|12. Check token expiry| TokenCheck{Token Expiring?}
    TokenCheck -->|Yes| RefreshWorker[Refresh Worker]
    TokenCheck -->|No| ProxyRequest[Proxy to Backend]
    RefreshWorker -->|Queue refresh| RefreshChannel[Buffered Channel]
    RefreshWorker -->|Refresh token| OAuthProvider
    RefreshWorker -->|Update Session| SessionAgg
    SessionAgg -->|Save| Redis
    ProxyRequest --> Backend[Backend API]
```

## DDD Best Practices & Encapsulation

### Session Aggregate Design

**Encapsulation Principles:**
- State fields are private (lowercase) - only accessible through aggregate methods
- Public methods expose only necessary operations (commands)
- State changes only through events (event sourcing pattern)
- No direct state mutation from outside the aggregate
- Business logic encapsulated within command handlers

**Command/Query Separation:**
- Commands: Return `(EventPack, error)` - modify state
- Queries: Return data without side effects - read-only access to state

**Domain Events:**
- Events represent what happened in the domain
- Events are immutable facts
- State is rebuilt by applying events

## Implementation Plan

### 1. Session Aggregate (`session.go`)

Create Session aggregate following DDD patterns with proper encapsulation:

**State Structure** (private fields):
```go
type SessionState struct {
    nonce              string
    redirectURL        string
    accessToken        string
    refreshToken       string
    tokenExpiry        time.Time
    refreshTokenExpiry time.Time
    status             sessionStatus
    refreshTimeout     time.Time
}

type sessionStatus string

const (
    statusPending      sessionStatus = "pending"
    statusAuthenticated sessionStatus = "authenticated"
    statusRefreshing   sessionStatus = "refreshing"
)
```

**Events** (domain events):
- `SessionCreated` - initial event with nonce and redirect URL
- `TokensReceived` - when tokens are received from OAuth provider
- `TokensRefreshed` - when tokens are refreshed
- `RefreshQueued` - when refresh is queued
- `RefreshTimeoutUpdated` - when refresh timeout is updated

**Commands** (public methods that modify state):
- `CreateSession(nonce, redirectURL) (EventPack, error)` - initialize session
- `ReceiveTokens(accessToken, refreshToken, expiry) (EventPack, error)` - store tokens from OAuth callback
- `QueueRefresh() (EventPack, error)` - mark session for refresh
- `UpdateTokens(accessToken, refreshToken, expiry) (EventPack, error)` - update after refresh
- `UpdateRefreshTimeout(timeout) (EventPack, error)` - update refresh retry timeout

**Queries** (public methods that read state):
- `Nonce() string` - get nonce for verification
- `RedirectURL() string` - get redirect URL
- `AccessToken() string` - get access token
- `RefreshToken() string` - get refresh token
- `TokenExpiry() time.Time` - get token expiry
- `Status() sessionStatus` - get current status
- `IsTokenExpiringSoon() bool` - check if token needs refresh (half-life check)
- `ShouldRetryRefresh() bool` - check if refresh timeout exceeded

**Event Application** (private Apply method):
- `Apply(event Event)` - applies events to state, rebuilding state from events
- Handles all event types, panics on unsupported events

**Storage Options**:
- `StorageOptions() []core.StorageOption` - returns `redis.WithNamespace("sessions")`

**Aggregate Structure**:
```go
type Session struct {
    core.Aggregate[SessionState]
}

func NewSession(id core.ID) *Session {
    agg := &Session{}
    // Initialize will be called with SessionCreated event
    return agg
}
```

### 2. Configuration (`config.go`)

Environment-based configuration with validation:

- `OAUTH_CLIENT_ID` - OAuth client ID (required)
- `OAUTH_CLIENT_SECRET` - OAuth client secret (required)
- `OAUTH_AUTH_URL` - OAuth authorization URL (required)
- `OAUTH_TOKEN_URL` - OAuth token URL (required)
- `OAUTH_REDIRECT_URL` - Callback URL (server domain + /callback) (required)
- `BACKEND_URL` - Backend API URL for proxying (required)
- `REDIS_ADDR` - Redis address (default: localhost:6379)
- `SESSION_COOKIE_NAME` - Cookie name (default: "session_id")
- `REFRESH_WORKER_CHANNEL_SIZE` - Buffered channel size (default: 100)
- `REFRESH_TIMEOUT` - Refresh retry timeout (default: 5 minutes)
- `SERVER_PORT` - Server port (default: 8080)

Encapsulation: Config struct with private fields, public getter methods.

### 3. HTTP Server (`server.go`)

Standard library HTTP server with three endpoints:

#### `/login` endpoint:
- Extract `redirect_url` query parameter
- Generate UUID for session ID using `github.com/google/uuid`
- Create Session aggregate with nonce (cryptographically secure)
- Save session to Redis using repository
- Set HTTP-only, secure cookie with session ID
- Generate OAuth authorization URL with nonce as state
- Redirect to OAuth provider

#### `/callback` endpoint:
- Extract session ID from cookie
- Load session from Redis repository
- Extract `code` and `state` (nonce) from query
- Verify nonce matches session state (encapsulated in aggregate)
- Exchange authorization code for tokens using `golang.org/x/oauth2`
- Update session with tokens via command
- Save session to Redis
- Redirect to original `redirect_url`

#### `/api` endpoint:
- Extract session ID from cookie
- Load session from Redis repository
- Check if access token is expiring soon (encapsulated query method)
- If yes, queue refresh request (non-blocking)
- Proxy request to backend with access token in Authorization header
- Return backend response

### 4. Token Refresh Worker (`worker.go`)

Background worker for token refresh with proper error handling:

- **Buffered channel** for refresh requests (size from config)
- **Worker goroutine** that:
  - Receives session IDs from channel
  - Loads session from Redis repository
  - Checks refresh timeout using aggregate query method
  - If timeout exceeded, updates timeout via command and retries
  - Uses refresh token to get new access token via OAuth2
  - Updates session with new tokens via command
  - Saves session to Redis
  - Handles errors gracefully with logging

- **Queue function** that non-blockingly sends session ID to channel
- **Shutdown** graceful shutdown handling

### 5. OAuth2 Integration (`oauth.go`)

Wrapper around `golang.org/x/oauth2`:

- Create `oauth2.Config` from configuration
- Helper functions for:
  - Generating authorization URL with state
  - Exchanging code for tokens
  - Refreshing access token
- Encapsulation: OAuth client struct with private config, public methods

### 6. Repository Setup (`main.go`)

Main application setup:

- Load configuration from environment
- Initialize Redis client
- Create Redis repository factory using `pkg/redis`
- Initialize OAuth2 config
- Create refresh worker
- Setup HTTP handlers
- Start HTTP server and worker
- Graceful shutdown handling

### 7. Integration Tests (`server_test.go`)

Comprehensive integration tests using testcontainers:

**Test Setup:**
- Use `testcontainers-go/modules/redis` for Redis testcontainer
- Follow pattern from `pkg/redis/repository_test.go`
- Setup function creates Redis container, returns client
- Cleanup handled via `t.Cleanup()`

**Mock OAuth Provider**: HTTP server simulating OAuth provider
- `/authorize` endpoint returning redirect with code
- `/token` endpoint returning tokens
- `/token` endpoint for refresh

**Mock Backend**: HTTP server simulating proxied backend
- Validates Authorization header
- Returns test response

**Test Scenarios**:
1. Complete OAuth flow: `/login` → OAuth → `/callback` → redirect
2. Token proxying: `/api` with valid token
3. Token refresh: token expiring → refresh queued → tokens updated
4. Refresh timeout: timeout exceeded → retry refresh
5. Invalid session: missing/invalid cookie handling
6. Invalid nonce: nonce mismatch in callback
7. Concurrent refresh: multiple refresh requests
8. Session expiration: expired session handling

**Test Structure:**
- Use table-driven tests where appropriate
- Each test sets up its own Redis container
- Proper cleanup and isolation

## File Structure

```
examples/oauth-server/
├── main.go              # Application entry point
├── config.go            # Configuration from environment (encapsulated)
├── session.go           # Session aggregate (DDD with encapsulation)
├── server.go            # HTTP server and handlers
├── worker.go            # Token refresh worker
├── oauth.go             # OAuth2 integration (encapsulated)
├── server_test.go       # Integration tests with testcontainers
├── go.mod               # Dependencies
└── requirements.txt     # Requirements (existing)
```

## Dependencies

Add to `go.mod`:
- `golang.org/x/oauth2` - OAuth2 client
- `github.com/aqaliarept/go-ddd-kit/pkg/redis` - Redis repository (via workspace)
- `github.com/google/uuid` - UUID generation
- `github.com/redis/go-redis/v9` - Redis client
- `github.com/testcontainers/testcontainers-go` - Testcontainers
- `github.com/testcontainers/testcontainers-go/modules/redis` - Redis testcontainer

## Key Implementation Details

### Encapsulation Best Practices

1. **Session Aggregate**:
   - All state fields private
   - Public commands for state changes
   - Public queries for read access
   - No direct state access from outside

2. **Configuration**:
   - Private config struct
   - Public getter methods
   - Validation in constructor

3. **OAuth Client**:
   - Private oauth2.Config
   - Public methods for operations

4. **Error Handling**:
   - Domain errors for business logic failures
   - Proper error wrapping
   - No panics in business logic

### DDD Patterns

1. **Aggregate Root**: Session is the aggregate root
2. **Event Sourcing**: State rebuilt from events
3. **Command/Query Separation**: Clear distinction
4. **Repository Pattern**: Redis repository for persistence
5. **Domain Events**: Events represent domain facts

### Testing with Testcontainers

1. **Setup**: Use `SetupRedisTestContainer` pattern from `pkg/redis/repository_test.go`
2. **Isolation**: Each test gets fresh Redis instance
3. **Cleanup**: Automatic cleanup via `t.Cleanup()`
4. **Realistic**: Tests against real Redis, not mocks

### Additional Details

1. **Session Cookie**: HTTP-only, secure (if HTTPS), SameSite=Lax
2. **Nonce Generation**: `crypto/rand` for secure random strings
3. **Token Expiry Check**: `time.Until(expiry) < expiry.Sub(now)/2` (half-life)
4. **Refresh Queue**: Non-blocking send with `select`/`default`
5. **Context Management**: Use request context throughout
6. **Redis TTL**: Set appropriate TTL for sessions based on token expiry
7. **Logging**: Structured logging for operations

## Implementation Todos

1. Create Session aggregate with proper encapsulation (private state, public commands/queries)
2. Implement configuration loading with validation
3. Create OAuth2 integration wrapper
4. Implement HTTP handlers following DDD patterns
5. Implement token refresh worker with buffered channel
6. Wire up all components in main.go
7. Create integration tests using testcontainers for Redis
8. Update go.mod with required dependencies

