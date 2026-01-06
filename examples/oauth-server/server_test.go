package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	core "github.com/aqaliarept/go-ddd-kit/pkg/core"
	redispkg "github.com/aqaliarept/go-ddd-kit/pkg/redis"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/aqaliarept/go-ddd-kit/examples/oauth-server/domain"
)

type redisTestContainer struct {
	container testcontainers.Container
	client    *redis.Client
}

func (r *redisTestContainer) Client() *redis.Client {
	return r.client
}

func (r *redisTestContainer) Cleanup() {
	ctx := context.Background()
	if r.client != nil {
		//nolint:errcheck
		_ = r.client.Close()
	}
	if r.container != nil {
		//nolint:errcheck
		_ = r.container.Terminate(ctx)
	}
}

func setupRedisTestContainer(t *testing.T) *redisTestContainer {
	ctx := context.Background()

	redisContainer, err := rediscontainer.Run(ctx,
		"redis:7-alpine",
		testcontainers.WithWaitStrategy(
			wait.ForLog("Ready to accept connections").
				WithOccurrence(1).
				WithStartupTimeout(60*time.Second).
				WithPollInterval(1*time.Second),
		),
	)
	require.NoError(t, err)

	connStr, err := redisContainer.ConnectionString(ctx)
	require.NoError(t, err)

	opt, err := redis.ParseURL(connStr)
	require.NoError(t, err)

	client := redis.NewClient(opt)

	err = client.Ping(ctx).Err()
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Logf("failed to close Redis client: %v", err)
		}
		if err := redisContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate Redis container: %v", err)
		}
	})

	return &redisTestContainer{
		container: redisContainer,
		client:    client,
	}
}

type mockOAuthProvider struct {
	server *httptest.Server
	code   string
}

func newMockOAuthProvider(t *testing.T) *mockOAuthProvider {
	mock := &mockOAuthProvider{
		code: uuid.New().String(),
	}

	mock.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/authorize":
			state := r.URL.Query().Get("state")
			redirectURI := r.URL.Query().Get("redirect_uri")
			redirectURL := fmt.Sprintf("%s?code=%s&state=%s", redirectURI, mock.code, state)
			http.Redirect(w, r, redirectURL, http.StatusFound)
		case "/token":
			var req struct {
				GrantType    string `json:"grant_type"`
				Code         string `json:"code"`
				RefreshToken string `json:"refresh_token"`
			}
			if r.Header.Get("Content-Type") == "application/json" {
				//nolint:errcheck
				_ = json.NewDecoder(r.Body).Decode(&req)
			} else {
				req.GrantType = r.FormValue("grant_type")
				req.Code = r.FormValue("code")
				req.RefreshToken = r.FormValue("refresh_token")
			}

			var resp struct {
				AccessToken  string `json:"access_token"`
				RefreshToken string `json:"refresh_token"`
				ExpiresIn    int    `json:"expires_in"`
			}

			if req.GrantType == "refresh_token" {
				resp.AccessToken = "refreshed_access_token_" + uuid.New().String()
				resp.RefreshToken = req.RefreshToken
			} else {
				resp.AccessToken = "access_token_" + uuid.New().String()
				resp.RefreshToken = "refresh_token_" + uuid.New().String()
			}
			resp.ExpiresIn = 3600

			w.Header().Set("Content-Type", "application/json")
			//nolint:errcheck
			_ = json.NewEncoder(w).Encode(resp)
		default:
			http.NotFound(w, r)
		}
	}))

	t.Cleanup(func() {
		mock.server.Close()
	})

	return mock
}

type mockBackend struct {
	server *httptest.Server
}

func newMockBackend(t *testing.T) *mockBackend {
	mock := &mockBackend{}

	mock.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" || len(authHeader) < 8 || authHeader[:7] != "Bearer " {
			http.Error(w, "missing or invalid authorization", http.StatusUnauthorized)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		//nolint:errcheck
		_ = json.NewEncoder(w).Encode(map[string]string{
			"message": "success",
			"token":   authHeader[7:],
		})
	}))

	t.Cleanup(func() {
		mock.server.Close()
	})

	return mock
}

func setupTestServer(t *testing.T, redisClient *redis.Client, oauthProvider *mockOAuthProvider, backend *mockBackend) (*Server, core.Repository, *RefreshWorker) {
	repoFactory := redispkg.NewRepositoryFactory(redisClient)
	ctx := context.Background()
	repo := repoFactory.Create(ctx)
	scope := core.NewConcurrentScope(repoFactory)

	cfg := &Config{
		oauthClientID:            "test_client_id",
		oauthClientSecret:        "test_client_secret",
		oauthAuthURL:             oauthProvider.server.URL + "/authorize",
		oauthTokenURL:            oauthProvider.server.URL + "/token",
		oauthRedirectURL:         "http://localhost:8080/callback",
		backendURL:               backend.server.URL,
		redisAddr:                "localhost:6379",
		sessionCookieName:        "session_id",
		refreshWorkerChannelSize: 100,
		refreshTimeout:           5 * time.Minute,
		sessionExpiration:        24 * time.Hour,
		serverPort:               "8080",
	}

	clock := &domain.WallClock{}

	oauth := NewOAuthClient(cfg)
	worker := NewRefreshWorker(scope, oauth, cfg, clock)
	worker.Start()

	server, err := NewServer(scope, oauth, worker, cfg, clock)
	require.NoError(t, err)

	t.Cleanup(func() {
		worker.Shutdown()
	})

	return server, repo, worker
}

func TestCompleteOAuthFlow(t *testing.T) {
	redisContainer := setupRedisTestContainer(t)
	oauthProvider := newMockOAuthProvider(t)
	backend := newMockBackend(t)
	server, repo, _ := setupTestServer(t, redisContainer.Client(), oauthProvider, backend)

	redirectURL := "http://example.com/callback"
	req := httptest.NewRequest("GET", "/login?redirect_url="+url.QueryEscape(redirectURL), nil)
	w := httptest.NewRecorder()

	server.handleLogin(w, req)

	require.Equal(t, http.StatusFound, w.Code)
	location := w.Header().Get("Location")
	require.Contains(t, location, "/authorize")
	require.Contains(t, location, "state=")

	cookies := w.Result().Cookies()
	require.Len(t, cookies, 1)
	sessionCookie := cookies[0]

	authURL, err := url.Parse(location)
	require.NoError(t, err)
	state := authURL.Query().Get("state")
	require.NotEmpty(t, state, "State should not be empty")

	sessionID := core.ID(sessionCookie.Value)
	loadedSession := &domain.Session{}
	err = repo.Load(context.Background(), sessionID, loadedSession)
	require.NoError(t, err, "Session should be loadable after login")
	require.Equal(t, state, loadedSession.Nonce().String(), "Loaded session nonce should match state")

	callbackReq := httptest.NewRequest("GET", fmt.Sprintf("/callback?code=%s&state=%s", oauthProvider.code, state), nil)
	callbackReq.AddCookie(sessionCookie)
	callbackW := httptest.NewRecorder()

	server.handleCallback(callbackW, callbackReq)

	if callbackW.Code != http.StatusFound {
		t.Logf("Callback response body: %s", callbackW.Body.String())
	}
	require.Equal(t, http.StatusFound, callbackW.Code)
	require.Equal(t, redirectURL, callbackW.Header().Get("Location"))
}

func TestTokenProxying(t *testing.T) {
	redisContainer := setupRedisTestContainer(t)
	oauthProvider := newMockOAuthProvider(t)
	backend := newMockBackend(t)
	server, repo, _ := setupTestServer(t, redisContainer.Client(), oauthProvider, backend)

	redirectURL, err := domain.NewRedirectURL("http://example.com")
	require.NoError(t, err)
	sessionExpiration, err := domain.NewSessionExpiration(24 * time.Hour)
	require.NoError(t, err)
	nonce, err := domain.NewNonce("test_nonce")
	require.NoError(t, err)
	session := domain.NewSession(nonce, redirectURL, sessionExpiration)
	sessionID := session.ID()

	ctx := context.Background()
	err = repo.Save(ctx, session)
	require.NoError(t, err)

	loadedSession := &domain.Session{}
	err = repo.Load(ctx, sessionID, loadedSession)
	require.NoError(t, err)

	expectedNonce, err := domain.NewNonce(loadedSession.Nonce().String())
	require.NoError(t, err)
	accessToken, err := domain.NewAccessToken("test_access_token")
	require.NoError(t, err)
	refreshToken, err := domain.NewRefreshToken("test_refresh_token")
	require.NoError(t, err)
	tokenExpiry, err := domain.NewTokenExpiry(domain.NewTimestamp(time.Now().Add(time.Hour)))
	require.NoError(t, err)
	sessionExp, err := domain.NewSessionExpiration(24 * time.Hour)
	require.NoError(t, err)
	now := domain.NewTimestamp(time.Now())
	_, err = loadedSession.CompleteAuthorizationCodeFlow(expectedNonce, accessToken, refreshToken, tokenExpiry, sessionExp, now)
	require.NoError(t, err)

	err = repo.Save(ctx, loadedSession)
	require.NoError(t, err)

	req := httptest.NewRequest("GET", "/api/test", nil)
	req.AddCookie(&http.Cookie{
		Name:  "session_id",
		Value: string(sessionID),
	})
	w := httptest.NewRecorder()

	server.handleAPI(w, req)

	if w.Code != http.StatusOK {
		t.Logf("Response body: %s", w.Body.String())
	}
	require.Equal(t, http.StatusOK, w.Code)

	var response map[string]string
	err = json.NewDecoder(w.Body).Decode(&response)
	require.NoError(t, err)
	require.Equal(t, "success", response["message"])
	require.Equal(t, "test_access_token", response["token"])
}

func TestTokenRefresh(t *testing.T) {
	redisContainer := setupRedisTestContainer(t)
	oauthProvider := newMockOAuthProvider(t)
	backend := newMockBackend(t)
	_, repo, worker := setupTestServer(t, redisContainer.Client(), oauthProvider, backend)

	redirectURL, err := domain.NewRedirectURL("http://example.com")
	require.NoError(t, err)
	sessionExpiration, err := domain.NewSessionExpiration(24 * time.Hour)
	require.NoError(t, err)
	nonce, err := domain.NewNonce("test_nonce")
	require.NoError(t, err)
	session := domain.NewSession(nonce, redirectURL, sessionExpiration)
	sessionID := session.ID()

	ctx := context.Background()
	expiry := time.Now().Add(30 * time.Second)
	accessToken, err := domain.NewAccessToken("test_access_token")
	require.NoError(t, err)
	refreshToken, err := domain.NewRefreshToken("test_refresh_token")
	require.NoError(t, err)
	tokenExpiry, err := domain.NewTokenExpiry(domain.NewTimestamp(expiry))
	require.NoError(t, err)
	sessionExp, err := domain.NewSessionExpiration(24 * time.Hour)
	require.NoError(t, err)
	now := domain.NewTimestamp(time.Now())
	_, err = session.CompleteAuthorizationCodeFlow(nonce, accessToken, refreshToken, tokenExpiry, sessionExp, now)
	require.NoError(t, err)

	err = repo.Save(ctx, session)
	require.NoError(t, err)

	state := session.State()
	worker.Queue(sessionID, string(state.RefreshToken))

	time.Sleep(100 * time.Millisecond)

	err = repo.Load(ctx, sessionID, session)
	require.NoError(t, err)

	state = session.State()
	require.True(t, string(state.AccessToken) != "test_access_token" || state.Status == "refreshing" || state.Status == "renew_ongoing")
}

func TestInvalidSession(t *testing.T) {
	redisContainer := setupRedisTestContainer(t)
	oauthProvider := newMockOAuthProvider(t)
	backend := newMockBackend(t)
	server, _, _ := setupTestServer(t, redisContainer.Client(), oauthProvider, backend)
	_ = server

	req := httptest.NewRequest("GET", "/api/test", nil)
	w := httptest.NewRecorder()

	server.handleAPI(w, req)

	require.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestInvalidNonce(t *testing.T) {
	redisContainer := setupRedisTestContainer(t)
	oauthProvider := newMockOAuthProvider(t)
	backend := newMockBackend(t)
	server, repo, _ := setupTestServer(t, redisContainer.Client(), oauthProvider, backend)

	redirectURL, err := domain.NewRedirectURL("http://example.com")
	require.NoError(t, err)
	sessionExpiration, err := domain.NewSessionExpiration(24 * time.Hour)
	require.NoError(t, err)
	nonce, err := domain.NewNonce("test_nonce")
	require.NoError(t, err)
	session := domain.NewSession(nonce, redirectURL, sessionExpiration)
	sessionID := session.ID()

	ctx := context.Background()
	err = repo.Save(ctx, session)
	require.NoError(t, err)

	callbackReq := httptest.NewRequest("GET", fmt.Sprintf("/callback?code=%s&state=invalid_nonce", oauthProvider.code), nil)
	callbackReq.AddCookie(&http.Cookie{
		Name:  "session_id",
		Value: string(sessionID),
	})
	callbackW := httptest.NewRecorder()

	server.handleCallback(callbackW, callbackReq)

	require.Equal(t, http.StatusUnauthorized, callbackW.Code)
}
