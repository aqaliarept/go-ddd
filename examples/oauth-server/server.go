package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	core "github.com/aqaliarept/go-ddd-kit/pkg/core"
	redis "github.com/aqaliarept/go-ddd-kit/pkg/redis"

	"github.com/aqaliarept/go-ddd-kit/examples/oauth-server/domain"
)

var errCodeAndStateRequired = errors.New("code and state are required")

func generateNonce() (string, error) {
	bytes := make([]byte, 32)
	_, err := rand.Read(bytes)
	if err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(bytes), nil
}

type Server struct {
	scope  *core.ConcurrentScope
	oauth  *OAuthClient
	worker *RefreshWorker
	cfg    *Config
	proxy  *httputil.ReverseProxy
	clock  domain.Clock
}

func NewServer(scope *core.ConcurrentScope, oauth *OAuthClient, worker *RefreshWorker, cfg *Config, clock domain.Clock) (*Server, error) {
	backendURL, err := url.Parse(cfg.BackendURL())
	if err != nil {
		return nil, fmt.Errorf("invalid backend URL: %w", err)
	}

	proxy := httputil.NewSingleHostReverseProxy(backendURL)
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		http.Error(w, "failed to proxy request", http.StatusBadGateway)
	}

	return &Server{
		scope:  scope,
		oauth:  oauth,
		worker: worker,
		cfg:    cfg,
		proxy:  proxy,
		clock:  clock,
	}, nil
}

func (s *Server) extractSessionID(r *http.Request) (core.ID, error) {
	cookie, err := r.Cookie(s.cfg.SessionCookieName())
	if err != nil {
		return "", err
	}
	return core.ID(cookie.Value), nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/login":
		s.handleLogin(w, r)
	case "/callback":
		s.handleCallback(w, r)
	case "/api":
		s.handleAPI(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	redirectURLStr := r.URL.Query().Get("redirect_url")
	if redirectURLStr == "" {
		http.Error(w, "redirect_url is required", http.StatusBadRequest)
		return
	}

	redirectURL, err := domain.NewRedirectURL(redirectURLStr)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid redirect URL: %v", err), http.StatusBadRequest)
		return
	}

	nonceStr, err := generateNonce()
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to generate nonce: %v", err), http.StatusInternalServerError)
		return
	}
	nonce, err := domain.NewNonce(nonceStr)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to create nonce: %v", err), http.StatusInternalServerError)
		return
	}

	sessionExpiration, err := domain.NewSessionExpiration(s.cfg.SessionExpiration())
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid session expiration: %v", err), http.StatusInternalServerError)
		return
	}

	session := domain.NewSession(nonce, redirectURL, sessionExpiration)
	sessionID := session.ID()

	ctx := r.Context()
	_, err = s.scope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
		return repo.Save(ctx, session, redis.WithExpiration(session.SessionExpiration()))
	})
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to save session: %v", err), http.StatusInternalServerError)
		return
	}

	http.SetCookie(w, &http.Cookie{
		Name:     s.cfg.SessionCookieName(),
		Value:    string(sessionID),
		HttpOnly: true,
		Secure:   false,
		SameSite: http.SameSiteLaxMode,
		Path:     "/",
	})

	authURL := s.oauth.AuthCodeURL(session.Nonce().String())
	http.Redirect(w, r, authURL, http.StatusFound)
}

func (s *Server) handleCallback(w http.ResponseWriter, r *http.Request) {
	sessionID, err := s.extractSessionID(r)
	if err != nil {
		http.Error(w, "session cookie not found", http.StatusUnauthorized)
		return
	}

	ctx := r.Context()
	var session *domain.Session
	_, err = s.scope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
		session = &domain.Session{}
		loadErr := repo.Load(ctx, sessionID, session)
		if loadErr != nil {
			return loadErr
		}

		code := r.URL.Query().Get("code")
		state := r.URL.Query().Get("state")

		if code == "" || state == "" {
			return errCodeAndStateRequired
		}

		token, exchangeErr := s.oauth.Exchange(ctx, code)
		if exchangeErr != nil {
			return fmt.Errorf("failed to exchange code for token: %w", exchangeErr)
		}

		expectedNonce, nonceErr := domain.NewNonce(state)
		if nonceErr != nil {
			return fmt.Errorf("invalid nonce: %w", nonceErr)
		}

		accessToken, tokenErr := domain.NewAccessToken(token.AccessToken)
		if tokenErr != nil {
			return fmt.Errorf("invalid access token: %w", tokenErr)
		}

		refreshTokenStr := token.RefreshToken
		if refreshTokenStr == "" {
			refreshTokenStr = token.AccessToken
		}
		refreshToken, refreshErr := domain.NewRefreshToken(refreshTokenStr)
		if refreshErr != nil {
			return fmt.Errorf("invalid refresh token: %w", refreshErr)
		}

		now := s.clock.UTCNow()
		tokenExpiryTime := now.Time().Add(time.Until(token.Expiry))
		tokenExpiry, expiryErr := domain.NewTokenExpiry(domain.NewTimestamp(tokenExpiryTime))
		if expiryErr != nil {
			return fmt.Errorf("invalid token expiry: %w", expiryErr)
		}

		sessionExpiration := 2 * time.Until(tokenExpiryTime)
		sessionExp, sessionExpErr := domain.NewSessionExpiration(sessionExpiration)
		if sessionExpErr != nil {
			return fmt.Errorf("invalid session expiration: %w", sessionExpErr)
		}

		_, completeErr := session.CompleteAuthorizationCodeFlow(expectedNonce, accessToken, refreshToken, tokenExpiry, sessionExp, now)
		if completeErr != nil {
			return fmt.Errorf("failed to receive tokens: %w", completeErr)
		}

		return repo.Save(ctx, session, redis.WithExpiration(session.SessionExpiration()))
	})
	if err != nil {
		errMsg := err.Error()
		if errMsg == "code and state are required" {
			http.Error(w, errMsg, http.StatusBadRequest)
			return
		}
		if errors.Is(err, core.ErrAggregateNotFound) {
			http.Error(w, fmt.Sprintf("session not found: %v", err), http.StatusUnauthorized)
			return
		}
		if strings.Contains(errMsg, "failed to exchange code for token") {
			http.Error(w, errMsg, http.StatusInternalServerError)
			return
		}
		if strings.Contains(errMsg, "failed to receive tokens") {
			http.Error(w, errMsg, http.StatusUnauthorized)
			return
		}
		http.Error(w, fmt.Sprintf("failed to process callback: %v", err), http.StatusInternalServerError)
		return
	}

	http.Redirect(w, r, session.GetRedirectURLForCallback().String(), http.StatusFound)
}

func (s *Server) handleAPI(w http.ResponseWriter, r *http.Request) {
	sessionID, err := s.extractSessionID(r)
	if err != nil {
		http.Error(w, "session cookie not found", http.StatusUnauthorized)
		return
	}

	ctx := r.Context()
	var result domain.ProcessRequestResult
	var events core.EventPack
	_, err = s.scope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
		session := &domain.Session{}
		loadErr := repo.Load(ctx, sessionID, session)
		if loadErr != nil {
			return loadErr
		}

		var processErr error
		now := s.clock.UTCNow()
		result, events, processErr = session.ProcessRequest(now)
		if processErr != nil {
			return processErr
		}

		return repo.Save(ctx, session, redis.WithExpiration(session.SessionExpiration()))
	})
	if err != nil {
		if errors.Is(err, core.ErrAggregateNotFound) {
			http.Error(w, fmt.Sprintf("session not found: %v", err), http.StatusUnauthorized)
			return
		}
		http.Error(w, fmt.Sprintf("failed to process request: %v", err), http.StatusUnauthorized)
		return
	}

	refreshQueuedEvents := core.EventsOfType[domain.RefreshQueued](events)
	if len(refreshQueuedEvents) > 0 {
		s.worker.Queue(sessionID, string(refreshQueuedEvents[0].RefreshToken))
	}

	r.Header.Set("Authorization", "Bearer "+string(result.AccessToken))
	r.Header.Del("Cookie")

	s.proxy.ServeHTTP(w, r)
}
