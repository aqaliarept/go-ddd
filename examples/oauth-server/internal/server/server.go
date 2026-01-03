package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	core "github.com/aqaliarept/go-ddd-kit/pkg/core"
	redis "github.com/aqaliarept/go-ddd-kit/pkg/redis"
)

type Server struct {
	scope  *core.ConcurrentScope
	oauth  *OAuthClient
	worker *RefreshWorker
	cfg    *Config
	proxy  *httputil.ReverseProxy
}

func NewServer(scope *core.ConcurrentScope, oauth *OAuthClient, worker *RefreshWorker, cfg *Config) (*Server, error) {
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
	redirectURL := r.URL.Query().Get("redirect_url")
	if redirectURL == "" {
		http.Error(w, "redirect_url is required", http.StatusBadRequest)
		return
	}

	session := NewSession(redirectURL, s.cfg.SessionExpiration())
	sessionID := session.ID()

	ctx := r.Context()
	_, err := s.scope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
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

	authURL := s.oauth.AuthCodeURL(session.Nonce())
	http.Redirect(w, r, authURL, http.StatusFound)
}

func (s *Server) handleCallback(w http.ResponseWriter, r *http.Request) {
	sessionID, err := s.extractSessionID(r)
	if err != nil {
		http.Error(w, "session cookie not found", http.StatusUnauthorized)
		return
	}

	ctx := r.Context()
	var session *Session
	_, err = s.scope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
		session = &Session{}
		err := repo.Load(ctx, sessionID, session)
		if err != nil {
			return err
		}

		code := r.URL.Query().Get("code")
		state := r.URL.Query().Get("state")

		if code == "" || state == "" {
			return fmt.Errorf("code and state are required")
		}

		token, err := s.oauth.Exchange(ctx, code)
		if err != nil {
			return fmt.Errorf("failed to exchange code for token: %w", err)
		}

		tokenExpiry := time.Now().Add(time.Until(token.Expiry))
		refreshTokenExpiry := time.Now().Add(24 * time.Hour)
		refreshToken := token.RefreshToken
		if refreshToken == "" {
			refreshToken = token.AccessToken
		}

		_, err = session.CompleteAuthorizationCodeFlow(state, token.AccessToken, refreshToken, tokenExpiry, refreshTokenExpiry)
		if err != nil {
			return fmt.Errorf("failed to receive tokens: %w", err)
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

	http.Redirect(w, r, session.GetRedirectURLForCallback(), http.StatusFound)
}

func (s *Server) handleAPI(w http.ResponseWriter, r *http.Request) {
	sessionID, err := s.extractSessionID(r)
	if err != nil {
		http.Error(w, "session cookie not found", http.StatusUnauthorized)
		return
	}

	ctx := r.Context()
	var result ProcessRequestResult
	var events core.EventPack
	_, err = s.scope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
		session := &Session{}
		err := repo.Load(ctx, sessionID, session)
		if err != nil {
			return err
		}

		var processErr error
		result, events, processErr = session.ProcessRequest()
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

	refreshQueuedEvents := core.EventsOfType[RefreshQueued](events)
	if len(refreshQueuedEvents) > 0 {
		s.worker.Queue(sessionID)
	}

	r.Header.Set("Authorization", "Bearer "+result.AccessToken)
	r.Header.Del("Cookie")

	s.proxy.ServeHTTP(w, r)
}
