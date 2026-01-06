package main

import (
	"context"
	"fmt"
	"log"
	"time"

	core "github.com/aqaliarept/go-ddd-kit/pkg/core"
	redis "github.com/aqaliarept/go-ddd-kit/pkg/redis"

	"github.com/aqaliarept/go-ddd-kit/examples/oauth-server/domain"
)

type RefreshTask struct {
	SessionID    core.ID
	RefreshToken string
}

type RefreshWorker struct {
	scope  *core.ConcurrentScope
	oauth  *OAuthClient
	queue  chan RefreshTask
	cfg    *Config
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	clock  domain.Clock
}

func NewRefreshWorker(scope *core.ConcurrentScope, oauth *OAuthClient, cfg *Config, clock domain.Clock) *RefreshWorker {
	ctx, cancel := context.WithCancel(context.Background())
	return &RefreshWorker{
		scope:  scope,
		oauth:  oauth,
		queue:  make(chan RefreshTask, cfg.RefreshWorkerChannelSize()),
		cfg:    cfg,
		ctx:    ctx,
		cancel: cancel,
		done:   make(chan struct{}),
		clock:  clock,
	}
}

func (w *RefreshWorker) Start() {
	go w.run()
}

func (w *RefreshWorker) run() {
	defer close(w.done)
	for {
		select {
		case <-w.ctx.Done():
			return
		case task := <-w.queue:
			w.processRefresh(task)
		}
	}
}

func (w *RefreshWorker) processRefresh(task RefreshTask) {
	ctx := context.Background()

	_, err := w.scope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
		session := &domain.Session{}
		err := repo.Load(ctx, task.SessionID, session)
		if err != nil {
			return err
		}

		if task.RefreshToken == "" {
			return fmt.Errorf("no refresh token for session %s", task.SessionID)
		}

		token, err := w.oauth.RefreshToken(ctx, task.RefreshToken)
		if err != nil {
			return fmt.Errorf("failed to refresh token: %w", err)
		}

		accessToken, err := domain.NewAccessToken(token.AccessToken)
		if err != nil {
			return fmt.Errorf("invalid access token: %w", err)
		}

		refreshTokenStr := task.RefreshToken
		if token.RefreshToken != "" {
			refreshTokenStr = token.RefreshToken
		}
		refreshToken, err := domain.NewRefreshToken(refreshTokenStr)
		if err != nil {
			return fmt.Errorf("invalid refresh token: %w", err)
		}

		now := w.clock.UTCNow()
		tokenExpiryTime := now.Time().Add(time.Until(token.Expiry))
		tokenExpiry, err := domain.NewTokenExpiry(domain.NewTimestamp(tokenExpiryTime))
		if err != nil {
			return fmt.Errorf("invalid token expiry: %w", err)
		}

		sessionExpiration := 2 * time.Until(tokenExpiryTime)
		sessionExp, err := domain.NewSessionExpiration(sessionExpiration)
		if err != nil {
			return fmt.Errorf("invalid session expiration: %w", err)
		}

		_, err = session.RefreshTokens(accessToken, refreshToken, tokenExpiry, sessionExp, now)
		if err != nil {
			return fmt.Errorf("failed to update tokens: %w", err)
		}

		return repo.Save(ctx, session, redis.WithExpiration(session.SessionExpiration()))
	})
	if err != nil {
		log.Printf("failed to process refresh for session %s: %v", task.SessionID, err)
		return
	}

	log.Printf("successfully refreshed tokens for session %s", task.SessionID)
}
func (w *RefreshWorker) Queue(sessionID core.ID, refreshToken string) {
	select {
	case w.queue <- RefreshTask{SessionID: sessionID, RefreshToken: refreshToken}:
	default:
		log.Printf("refresh queue full, dropping session %s", sessionID)
	}
}
func (w *RefreshWorker) Shutdown() {
	w.cancel()
	<-w.done
}
