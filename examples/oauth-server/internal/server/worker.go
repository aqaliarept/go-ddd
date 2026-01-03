package server

import (
	"context"
	"fmt"
	"log"
	"time"

	core "github.com/aqaliarept/go-ddd-kit/pkg/core"
	redis "github.com/aqaliarept/go-ddd-kit/pkg/redis"
)

type RefreshWorker struct {
	scope  *core.ConcurrentScope
	oauth  *OAuthClient
	queue  chan core.ID
	cfg    *Config
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

func NewRefreshWorker(scope *core.ConcurrentScope, oauth *OAuthClient, cfg *Config) *RefreshWorker {
	ctx, cancel := context.WithCancel(context.Background())
	return &RefreshWorker{
		scope:  scope,
		oauth:  oauth,
		queue:  make(chan core.ID, cfg.RefreshWorkerChannelSize()),
		cfg:    cfg,
		ctx:    ctx,
		cancel: cancel,
		done:   make(chan struct{}),
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
		case sessionID := <-w.queue:
			w.processRefresh(sessionID)
		}
	}
}

func (w *RefreshWorker) processRefresh(sessionID core.ID) {
	ctx := context.Background()

	_, err := w.scope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
		session := &Session{}
		err := repo.Load(ctx, sessionID, session)
		if err != nil {
			return err
		}

		state := session.State()
		if state.RefreshToken == "" {
			return fmt.Errorf("no refresh token for session %s", sessionID)
		}
		refreshToken := state.RefreshToken

		token, err := w.oauth.RefreshToken(ctx, refreshToken)
		if err != nil {
			return fmt.Errorf("failed to refresh token: %w", err)
		}

		tokenExpiry := time.Now().Add(time.Until(token.Expiry))
		refreshTokenExpiry := time.Now().Add(24 * time.Hour)
		if token.RefreshToken != "" {
			refreshToken = token.RefreshToken
		}

		_, err = session.RefreshTokens(token.AccessToken, refreshToken, tokenExpiry, refreshTokenExpiry)
		if err != nil {
			return fmt.Errorf("failed to update tokens: %w", err)
		}

		return repo.Save(ctx, session, redis.WithExpiration(session.SessionExpiration()))
	})
	if err != nil {
		log.Printf("failed to process refresh for session %s: %v", sessionID, err)
		return
	}

	log.Printf("successfully refreshed tokens for session %s", sessionID)
}

func (w *RefreshWorker) Queue(sessionID core.ID) {
	select {
	case w.queue <- sessionID:
	default:
		log.Printf("refresh queue full, dropping session %s", sessionID)
	}
}

func (w *RefreshWorker) Shutdown() {
	w.cancel()
	<-w.done
}
