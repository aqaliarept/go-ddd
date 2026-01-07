// Package domain provides domain models for the OAuth server example.
package domain

import (
	"fmt"
	"time"

	core "github.com/aqaliarept/go-ddd-kit/pkg/core"
	redis "github.com/aqaliarept/go-ddd-kit/pkg/redis"
	"github.com/google/uuid"
)

//nolint:govet
type SessionState struct {
	Nonce             Nonce             `json:"nonce"`
	RedirectURL       RedirectURL       `json:"redirectURL"`
	AccessToken       AccessToken       `json:"accessToken"`
	RefreshToken      RefreshToken      `json:"refreshToken"`
	TokenExpiry       TokenExpiry       `json:"tokenExpiry"`
	Status            SessionStatus     `json:"status"`
	StartRefreshAfter Timestamp         `json:"startRefreshAfter"`
	RefreshTimeout    RefreshTimeout    `json:"refreshTimeout"`
	RefreshStartedAt  Timestamp         `json:"refreshStarted"`
	SessionExpiration SessionExpiration `json:"sessionExpiration"`
}

func (s *SessionState) Apply(event core.Event) {
	switch e := event.(type) {
	case SessionCreated:
		s.Nonce = e.Nonce
		s.RedirectURL = e.RedirectURL
		s.SessionExpiration = e.SessionExpiration
		s.Status = statusPending
	case TokensReceived:
		s.AccessToken = e.AccessToken
		s.RefreshToken = e.RefreshToken
		s.TokenExpiry = e.TokenExpiry
		s.SessionExpiration = e.SessionExpiration
		s.StartRefreshAfter = e.StartRefreshAfter
		s.Status = statusAuthenticated
	case RefreshQueued:
		s.Status = statusRefreshOngoing
		s.RefreshStartedAt = e.At
	case core.Tombstone:
		// ignore
	default:
		core.PanicUnsupportedEvent(event)
	}
}

type SessionCreated struct {
	Nonce             Nonce
	RedirectURL       RedirectURL
	SessionExpiration SessionExpiration
}

//nolint:govet
type TokensReceived struct {
	AccessToken       AccessToken
	RefreshToken      RefreshToken
	TokenExpiry       TokenExpiry
	SessionExpiration SessionExpiration
	StartRefreshAfter Timestamp
}

type RefreshQueued struct {
	At           Timestamp
	RefreshToken RefreshToken
}
type ProcessRequestResult struct {
	AccessToken AccessToken
}

type Session struct {
	core.Aggregate[SessionState]
}

func NewSession(nonce Nonce, redirectURL RedirectURL, sessionExpiration SessionExpiration) *Session {
	agg := &Session{}
	sessionID := core.ID(uuid.New().String())
	agg.Initialize(sessionID, SessionCreated{
		Nonce:             nonce,
		RedirectURL:       redirectURL,
		SessionExpiration: sessionExpiration,
	})
	return agg
}

func (s *Session) StorageOptions() []core.StorageOption {
	return []core.StorageOption{redis.WithNamespace("sessions")}
}

func startRefreshAfter(tokenExpiry TokenExpiry, now Timestamp) Timestamp {
	return NewTimestamp(now.Time().Add(tokenExpiry.Time().Sub(now.Time()) / 2))
}

func (s *Session) CompleteAuthorizationCodeFlow(expectedNonce Nonce, accessToken AccessToken, refreshToken RefreshToken, tokenExpiry TokenExpiry, sessionExpiration SessionExpiration, now Timestamp) (core.EventPack, error) {
	return s.ProcessCommand(func(state *SessionState, er core.EventRiser) error {
		if state.Nonce != expectedNonce {
			return fmt.Errorf("invalid nonce")
		}
		er.Raise(TokensReceived{
			AccessToken:       accessToken,
			RefreshToken:      refreshToken,
			TokenExpiry:       tokenExpiry,
			SessionExpiration: sessionExpiration,
			StartRefreshAfter: startRefreshAfter(tokenExpiry, now),
		})
		return nil
	})
}

func (s *Session) RefreshTokens(accessToken AccessToken, refreshToken RefreshToken, tokenExpiry TokenExpiry, sessionExpiration SessionExpiration, now Timestamp) (core.EventPack, error) {
	return s.ProcessCommand(func(state *SessionState, er core.EventRiser) error {
		er.Raise(TokensReceived{
			AccessToken:       accessToken,
			RefreshToken:      refreshToken,
			TokenExpiry:       tokenExpiry,
			SessionExpiration: sessionExpiration,
			StartRefreshAfter: startRefreshAfter(tokenExpiry, now),
		})
		return nil
	})
}

func (s *Session) shouldStartRefresh(now Timestamp) bool {
	state := s.State()
	if state.Status == statusRefreshOngoing &&
		now.Time().After(state.RefreshStartedAt.Time().Add(state.RefreshTimeout.Duration())) {
		return true
	} else if state.Status == statusAuthenticated {
		if now.Time().After(state.StartRefreshAfter.Time()) || now.Time().Equal(state.StartRefreshAfter.Time()) {
			return true
		}
	}
	return false
}

func (s *Session) ProcessRequest(now Timestamp) (ProcessRequestResult, core.EventPack, error) {
	var result ProcessRequestResult
	events, err := s.ProcessCommand(func(state *SessionState, er core.EventRiser) error {
		if state.Status != statusAuthenticated && state.Status != statusRefreshOngoing {
			return fmt.Errorf("session is not in a valid state for processing requests")
		}
		result.AccessToken = state.AccessToken
		if s.shouldStartRefresh(now) {
			er.Raise(RefreshQueued{RefreshToken: state.RefreshToken, At: now})
		}
		return nil
	})
	return result, events, err
}

func (s *Session) GetRedirectURLForCallback() RedirectURL {
	return s.State().RedirectURL
}

func (s *Session) Nonce() Nonce {
	return s.State().Nonce
}

func (s *Session) SessionExpiration() time.Duration {
	return s.State().SessionExpiration.Duration()
}
