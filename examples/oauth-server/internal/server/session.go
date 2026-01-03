package server

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"time"

	core "github.com/aqaliarept/go-ddd-kit/pkg/core"
	redis "github.com/aqaliarept/go-ddd-kit/pkg/redis"
	"github.com/google/uuid"
)

type sessionStatus string

const (
	statusPending       sessionStatus = "pending"
	statusAuthenticated sessionStatus = "authenticated"
	statusRefreshing    sessionStatus = "refreshing"
	statusRenewOngoing  sessionStatus = "renew_ongoing"
)

type SessionState struct {
	Nonce              string        `json:"nonce"`
	RedirectURL        string        `json:"redirectURL"`
	AccessToken        string        `json:"accessToken"`
	RefreshToken       string        `json:"refreshToken"`
	TokenExpiry        time.Time     `json:"tokenExpiry"`
	RefreshTokenExpiry time.Time     `json:"refreshTokenExpiry"`
	Status             sessionStatus `json:"status"`
	RefreshTimeout     time.Time     `json:"refreshTimeout"`
	SessionExpiration  time.Duration `json:"sessionExpiration"`
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
		s.RefreshTokenExpiry = e.RefreshTokenExpiry
		s.SessionExpiration = e.SessionExpiration
		s.Status = statusAuthenticated
	case TokensRefreshed:
		s.AccessToken = e.AccessToken
		s.RefreshToken = e.RefreshToken
		s.TokenExpiry = e.TokenExpiry
		s.RefreshTokenExpiry = e.RefreshTokenExpiry
		s.SessionExpiration = e.SessionExpiration
		s.Status = statusAuthenticated
	case RefreshQueued:
		s.Status = statusRenewOngoing
	case RefreshTimeoutUpdated:
		s.RefreshTimeout = e.Timeout
	default:
		core.PanicUnsupportedEvent(event)
	}
}

type SessionCreated struct {
	Nonce             string
	RedirectURL       string
	SessionExpiration time.Duration
}

type TokensReceived struct {
	AccessToken        string
	RefreshToken       string
	TokenExpiry        time.Time
	RefreshTokenExpiry time.Time
	SessionExpiration  time.Duration
}

type TokensRefreshed struct {
	AccessToken        string
	RefreshToken       string
	TokenExpiry        time.Time
	RefreshTokenExpiry time.Time
	SessionExpiration  time.Duration
}

type RefreshQueued struct {
	RefreshToken string
}

type RefreshTimeoutUpdated struct {
	Timeout time.Time
}

type ProcessRequestResult struct {
	AccessToken string
}

type Session struct {
	core.Aggregate[SessionState]
}

func NewSession(redirectURL string, initialExpiration time.Duration) *Session {
	agg := &Session{}
	if redirectURL != "" {
		nonce, err := generateNonce()
		if err != nil {
			panic(fmt.Sprintf("failed to generate nonce: %v", err))
		}
		sessionID := core.ID(uuid.New().String())
		agg.Initialize(sessionID, SessionCreated{
			Nonce:             nonce,
			RedirectURL:       redirectURL,
			SessionExpiration: initialExpiration,
		})
	}
	return agg
}

func generateNonce() (string, error) {
	bytes := make([]byte, 32)
	_, err := rand.Read(bytes)
	if err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(bytes), nil
}

func (s *Session) StorageOptions() []core.StorageOption {
	return []core.StorageOption{redis.WithNamespace("sessions")}
}

func (s *Session) CompleteAuthorizationCodeFlow(expectedNonce, accessToken, refreshToken string, tokenExpiry, refreshTokenExpiry time.Time) (core.EventPack, error) {
	return s.ProcessCommand(func(state *SessionState, er core.EventRiser) error {
		if state.Nonce != expectedNonce {
			return fmt.Errorf("invalid nonce")
		}
		sessionExpiration := 2 * time.Until(refreshTokenExpiry)
		er.Raise(TokensReceived{
			AccessToken:        accessToken,
			RefreshToken:       refreshToken,
			TokenExpiry:        tokenExpiry,
			RefreshTokenExpiry: refreshTokenExpiry,
			SessionExpiration:  sessionExpiration,
		})
		return nil
	})
}

func (s *Session) RefreshTokens(accessToken, refreshToken string, tokenExpiry, refreshTokenExpiry time.Time) (core.EventPack, error) {
	return s.ProcessCommand(func(state *SessionState, er core.EventRiser) error {
		sessionExpiration := 2 * time.Until(refreshTokenExpiry)
		er.Raise(TokensRefreshed{
			AccessToken:        accessToken,
			RefreshToken:       refreshToken,
			TokenExpiry:        tokenExpiry,
			RefreshTokenExpiry: refreshTokenExpiry,
			SessionExpiration:  sessionExpiration,
		})
		return nil
	})
}

func (s *Session) ProcessRequest() (ProcessRequestResult, core.EventPack, error) {
	var result ProcessRequestResult
	events, err := s.ProcessCommand(func(state *SessionState, er core.EventRiser) error {
		if state.AccessToken == "" {
			return fmt.Errorf("no access token")
		}

		result.AccessToken = state.AccessToken

		if state.Status == statusRenewOngoing {
			if state.RefreshTimeout.IsZero() || time.Now().After(state.RefreshTimeout) {
				newTimeout := time.Now().Add(5 * time.Minute)
				er.Raise(RefreshTimeoutUpdated{Timeout: newTimeout})
			}
			return nil
		}

		if state.TokenExpiry.IsZero() {
			return nil
		}

		now := time.Now()
		halfLife := state.TokenExpiry.Sub(now) / 2
		if time.Until(state.TokenExpiry) < halfLife {
			if state.Status != statusRenewOngoing {
				if state.RefreshToken == "" {
					return fmt.Errorf("no refresh token available")
				}
				er.Raise(RefreshQueued{RefreshToken: state.RefreshToken})
			}
		}

		return nil
	})
	return result, events, err
}

func (s *Session) GetRedirectURLForCallback() string {
	return s.State().RedirectURL
}

func (s *Session) Nonce() string {
	return s.State().Nonce
}

func (s *Session) SessionExpiration() time.Duration {
	return s.State().SessionExpiration
}
