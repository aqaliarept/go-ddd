package domain

import (
	"errors"
	"fmt"
	"net/url"
	"time"
)

var (
	errNonceEmpty                = errors.New("nonce cannot be empty")
	errRedirectURLEmpty         = errors.New("redirect URL cannot be empty")
	errAccessTokenEmpty         = errors.New("access token cannot be empty")
	errRefreshTokenEmpty        = errors.New("refresh token cannot be empty")
	errTokenExpiryZero          = errors.New("token expiry cannot be zero")
	errRefreshTimeoutPositive   = errors.New("refresh timeout must be positive")
	errSessionExpirationPositive = errors.New("session expiration must be positive")
)


type SessionStatus string

const (
	statusPending        SessionStatus = "pending"
	statusAuthenticated  SessionStatus = "authenticated"
	statusRefreshOngoing SessionStatus = "refresh_ongoing"
)

type Nonce string

func NewNonce(value string) (Nonce, error) {
	if value == "" {
		return "", errNonceEmpty
	}
	return Nonce(value), nil
}

func (n Nonce) String() string {
	return string(n)
}

type RedirectURL string

func NewRedirectURL(value string) (RedirectURL, error) {
	if value == "" {
		return "", errRedirectURLEmpty
	}
	if _, err := url.Parse(value); err != nil {
		return "", fmt.Errorf("invalid redirect URL: %w", err)
	}
	return RedirectURL(value), nil
}

func (r RedirectURL) String() string {
	return string(r)
}

type AccessToken string

func NewAccessToken(value string) (AccessToken, error) {
	if value == "" {
		return "", errAccessTokenEmpty
	}
	return AccessToken(value), nil
}

type RefreshToken string

func NewRefreshToken(value string) (RefreshToken, error) {
	if value == "" {
		return "", errRefreshTokenEmpty
	}
	return RefreshToken(value), nil
}

type TokenExpiry Timestamp

func NewTokenExpiry(value Timestamp) (TokenExpiry, error) {
	if value.Time().IsZero() {
		return TokenExpiry{}, errTokenExpiryZero
	}
	return TokenExpiry(value), nil
}

func (t TokenExpiry) Time() time.Time {
	return Timestamp(t).Time()
}

type RefreshTimeout time.Duration

func NewRefreshTimeout(value time.Duration) (RefreshTimeout, error) {
	if value <= 0 {
		return RefreshTimeout(0), errRefreshTimeoutPositive
	}
	return RefreshTimeout(value), nil
}

func (r RefreshTimeout) Duration() time.Duration {
	return time.Duration(r)
}

func (r RefreshTimeout) IsZero() bool {
	return time.Duration(r) == 0
}

type SessionExpiration time.Duration

func NewSessionExpiration(value time.Duration) (SessionExpiration, error) {
	if value <= 0 {
		return SessionExpiration(0), errSessionExpirationPositive
	}
	return SessionExpiration(value), nil
}

func (s SessionExpiration) Duration() time.Duration {
	return time.Duration(s)
}
