// Package server provides OAuth server functionality.
package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

var (
	errOAuthClientIDRequired     = errors.New("OAUTH_CLIENT_ID is required")
	errOAuthClientSecretRequired = errors.New("OAUTH_CLIENT_SECRET is required")
	errOAuthAuthURLRequired      = errors.New("OAUTH_AUTH_URL is required")
	errOAuthTokenURLRequired     = errors.New("OAUTH_TOKEN_URL is required")
	errOAuthRedirectURLRequired  = errors.New("OAUTH_REDIRECT_URL is required")
	errBackendURLRequired        = errors.New("BACKEND_URL is required")
)

//nolint:govet
type Config struct {
	oauthClientID            string
	oauthClientSecret        string
	oauthAuthURL             string
	oauthTokenURL            string
	oauthRedirectURL         string
	backendURL               string
	redisAddr                string
	sessionCookieName        string
	refreshWorkerChannelSize int
	refreshTimeout           time.Duration
	sessionExpiration        time.Duration
	serverPort               string
}

func LoadConfig() (*Config, error) {
	cfg := &Config{}

	cfg.oauthClientID = os.Getenv("OAUTH_CLIENT_ID")
	if cfg.oauthClientID == "" {
		return nil, errOAuthClientIDRequired
	}

	cfg.oauthClientSecret = os.Getenv("OAUTH_CLIENT_SECRET")
	if cfg.oauthClientSecret == "" {
		return nil, errOAuthClientSecretRequired
	}

	cfg.oauthAuthURL = os.Getenv("OAUTH_AUTH_URL")
	if cfg.oauthAuthURL == "" {
		return nil, errOAuthAuthURLRequired
	}

	cfg.oauthTokenURL = os.Getenv("OAUTH_TOKEN_URL")
	if cfg.oauthTokenURL == "" {
		return nil, errOAuthTokenURLRequired
	}

	cfg.oauthRedirectURL = os.Getenv("OAUTH_REDIRECT_URL")
	if cfg.oauthRedirectURL == "" {
		return nil, errOAuthRedirectURLRequired
	}

	cfg.backendURL = os.Getenv("BACKEND_URL")
	if cfg.backendURL == "" {
		return nil, errBackendURLRequired
	}

	cfg.redisAddr = os.Getenv("REDIS_ADDR")
	if cfg.redisAddr == "" {
		cfg.redisAddr = "localhost:6379"
	}

	cfg.sessionCookieName = os.Getenv("SESSION_COOKIE_NAME")
	if cfg.sessionCookieName == "" {
		cfg.sessionCookieName = "session_id"
	}

	channelSizeStr := os.Getenv("REFRESH_WORKER_CHANNEL_SIZE")
	if channelSizeStr == "" {
		cfg.refreshWorkerChannelSize = 100
	} else {
		size, err := strconv.Atoi(channelSizeStr)
		if err != nil {
			return nil, fmt.Errorf("invalid REFRESH_WORKER_CHANNEL_SIZE: %w", err)
		}
		cfg.refreshWorkerChannelSize = size
	}

	timeoutStr := os.Getenv("REFRESH_TIMEOUT")
	if timeoutStr == "" {
		cfg.refreshTimeout = 5 * time.Minute
	} else {
		timeout, err := time.ParseDuration(timeoutStr)
		if err != nil {
			return nil, fmt.Errorf("invalid REFRESH_TIMEOUT: %w", err)
		}
		cfg.refreshTimeout = timeout
	}

	expirationStr := os.Getenv("SESSION_EXPIRATION")
	if expirationStr == "" {
		cfg.sessionExpiration = 24 * time.Hour
	} else {
		expiration, err := time.ParseDuration(expirationStr)
		if err != nil {
			return nil, fmt.Errorf("invalid SESSION_EXPIRATION: %w", err)
		}
		cfg.sessionExpiration = expiration
	}

	cfg.serverPort = os.Getenv("SERVER_PORT")
	if cfg.serverPort == "" {
		cfg.serverPort = "8080"
	}

	return cfg, nil
}

func (c *Config) OAuthClientID() string {
	return c.oauthClientID
}

func (c *Config) OAuthClientSecret() string {
	return c.oauthClientSecret
}

func (c *Config) OAuthAuthURL() string {
	return c.oauthAuthURL
}

func (c *Config) OAuthTokenURL() string {
	return c.oauthTokenURL
}

func (c *Config) OAuthRedirectURL() string {
	return c.oauthRedirectURL
}

func (c *Config) BackendURL() string {
	return c.backendURL
}

func (c *Config) RedisAddr() string {
	return c.redisAddr
}

func (c *Config) SessionCookieName() string {
	return c.sessionCookieName
}

func (c *Config) RefreshWorkerChannelSize() int {
	return c.refreshWorkerChannelSize
}

func (c *Config) RefreshTimeout() time.Duration {
	return c.refreshTimeout
}

func (c *Config) SessionExpiration() time.Duration {
	return c.sessionExpiration
}

func (c *Config) ServerPort() string {
	return c.serverPort
}
