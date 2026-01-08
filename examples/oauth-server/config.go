package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
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

func getEnvOrPanic(key string) string {
	value := os.Getenv(key)
	if value == "" {
		panic(fmt.Sprintf("environment variable %s not found", key))
	}
	return value
}

func getEnvOrDefault(key string, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func LoadConfig() (*Config, error) {
	channelSizeStr := getEnvOrDefault("REFRESH_WORKER_CHANNEL_SIZE", "100")
	size, err := strconv.Atoi(channelSizeStr)
	if err != nil {
		return nil, fmt.Errorf("invalid REFRESH_WORKER_CHANNEL_SIZE: %w", err)
	}

	timeoutStr := getEnvOrDefault("REFRESH_TIMEOUT", "5m")
	timeout, err := time.ParseDuration(timeoutStr)
	if err != nil {
		return nil, fmt.Errorf("invalid REFRESH_TIMEOUT: %w", err)
	}

	expirationStr := getEnvOrDefault("SESSION_EXPIRATION", "24h")
	expiration, err := time.ParseDuration(expirationStr)
	if err != nil {
		return nil, fmt.Errorf("invalid SESSION_EXPIRATION: %w", err)
	}

	cfg := &Config{
		oauthClientID:            getEnvOrPanic("OAUTH_CLIENT_ID"),
		oauthClientSecret:        getEnvOrPanic("OAUTH_CLIENT_SECRET"),
		oauthAuthURL:             getEnvOrPanic("OAUTH_AUTH_URL"),
		oauthTokenURL:            getEnvOrPanic("OAUTH_TOKEN_URL"),
		oauthRedirectURL:         getEnvOrPanic("OAUTH_REDIRECT_URL"),
		backendURL:               getEnvOrPanic("BACKEND_URL"),
		redisAddr:                getEnvOrDefault("REDIS_ADDR", "localhost:6379"),
		sessionCookieName:        getEnvOrDefault("SESSION_COOKIE_NAME", "session_id"),
		refreshWorkerChannelSize: size,
		refreshTimeout:           timeout,
		sessionExpiration:        expiration,
		serverPort:               getEnvOrDefault("SERVER_PORT", "8080"),
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
