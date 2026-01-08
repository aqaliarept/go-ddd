// Package server provides OAuth server functionality.
package main

import (
	"context"
	"fmt"

	"golang.org/x/oauth2"
)

type OAuthClient struct {
	config *oauth2.Config
}

func NewOAuthClient(cfg *Config) *OAuthClient {
	oauthConfig := &oauth2.Config{
		ClientID:     cfg.OAuthClientID(),
		ClientSecret: cfg.OAuthClientSecret(),
		RedirectURL:  cfg.OAuthRedirectURL(),
		Endpoint: oauth2.Endpoint{
			AuthURL:  cfg.OAuthAuthURL(),
			TokenURL: cfg.OAuthTokenURL(),
			AuthStyle: oauth2.AuthStyleAutoDetect,
			DeviceAuthURL: "",
		},
		Scopes: []string{"openid", "profile", "email"},
	}

	return &OAuthClient{
		config: oauthConfig,
	}
}

func (c *OAuthClient) AuthCodeURL(state string) string {
	return c.config.AuthCodeURL(state, oauth2.AccessTypeOffline)
}

func (c *OAuthClient) Exchange(ctx context.Context, code string) (*oauth2.Token, error) {
	token, err := c.config.Exchange(ctx, code)
	if err != nil {
		return nil, fmt.Errorf("failed to exchange code for token: %w", err)
	}
	return token, nil
}

func (c *OAuthClient) RefreshToken(ctx context.Context, refreshToken string) (*oauth2.Token, error) {
	//nolint:exhaustruct
	tokenSource := c.config.TokenSource(ctx, &oauth2.Token{
		RefreshToken: refreshToken,
	})
	token, err := tokenSource.Token()
	if err != nil {
		return nil, fmt.Errorf("failed to refresh token: %w", err)
	}
	return token, nil
}
