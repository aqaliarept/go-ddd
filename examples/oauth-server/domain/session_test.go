package domain_test

import (
	"testing"
	"time"

	"github.com/aqaliarept/go-ddd-kit/examples/oauth-server/domain"
	core "github.com/aqaliarept/go-ddd-kit/pkg/core"
	"github.com/stretchr/testify/require"
)

func setup(t *testing.T) (*domain.Session, domain.Nonce, domain.SessionExpiration) {
	t.Helper()
	nonce, err := domain.NewNonce("test-nonce")
	require.NoError(t, err)

	redirectURL, err := domain.NewRedirectURL("https://example.com/callback")
	require.NoError(t, err)

	sessionExpiration, err := domain.NewSessionExpiration(30 * time.Minute)
	require.NoError(t, err)

	return domain.NewSession(nonce, redirectURL, sessionExpiration), nonce, sessionExpiration
}

func TestCompleteAuthorizationCodeFlow(t *testing.T) {
	t.Run("Given a session in pending state When CompleteAuthorizationCodeFlow is called with valid nonce Then TokensReceived event is raised And state transitions to authenticated", func(t *testing.T) {
		session, nonce, sessionExpiration := setup(t)

		accessToken, err := domain.NewAccessToken("access-token-123")
		require.NoError(t, err)
		refreshToken, err := domain.NewRefreshToken("refresh-token-456")
		require.NoError(t, err)
		tokenExpiry, err := domain.NewTokenExpiry(domain.NewTimestamp(time.Now().Add(1 * time.Hour)))
		require.NoError(t, err)
		now := domain.NewTimestamp(time.Now())

		events, err := session.CompleteAuthorizationCodeFlow(nonce, accessToken, refreshToken, tokenExpiry, sessionExpiration, now)

		require.NoError(t, err)
		require.Len(t, events, 1)

		tokensReceived, err := core.EventOfType[domain.TokensReceived](events)
		require.NoError(t, err)
		require.Equal(t, accessToken, tokensReceived.AccessToken)
		require.Equal(t, refreshToken, tokensReceived.RefreshToken)
		require.Equal(t, tokenExpiry, tokensReceived.TokenExpiry)

		state := session.State()
		require.Equal(t, domain.StatusAuthenticated, state.Status)
		require.Equal(t, accessToken, state.AccessToken)
		require.Equal(t, refreshToken, state.RefreshToken)
		require.Equal(t, tokenExpiry, state.TokenExpiry)
		require.False(t, state.StartRefreshAfter.Time().IsZero())
	})

	t.Run("Given a session in pending state When CompleteAuthorizationCodeFlow is called with invalid nonce Then error is returned And no events are raised", func(t *testing.T) {
		session, _, sessionExpiration := setup(t)

		initialState := session.State()
		require.Equal(t, domain.StatusPending, initialState.Status)

		invalidNonce, err := domain.NewNonce("invalid-nonce")
		require.NoError(t, err)
		accessToken, err := domain.NewAccessToken("access-token-123")
		require.NoError(t, err)
		refreshToken, err := domain.NewRefreshToken("refresh-token-456")
		require.NoError(t, err)
		tokenExpiry, err := domain.NewTokenExpiry(domain.NewTimestamp(time.Now().Add(1 * time.Hour)))
		require.NoError(t, err)
		now := domain.NewTimestamp(time.Now())

		events, err := session.CompleteAuthorizationCodeFlow(invalidNonce, accessToken, refreshToken, tokenExpiry, sessionExpiration, now)

		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid nonce")
		require.Empty(t, events)
		require.Error(t, session.Error())
	})
}

func TestRefreshTokens(t *testing.T) {
	t.Run("Given a session in authenticated state When RefreshTokens is called Then TokensRefreshed event is raised And new tokens are set", func(t *testing.T) {
		session, nonce, sessionExpiration := setup(t)

		accessToken1, err := domain.NewAccessToken("access-token-1")
		require.NoError(t, err)
		refreshToken1, err := domain.NewRefreshToken("refresh-token-1")
		require.NoError(t, err)
		tokenExpiry1, err := domain.NewTokenExpiry(domain.NewTimestamp(time.Now().Add(1 * time.Hour)))
		require.NoError(t, err)
		now := domain.NewTimestamp(time.Now())

		_, err = session.CompleteAuthorizationCodeFlow(nonce, accessToken1, refreshToken1, tokenExpiry1, sessionExpiration, now)
		require.NoError(t, err)

		accessToken2, err := domain.NewAccessToken("access-token-2")
		require.NoError(t, err)
		refreshToken2, err := domain.NewRefreshToken("refresh-token-2")
		require.NoError(t, err)
		tokenExpiry2, err := domain.NewTokenExpiry(domain.NewTimestamp(time.Now().Add(1 * time.Hour)))
		require.NoError(t, err)
		now2 := domain.NewTimestamp(time.Now().Add(30 * time.Minute))

		events, err := session.RefreshTokens(accessToken2, refreshToken2, tokenExpiry2, sessionExpiration, now2)

		require.NoError(t, err)
		require.Len(t, events, 1)

		tokensReceived, err := core.EventOfType[domain.TokensReceived](events)
		require.NoError(t, err)
		require.Equal(t, accessToken2, tokensReceived.AccessToken)
		require.Equal(t, refreshToken2, tokensReceived.RefreshToken)
		require.Equal(t, tokenExpiry2, tokensReceived.TokenExpiry)

		state := session.State()
		require.Equal(t, domain.StatusAuthenticated, state.Status)
		require.Equal(t, accessToken2, state.AccessToken)
		require.Equal(t, refreshToken2, state.RefreshToken)
		require.Equal(t, tokenExpiry2, state.TokenExpiry)
	})
}

func TestProcessRequest(t *testing.T) {
	t.Run("Given a session in authenticated state When ProcessRequest is called Then access token is returned And no refresh is queued if not needed", func(t *testing.T) {
		session, nonce, sessionExpiration := setup(t)

		accessToken, err := domain.NewAccessToken("access-token-123")
		require.NoError(t, err)
		refreshToken, err := domain.NewRefreshToken("refresh-token-456")
		require.NoError(t, err)
		tokenExpiry, err := domain.NewTokenExpiry(domain.NewTimestamp(time.Now().Add(1 * time.Hour)))
		require.NoError(t, err)
		now := domain.NewTimestamp(time.Now())

		_, err = session.CompleteAuthorizationCodeFlow(nonce, accessToken, refreshToken, tokenExpiry, sessionExpiration, now)
		require.NoError(t, err)

		result, events, err := session.ProcessRequest(now)

		require.NoError(t, err)
		require.Equal(t, accessToken, result.AccessToken)
		require.Empty(t, events)
	})

	t.Run("Given a session in authenticated state When ProcessRequest is called after StartRefreshAfter Then RefreshQueued event is raised", func(t *testing.T) {
		session, nonce, sessionExpiration := setup(t)

		accessToken, err := domain.NewAccessToken("access-token-123")
		require.NoError(t, err)
		refreshToken, err := domain.NewRefreshToken("refresh-token-456")
		require.NoError(t, err)
		tokenExpiry, err := domain.NewTokenExpiry(domain.NewTimestamp(time.Now().Add(1 * time.Hour)))
		require.NoError(t, err)
		now := domain.NewTimestamp(time.Now())

		_, err = session.CompleteAuthorizationCodeFlow(nonce, accessToken, refreshToken, tokenExpiry, sessionExpiration, now)
		require.NoError(t, err)

		state := session.State()
		afterRefreshTime := state.StartRefreshAfter.Time().Add(1 * time.Second)
		nowAfter := domain.NewTimestamp(afterRefreshTime)

		result, events, err := session.ProcessRequest(nowAfter)

		require.NoError(t, err)
		require.Equal(t, accessToken, result.AccessToken)
		require.Len(t, events, 1)

		refreshQueued, err := core.EventOfType[domain.RefreshQueued](events)
		require.NoError(t, err)
		require.Equal(t, refreshToken, refreshQueued.RefreshToken)
		require.Equal(t, nowAfter, refreshQueued.At)

		updatedState := session.State()
		require.Equal(t, domain.StatusRefreshOngoing, updatedState.Status)
		require.Equal(t, nowAfter, updatedState.RefreshStartedAt)
	})

	t.Run("Given a session in renew_ongoing state When ProcessRequest is called Then access token is returned", func(t *testing.T) {
		session, nonce, sessionExpiration := setup(t)

		accessToken, err := domain.NewAccessToken("access-token-123")
		require.NoError(t, err)
		refreshToken, err := domain.NewRefreshToken("refresh-token-456")
		require.NoError(t, err)
		tokenExpiry, err := domain.NewTokenExpiry(domain.NewTimestamp(time.Now().Add(1 * time.Hour)))
		require.NoError(t, err)
		now := domain.NewTimestamp(time.Now())

		_, err = session.CompleteAuthorizationCodeFlow(nonce, accessToken, refreshToken, tokenExpiry, sessionExpiration, now)
		require.NoError(t, err)

		state := session.State()
		afterRefreshTime := state.StartRefreshAfter.Time().Add(1 * time.Second)
		nowAfter := domain.NewTimestamp(afterRefreshTime)

		_, events, err := session.ProcessRequest(nowAfter)
		require.NoError(t, err)
		require.Len(t, events, 1)
		require.Equal(t, domain.SessionStatus("refresh_ongoing"), session.State().Status)

		result, events2, err := session.ProcessRequest(nowAfter)
		require.NoError(t, err)
		require.Equal(t, accessToken, result.AccessToken)
		require.Empty(t, events2)
	})

	t.Run("Given a session in pending state When ProcessRequest is called Then error is returned", func(t *testing.T) {
		session, _, _ := setup(t)

		now := domain.NewTimestamp(time.Now())
		_, events, err := session.ProcessRequest(now)

		require.Error(t, err)
		require.Contains(t, err.Error(), "not in a valid state")
		require.Empty(t, events)
	})
}


func TestStartRefreshAfter(t *testing.T) {
	t.Run("Given token expiry and current time When startRefreshAfter is called Then returns midpoint between now and expiry", func(t *testing.T) {
		now := time.Now()
		tokenExpiryTime := now.Add(1 * time.Hour)
		tokenExpiry, err := domain.NewTokenExpiry(domain.NewTimestamp(tokenExpiryTime))
		require.NoError(t, err)
		nowTimestamp := domain.NewTimestamp(now)

		session, _, sessionExpiration := setup(t)
		nonce, err := domain.NewNonce("test-nonce")
		require.NoError(t, err)
		accessToken, err := domain.NewAccessToken("access-token")
		require.NoError(t, err)
		refreshToken, err := domain.NewRefreshToken("refresh-token")
		require.NoError(t, err)

		_, err = session.CompleteAuthorizationCodeFlow(nonce, accessToken, refreshToken, tokenExpiry, sessionExpiration, nowTimestamp)
		require.NoError(t, err)

		state := session.State()
		startRefresh := state.StartRefreshAfter

		expectedMidpoint := now.Add(30 * time.Minute)
		actualTime := startRefresh.Time()
		diff := actualTime.Sub(expectedMidpoint)
		if diff < 0 {
			diff = -diff
		}
		require.True(t, diff < time.Second, "startRefreshAfter should return midpoint between now and expiry")
	})
}

func TestHelperMethods(t *testing.T) {
	t.Run("Given a session When GetRedirectURLForCallback is called Then returns redirect URL string", func(t *testing.T) {
		session, _, _ := setup(t)

		result := session.GetRedirectURLForCallback()
		require.Equal(t, "https://example.com/callback", result.String())
	})

	t.Run("Given a session When Nonce is called Then returns nonce string", func(t *testing.T) {
		session, _, _ := setup(t)

		result := session.Nonce()
		require.Equal(t, "test-nonce", result.String())
	})

	t.Run("Given a session When SessionExpiration is called Then returns session expiration duration", func(t *testing.T) {
		session, _, _ := setup(t)

		result := session.SessionExpiration()
		require.Equal(t, 30*time.Minute, result)
	})
}

func TestTokenExpiryTime(t *testing.T) {
	t.Run("Given a TokenExpiry When Time is called Then returns correct time without infinite recursion", func(t *testing.T) {
		expectedTime := time.Now().Add(1 * time.Hour)
		tokenExpiry, err := domain.NewTokenExpiry(domain.NewTimestamp(expectedTime))
		require.NoError(t, err)

		actualTime := tokenExpiry.Time()
		require.False(t, actualTime.IsZero())
		require.WithinDuration(t, expectedTime, actualTime, time.Second)
	})
}

func TestFullTokenFlow(t *testing.T) {
	t.Run("Given a complete token flow When all operations are performed Then state transitions correctly", func(t *testing.T) {
		session, nonce, sessionExpiration := setup(t)

		require.Equal(t, domain.SessionStatus("pending"), session.State().Status)

		accessToken1, err := domain.NewAccessToken("access-token-1")
		require.NoError(t, err)
		refreshToken1, err := domain.NewRefreshToken("refresh-token-1")
		require.NoError(t, err)
		tokenExpiry1, err := domain.NewTokenExpiry(domain.NewTimestamp(time.Now().Add(1 * time.Hour)))
		require.NoError(t, err)
		now1 := domain.NewTimestamp(time.Now())

		_, err = session.CompleteAuthorizationCodeFlow(nonce, accessToken1, refreshToken1, tokenExpiry1, sessionExpiration, now1)
		require.NoError(t, err)
		require.Equal(t, domain.SessionStatus("authenticated"), session.State().Status)

		now2 := domain.NewTimestamp(time.Now().Add(31 * time.Minute))
		result, events, err := session.ProcessRequest(now2)
		require.NoError(t, err)
		require.Equal(t, accessToken1, result.AccessToken)
		require.Len(t, events, 1)

		refreshQueued, err := core.EventOfType[domain.RefreshQueued](events)
		require.NoError(t, err)
		require.NotNil(t, refreshQueued)
		require.Equal(t, domain.SessionStatus("refresh_ongoing"), session.State().Status)

		accessToken2, err := domain.NewAccessToken("access-token-2")
		require.NoError(t, err)
		refreshToken2, err := domain.NewRefreshToken("refresh-token-2")
		require.NoError(t, err)
		tokenExpiry2, err := domain.NewTokenExpiry(domain.NewTimestamp(time.Now().Add(1 * time.Hour)))
		require.NoError(t, err)
		now3 := domain.NewTimestamp(time.Now().Add(32 * time.Minute))

		_, err = session.RefreshTokens(accessToken2, refreshToken2, tokenExpiry2, sessionExpiration, now3)
		require.NoError(t, err)
		require.Equal(t, domain.SessionStatus("authenticated"), session.State().Status)
		require.Equal(t, accessToken2, session.State().AccessToken)
		require.Equal(t, refreshToken2, session.State().RefreshToken)
	})
}
