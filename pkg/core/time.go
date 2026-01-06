package core

import "time"

// Timestamp represents a UTC timestamp, ensuring all time values are stored and compared in UTC.
// This prevents timezone-related bugs in distributed systems.
//
// Example:
//   now := core.NewTimestamp(time.Now())
//   expiry := core.NewTimestamp(time.Now().Add(24 * time.Hour))
//
//   // Use in domain logic
//   if now.Time().After(expiry.Time()) {
//       // Token expired
//   }
type Timestamp time.Time

// NewTimestamp creates a new Timestamp from a time.Time, converting it to UTC.
// This ensures all timestamps are normalized to UTC for consistency.
//
// Example:
//   timestamp := core.NewTimestamp(time.Now())
//   // timestamp is now in UTC
func NewTimestamp(t time.Time) Timestamp {
	return Timestamp(t.UTC())
}

// Time converts the Timestamp back to a time.Time.
// The returned time will be in UTC.
//
// Example:
//   timestamp := core.NewTimestamp(time.Now())
//   t := timestamp.Time()
//   fmt.Println(t.Format(time.RFC3339))
func (t Timestamp) Time() time.Time {
	return time.Time(t)
}

// Clock provides an abstraction for getting the current time.
// This enables testability by allowing time to be controlled in tests.
//
// Example:
//   type Server struct {
//       clock domain.Clock
//   }
//
//   func (s *Server) ProcessRequest() {
//       now := s.clock.UTCNow()
//       // Use now for time-dependent logic
//   }
//
//   // In production
//   server := &Server{clock: &core.WallClock{}}
//
//   // In tests
//   testClock := &core.TestClock{}
//   testClock.SetNow(core.NewTimestamp(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)))
//   server := &Server{clock: testClock}
type Clock interface {
	// UTCNow returns the current time as a UTC Timestamp.
	UTCNow() Timestamp
}

// WallClock is a Clock implementation that returns the actual current time.
// Use this in production code.
//
// Example:
//   clock := &core.WallClock{}
//   now := clock.UTCNow()
type WallClock struct {
}

// UTCNow returns the current wall clock time in UTC.
func (w *WallClock) UTCNow() Timestamp {
	return NewTimestamp(time.Now().UTC())
}

var _ Clock = (*WallClock)(nil)

// TestClock is a Clock implementation for testing that allows controlling the current time.
// This enables deterministic tests for time-dependent logic.
//
// Example:
//   func TestTokenExpiry(t *testing.T) {
//       clock := &core.TestClock{}
//       now := core.NewTimestamp(time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC))
//       clock.SetNow(now)
//
//       // Create token that expires in 1 hour
//       expiry := clock.Add(1 * time.Hour)
//
//       // Advance time by 2 hours
//       clock.SetNow(clock.Add(2 * time.Hour))
//
//       // Verify token is expired
//       if clock.UTCNow().Time().After(expiry.Time()) {
//           t.Log("Token expired as expected")
//       }
//   }
type TestClock struct {
	now Timestamp
}

// UTCNow returns the current time set on the test clock.
func (t *TestClock) UTCNow() Timestamp {
	return t.now
}

// SetNow sets the current time for the test clock.
// This allows controlling time in tests.
//
// Example:
//   clock := &core.TestClock{}
//   clock.SetNow(core.NewTimestamp(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)))
func (t *TestClock) SetNow(now Timestamp) {
	t.now = now
}

// Add returns a new Timestamp that is the current test clock time plus the given duration.
// This is useful for calculating future times in tests without modifying the clock.
//
// Example:
//   clock := &core.TestClock{}
//   clock.SetNow(core.NewTimestamp(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)))
//   futureTime := clock.Add(24 * time.Hour)
//   // futureTime is 2024-01-02 00:00:00 UTC
func (t *TestClock) Add(d time.Duration) Timestamp {
	return NewTimestamp(time.Time(t.now).Add(d))
}

var _ Clock = (*TestClock)(nil)
