package core

import "time"

type Timestamp time.Time

func NewTimestamp(t time.Time) Timestamp {
	return Timestamp(t.UTC())
}

func (t Timestamp) Time() time.Time {
	return time.Time(t)
}

type Clock interface {
	UTCNow() Timestamp
}

type WallClock struct {
}

func (w *WallClock) UTCNow() Timestamp {
	return NewTimestamp(time.Now().UTC())
}

var _ Clock = (*WallClock)(nil)

type TestClock struct {
	now Timestamp
}

func (t *TestClock) UTCNow() Timestamp {
	return t.now
}

func (t *TestClock) SetNow(now Timestamp) {
	t.now = now
}

func (t *TestClock) Add(d time.Duration) Timestamp {
	return NewTimestamp(time.Time(t.now).Add(d))
}

var _ Clock = (*TestClock)(nil)
