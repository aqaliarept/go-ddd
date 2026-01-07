// Package core provides the core domain-driven design abstractions for aggregates, events, and repositories.
package core

import (
	"errors"
	"fmt"

	"github.com/google/go-cmp/cmp"
)

var (
	// ErrNoEvents is returned by EventOfType when no events of the requested type are found in the event pack.
	//
	// Example:
	//   pack := EventPack{Created{}}
	//   _, err := EventOfType[ValueUpdated](pack)
	//   if err == ErrNoEvents {
	//       // Handle no events case
	//   }
	ErrNoEvents = errors.New("no events")

	// ErrTooManyEvents is returned by EventOfType when multiple events of the requested type are found in the event pack.
	//
	// Example:
	//   pack := EventPack{ValueUpdated{"first"}, ValueUpdated{"second"}}
	//   _, err := EventOfType[ValueUpdated](pack)
	//   if err == ErrTooManyEvents {
	//       // Handle multiple events case - use EventsOfType instead
	//   }
	ErrTooManyEvents = errors.New("too many events")

	errAggregateAlreadyInitialized = errors.New("aggregate is already initialized")
)

type (
	// State is a type alias for any type that represents aggregate state.
	State any

	// StatePtr is a type alias for a pointer to state, used in storage and restoration operations.
	StatePtr any

	// AggregatePtr is a type alias for a pointer to an aggregate, used for change tracking.
	AggregatePtr any

	// Event represents a domain event. Events are plain structs that represent state changes.
	// Each event should correspond to a meaningful business occurrence.
	//
	// Example:
	//   type UserCreated struct {
	//       UserID   ID
	//       Username string
	//       Email    string
	//   }
	Event any

	// EventPack is a slice of events raised during command execution.
	// Commands return EventPack to allow the application layer to react to domain events.
	//
	// Example:
	//   events, err := session.ProcessRequest(now)
	//   if err != nil {
	//       return err
	//   }
	//   refreshEvent, err := EventOfType[RefreshQueued](events)
	//   if err == nil {
	//       // Queue refresh operation
	//   }
	EventPack []Event

	// ID is the unique identifier for an aggregate.
	// It is a string type to allow flexibility in ID generation strategies.
	//
	// Example:
	//   sessionID := ID(uuid.New().String())
	//   session.Initialize(sessionID, SessionCreated{...})
	ID string

	// Version represents the aggregate's version number, used for optimistic concurrency control.
	// Versions start at 0 and increment with each successful save operation.
	//
	// Example:
	//   version := agg.Version()
	//   nextVersion := version.Next()
	Version uint64
)

// Next returns the next version number, incrementing by 1.
// This is useful for version comparisons and optimistic concurrency control.
//
// Example:
//
//	currentVersion := agg.Version()
//	expectedNextVersion := currentVersion.Next()
func (v Version) Next() Version {
	return v + 1
}

// Tombstone is a special domain event that marks an aggregate for deletion.
// When raised, it indicates that the aggregate should be removed from storage.
// The aggregate's state should handle this event in its Apply method.
//
// Example:
//
//	func (s *SessionState) Apply(event Event) {
//	    switch e := event.(type) {
//	    case SessionCreated:
//	        // Initialize state
//	    case Tombstone:
//	        // Aggregate is being deleted
//	    default:
//	        PanicUnsupportedEvent(event)
//	    }
//	}
//
// To remove an aggregate, use the Remove method:
//
//	events, err := agg.Remove()
type Tombstone struct {
}

// EventRiser provides methods to raise domain events within command handlers.
// It is passed to command handlers via ProcessCommand to allow event-driven state mutations.
//
// Example:
//
//	func (s *Session) CompleteAuthorizationCodeFlow(...) (EventPack, error) {
//	    return s.ProcessCommand(func(state *SessionState, er EventRiser) error {
//	        // Validate business rules
//	        if state.Nonce != expectedNonce {
//	            return fmt.Errorf("invalid nonce")
//	        }
//	        // Raise event to update state
//	        er.Raise(TokensReceived{
//	            AccessToken: accessToken,
//	            RefreshToken: refreshToken,
//	        })
//	        return nil
//	    })
//	}
type EventRiser interface {
	// Raise raises a single domain event.
	// The event will be applied to the aggregate's state and included in the returned EventPack.
	Raise(event Event)

	// RaisePack raises multiple events at once.
	// This is useful when a command needs to raise several related events.
	//
	// Example:
	//   er.RaisePack(EventPack{
	//       UserCreated{UserID: id},
	//       WelcomeEmailQueued{UserID: id},
	//   })
	RaisePack(pack EventPack)

	// RaiseNotEqual raises an event if two values are not equal.
	// This is useful for conditional event raising based on state changes.
	//
	// Example:
	//   er.RaiseNotEqual(oldValue, newValue, ValueChanged{NewValue: newValue})
	RaiseNotEqual(first any, second any, event Event)

	// RaiseTrue raises an event if the predicate is true.
	// This is useful for conditional event raising.
	//
	// Example:
	//   er.RaiseTrue(shouldRefresh, RefreshQueued{RefreshToken: token})
	RaiseTrue(predicate bool, event Event)
}

// EventApplier is implemented by aggregate state types to handle domain events.
// The Apply method mutates the state based on the event type.
//
// Example:
//
//	type SessionState struct {
//	    Status SessionStatus
//	    AccessToken AccessToken
//	}
//
//	func (s *SessionState) Apply(event Event) {
//	    switch e := event.(type) {
//	    case SessionCreated:
//	        s.Status = statusPending
//	    case TokensReceived:
//	        s.AccessToken = e.AccessToken
//	        s.Status = statusAuthenticated
//	    case RefreshQueued:
//	        s.Status = statusRefreshOngoing
//	    case Tombstone:
//	        // Handle deletion
//	    default:
//	        PanicUnsupportedEvent(event)
//	    }
//	}
//
// IMPORTANT: State should only be mutated via events. Events are used for change tracking,
// and the repository will skip saving aggregates with no events.
type EventApplier interface {
	// Apply mutates the state based on the provided event.
	// All state changes must happen through this method.
	Apply(event Event)
}

type raiser[T Event] struct {
	a *Aggregate[T]
}

func (r *raiser[T]) Raise(event Event) {
	r.a.raise(event)
}

func (r *raiser[T]) RaisePack(pack EventPack) {
	for _, e := range pack {
		r.Raise(e)
	}
}

func (r *raiser[T]) RaiseNotEqual(first any, second any, event Event) {
	if !cmp.Equal(first, second) {
		r.Raise(event)
	}
}

func (r *raiser[T]) RaiseTrue(predicate bool, event Event) {
	if predicate {
		r.Raise(event)
	}
}

// Aggregate is the core building block for domain-driven design.
// It encapsulates business logic through commands and manages state through events.
//
// Type parameter T is the state type, which must implement EventApplier.
//
// Example aggregate definition:
//
//	type SessionState struct {
//	    Status SessionStatus
//	    AccessToken AccessToken
//	}
//
//	func (s *SessionState) Apply(event Event) {
//	    switch e := event.(type) {
//	    case SessionCreated:
//	        s.Status = statusPending
//	    case TokensReceived:
//	        s.AccessToken = e.AccessToken
//	        s.Status = statusAuthenticated
//	    default:
//	        PanicUnsupportedEvent(event)
//	    }
//	}
//
//	type Session struct {
//	    core.Aggregate[SessionState]
//	}
//
//	func NewSession(...) *Session {
//	    agg := &Session{}
//	    agg.Initialize(sessionID, SessionCreated{...})
//	    return agg
//	}
//
//	func (s *Session) CompleteAuthorizationCodeFlow(...) (EventPack, error) {
//	    return s.ProcessCommand(func(state *SessionState, er EventRiser) error {
//	        // Business logic and validation
//	        er.Raise(TokensReceived{...})
//	        return nil
//	    })
//	}
//
// IMPORTANT: If ProcessCommand returns an error, the aggregate is marked as corrupted
// and cannot be used anymore. All subsequent method calls will panic.
type Aggregate[T State] struct {
	err     error
	state   T
	raiser  raiser[T]
	id      ID
	events  EventPack
	version Version
}

func (a *Aggregate[T]) checkError() {
	if a.err != nil {
		panic("aggregate state corrupted")
	}
}

func (a *Aggregate[T]) raise(event Event) {
	applier, ok := any(&a.state).(EventApplier)
	if !ok {
		panic("state must implement EventApplier")
	}
	applier.Apply(event)
	a.events = append(a.events, event)
}

// ProcessCommand executes a command handler that can mutate state through events.
// Commands receive the current state and an EventRiser to raise domain events.
//
// The handler should:
//   - Use the state parameter only for reading current state
//   - Raise events via EventRiser to mutate state
//   - Return an error if business rules are violated
//
// If the handler returns an error, the aggregate is marked as corrupted and all
// subsequent operations will panic. Events raised before the error are discarded.
//
// Returns the events raised during this command execution and any error that occurred.
//
// Example:
//
//	func (s *Session) ProcessRequest(now Timestamp) (ProcessRequestResult, EventPack, error) {
//	    var result ProcessRequestResult
//	    events, err := s.ProcessCommand(func(state *SessionState, er EventRiser) error {
//	        if state.Status != statusAuthenticated {
//	            return fmt.Errorf("session not authenticated")
//	        }
//	        result.AccessToken = state.AccessToken
//	        if s.shouldStartRefresh(now) {
//	            er.Raise(RefreshQueued{RefreshToken: state.RefreshToken, At: now})
//	        }
//	        return nil
//	    })
//	    return result, events, err
//	}
func (a *Aggregate[T]) ProcessCommand(handler func(state *T, er EventRiser) error) (EventPack, error) {
	a.checkError()
	eventsCount := len(a.events)
	a.raiser = raiser[T]{a}
	err := handler(&a.state, &a.raiser)
	if err != nil {
		a.err = err
		a.events = nil
		return nil, err
	}
	return a.events[eventsCount:], nil
}

// ID returns the aggregate's unique identifier.
// Panics if the aggregate is in a corrupted state.
//
// Example:
//
//	sessionID := session.ID()
func (a *Aggregate[T]) ID() ID {
	a.checkError()
	return a.id
}

// State returns a copy of the aggregate's current state.
// Use this method to read state, never modify the returned state directly.
// Panics if the aggregate is in a corrupted state.
//
// Example:
//
//	state := session.State()
//	if state.Status == statusAuthenticated {
//	    // Use authenticated state
//	}
func (a *Aggregate[T]) State() T {
	a.checkError()
	return a.state
}

// Initialize sets up the aggregate with an initial state by raising a creation event.
// This must be called exactly once in the aggregate's constructor before any other operations.
// Panics if called on an already initialized aggregate.
//
// The created event should represent the initial state of the aggregate and will be
// applied to set up the initial state through the Apply method.
//
// Example:
//
//	func NewSession(nonce Nonce, redirectURL RedirectURL) *Session {
//	    agg := &Session{}
//	    sessionID := ID(uuid.New().String())
//	    agg.Initialize(sessionID, SessionCreated{
//	        Nonce:       nonce,
//	        RedirectURL: redirectURL,
//	    })
//	    return agg
//	}
func (a *Aggregate[T]) Initialize(id ID, created Event) {
	if a.version > 0 || len(a.events) > 0 {
		panic(errAggregateAlreadyInitialized)
	}
	a.id = id
	a.version = 0
	var state T
	a.state = state
	a.events = nil
	a.err = nil
	a.raise(created)
}

// Remove marks the aggregate for deletion by raising a Tombstone event.
// The aggregate should handle this event in its Apply method.
// When saved, repositories will delete the aggregate from storage.
//
// Returns the event pack containing the Tombstone event and any error.
//
// Example:
//
//	events, err := session.Remove()
//	if err != nil {
//	    return err
//	}
//	// Save the aggregate to persist the deletion
//	err = repo.Save(ctx, session)
func (a *Aggregate[T]) Remove() (EventPack, error) {
	return a.ProcessCommand(func(_ *T, er EventRiser) error {
		er.Raise(Tombstone{})
		return nil
	})
}

// Store persists the aggregate's state and events to storage.
// This is called by repository implementations and should not be called directly.
//
// If there are no pending events, Store returns immediately without calling storeFunc.
// This optimization allows repositories to skip unnecessary save operations.
//
// After a successful store:
//   - Events are cleared
//   - Version is incremented
//
// If the state implements StateStorer, it will be used to provide custom storage logic
// and schema version information.
//
// Example (repository implementation):
//
//	err := aggregate.Store(func(id ID, aggregate AggregatePtr, state StatePtr, events EventPack, version Version, schemaVersion SchemaVersion) error {
//	    // Persist to storage backend
//	    return storage.Save(id, state, events, version, schemaVersion)
//	})
func (a *Aggregate[T]) Store(storeFunc func(ID, AggregatePtr, StatePtr, EventPack, Version, SchemaVersion) error) error {
	a.checkError()
	if len(a.events) == 0 {
		return nil
	}
	aggregatePtr := AggregatePtr(a)
	stateStorer, ok := any(&a.state).(StateStorer)
	if ok {
		if err := stateStorer.Store(func(state StatePtr, schemaVersion SchemaVersion) error {
			return storeFunc(a.id, aggregatePtr, state, a.events, a.version, schemaVersion)
		}); err != nil {
			return err
		}
	} else {
		if err := storeFunc(a.id, aggregatePtr, &a.state, a.events, a.version, DefaultSchemaVersion); err != nil {
			return err
		}
	}
	a.events = nil
	a.version++
	return nil
}

// Restore loads the aggregate's state from storage.
// This is called by repository implementations and should not be called directly.
//
// After restoration:
//   - Events are cleared
//   - Error state is cleared
//   - ID and version are set from storage
//
// If the state implements StateRestorer, it will be used to handle schema versioning
// and custom restoration logic.
//
// Example (repository implementation):
//
//	err := aggregate.Restore(id, version, schemaVersion, func(state StatePtr) error {
//	    // Load state from storage backend
//	    return storage.Load(id, state)
//	})
func (a *Aggregate[TState]) Restore(id ID, version Version, schemaVersion SchemaVersion, restoreFunc func(state StatePtr) error) error {
	a.id = id
	a.version = version
	restorer, ok := any(&a.state).(StateRestorer)
	if ok {
		if err := restorer.Restore(schemaVersion, restoreFunc); err != nil {
			return err
		}
	} else {
		if err := restoreFunc(&a.state); err != nil {
			return err
		}
	}
	a.events = nil
	a.err = nil
	return nil
}

// Error returns the error that caused the aggregate to become corrupted, if any.
// Returns nil if the aggregate is in a valid state.
//
// Once an aggregate has an error, it cannot be used for further operations.
// All subsequent method calls (except Error) will panic.
//
// Example:
//
//	if err := agg.Error(); err != nil {
//	    // Aggregate is corrupted, cannot use it
//	    return err
//	}
func (a *Aggregate[TState]) Error() error {
	return a.err
}

// Version returns the aggregate's current version number.
// Versions start at 0 and increment with each successful save operation.
// Panics if the aggregate is in a corrupted state.
//
// Example:
//
//	version := session.Version()
//	if version > 0 {
//	    // Aggregate has been persisted
//	}
func (a *Aggregate[T]) Version() Version {
	a.checkError()
	return a.version
}

// Events returns all events raised since the last save operation.
// Panics if the aggregate is in a corrupted state.
//
// Example:
//
//	events := session.Events()
//	for _, event := range events {
//	    // Process events for side effects
//	}
func (a *Aggregate[T]) Events() EventPack {
	a.checkError()
	return a.events
}

// PanicUnsupportedEvent panics with a message indicating an unsupported event type.
// This should be called in the default case of an Apply method's type switch
// to ensure all event types are handled.
//
// Example:
//
//	func (s *SessionState) Apply(event Event) {
//	    switch e := event.(type) {
//	    case SessionCreated:
//	        // Handle creation
//	    case TokensReceived:
//	        // Handle tokens
//	    default:
//	        PanicUnsupportedEvent(event)
//	    }
//	}
func PanicUnsupportedEvent(event Event) {
	panic(fmt.Sprintf("unsupported event %T", event))
}

// EventOfType extracts a single event of the specified type from an event pack.
// Returns ErrNoEvents if no events of the type are found.
// Returns ErrTooManyEvents if multiple events of the type are found.
//
// Use this when you expect exactly one event of a specific type.
// For multiple events, use EventsOfType instead.
//
// Example:
//
//	events, err := session.ProcessRequest(now)
//	if err != nil {
//	    return err
//	}
//	refreshEvent, err := EventOfType[RefreshQueued](events)
//	if err == nil {
//	    // Queue refresh operation with refreshEvent
//	} else if err == ErrNoEvents {
//	    // No refresh needed
//	}
func EventOfType[T any](pack EventPack) (T, error) {
	e := EventsOfType[T](pack)
	var evt T
	if len(e) == 0 {
		return evt, ErrNoEvents
	} else if len(e) > 1 {
		return evt, ErrTooManyEvents
	} else {
		return e[0], nil
	}
}

// EventsOfType extracts all events of the specified type from an event pack.
// Returns an empty slice if no events of the type are found.
//
// Use this when you need to handle multiple events of the same type.
// For a single event, use EventOfType instead.
//
// Example:
//
//	events, err := session.ProcessRequest(now)
//	if err != nil {
//	    return err
//	}
//	refreshEvents := EventsOfType[RefreshQueued](events)
//	for _, refreshEvent := range refreshEvents {
//	    // Process each refresh event
//	}
func EventsOfType[T any](pack EventPack) []T {
	res := make([]T, 0)
	for _, e := range pack {
		switch evt := e.(type) {
		case T:
			res = append(res, evt)
		}
	}
	return res
}

// IsEmpty checks if an event pack contains no events.
//
// Example:
//
//	events, err := session.ProcessRequest(now)
//	if err != nil {
//	    return err
//	}
//	if IsEmpty(events) {
//	    // No events raised, nothing to process
//	}
func IsEmpty(pack EventPack) bool {
	return len(pack) == 0
}
