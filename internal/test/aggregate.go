// Package test provides testing utilities and types for repository implementations.
package test

import (
	core "github.com/aqaliarept/go-ddd-kit/pkg/core"
	mongopkg "github.com/aqaliarept/go-ddd-kit/pkg/mongo"
	postgrespkg "github.com/aqaliarept/go-ddd-kit/pkg/postgres"
	redispkg "github.com/aqaliarept/go-ddd-kit/pkg/redis"
	sqlitepkg "github.com/aqaliarept/go-ddd-kit/pkg/sqlite"
)

// NestedEntity represents a nested entity in test state
type NestedEntity struct {
	Map         map[string]string
	String      string
	StringSlice []string
}

// TestAggState represents the state of a test aggregate
type TestAggState struct {
	Map         map[string]NestedEntity
	String      string
	EntitySlice []NestedEntity
	Removed     bool
}

func (t *TestAggState) Apply(event core.Event) {
	switch e := event.(type) {
	case Created:
		t.EntitySlice = make([]NestedEntity, 0)
		t.String = "created"
	case ValueUpdated:
		t.String = e.Value
	case core.Tombstone:
		t.Removed = true
	default:
		core.PanicUnsupportedEvent(event)
	}
}

// Created is the initial event emitted when a test aggregate is first created
type Created struct{}

// ValueUpdated is an event that updates the string value in the test aggregate state
type ValueUpdated struct {
	Value string
}

// TestAgg is the base test aggregate that can be embedded by repository-specific implementations
type TestAgg struct {
	core.Aggregate[TestAggState]
}

// StorageOptions returns the storage options for this aggregate
func (t *TestAgg) StorageOptions() []core.StorageOption {
	return []core.StorageOption{
		mongopkg.WithCollectionName("test_agg"),
		postgrespkg.WithTableName("test_agg"),
		redispkg.WithNamespace("test_agg"),
		sqlitepkg.WithTableName("test_agg"),
	}
}

// SingleEventCommand executes a command that generates a single event
func (t *TestAgg) SingleEventCommand(val string) (core.EventPack, error) {
	return t.ProcessCommand(func(s *TestAggState, er core.EventRiser) error {
		er.Raise(ValueUpdated{Value: val})
		return nil
	})
}

// NewTestAgg creates a new test aggregate with the given ID
func NewTestAgg(id core.ID) *TestAgg {
	agg := &TestAgg{}
	agg.Initialize(id, Created{})
	return agg
}

// StateTestAggState represents the state for state-only test aggregate
//
//nolint:govet
type StateTestAggState struct {
	Value    string
	Tags     []string
	Metadata map[string]string
	Number   int
	IsActive bool
}

// StateTestAgg is a test aggregate that implements StateRestorer and StateStorer interfaces.
// This aggregate is available for testing custom state restoration and storage logic,
// though it is not currently used in the main test suite.
type StateTestAgg struct {
	state StateTestAggState
}

var _ core.StateRestorer = (*StateTestAgg)(nil)
var _ core.StateStorer = (*StateTestAgg)(nil)

// Restore implements StateRestorer interface
func (s *StateTestAgg) Restore(schemaVersion core.SchemaVersion, restoreFunc func(state core.StatePtr) error) error {
	return restoreFunc(&s.state)
}

// Store implements StateStorer interface
func (s *StateTestAgg) Store(storeFunc func(state core.StatePtr, schemaVersion core.SchemaVersion) error) error {
	return storeFunc(&s.state, core.DefaultSchemaVersion)
}

// State returns the current state
func (s *StateTestAgg) State() StateTestAggState {
	return s.state
}

// SetState sets the state directly (for testing purposes)
func (s *StateTestAgg) SetState(state StateTestAggState) {
	s.state = state
}

// NewStateTestAgg creates a new state test aggregate with default values
func NewStateTestAgg() *StateTestAgg {
	return &StateTestAgg{
		state: StateTestAggState{
			Value:    "default",
			Number:   0,
			IsActive: false,
			Tags:     make([]string, 0),
			Metadata: make(map[string]string),
		},
	}
}
