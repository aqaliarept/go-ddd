package test

import (
	"errors"
	"fmt"
	"strings"

	core "github.com/aqaliarept/go-ddd-kit/pkg/core"
	mongopkg "github.com/aqaliarept/go-ddd-kit/pkg/mongo"
	postgrespkg "github.com/aqaliarept/go-ddd-kit/pkg/postgres"
	redispkg "github.com/aqaliarept/go-ddd-kit/pkg/redis"
	sqlitepkg "github.com/aqaliarept/go-ddd-kit/pkg/sqlite"
)

var (
	errUnsupportedSchemaVersion = errors.New("unsupported schema version")
	errMigratedStateEmpty       = errors.New("migrated state from V1 has empty ValueV1")
)

// StateV1 represents version 1 of the test state for schema migration testing
type StateV1 struct {
	ValueV1 string
	value   string
}

func (s *StateV1) Apply(event core.Event) {
	switch e := event.(type) {
	case Created:
		s.ValueV1 = "version 1"
	case ValueUpdated:
		s.ValueV1 = e.Value
	default:
		core.PanicUnsupportedEvent(event)
	}
}

// Restore implements StateRestorer interface
// The private 'value' field is used to test internal state transformations during restoration
func (s *StateV1) Restore(schemaVersion core.SchemaVersion, restoreFunc func(state core.StatePtr) error) error {
		switch schemaVersion {
		case core.DefaultSchemaVersion:
			err := restoreFunc(s)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("%w: %d", errUnsupportedSchemaVersion, schemaVersion)
		}
	s.value = strings.ToUpper(s.ValueV1)
	return nil
}

func (s *StateTestAggV1) SingleEventCommand(val string) (core.EventPack, error) {
	return s.ProcessCommand(func(state *StateV1, er core.EventRiser) error {
		er.Raise(ValueUpdated{Value: val})
		return nil
	})
}

// StateV2SchemaVersion is the schema version constant for StateV2
const StateV2SchemaVersion = core.SchemaVersion(2)

// StateV2 represents version 2 of the test state for schema migration testing

var _ core.StateRestorer = (*StateV2)(nil)
var _ core.StateStorer = (*StateV2)(nil)

// StateV2 represents version 2 of the test state for schema migration testing
type StateV2 struct {
	ValueV2 string
	value   string
}

func (s *StateV2) Apply(event core.Event) {
	switch e := event.(type) {
	case Created:
		s.ValueV2 = "version 2"
	case ValueUpdated:
		s.ValueV2 = e.Value
	default:
		core.PanicUnsupportedEvent(event)
	}
}

func (s *StateTestAggV2) SingleEventCommand(val string) (core.EventPack, error) {
	return s.ProcessCommand(func(state *StateV2, er core.EventRiser) error {
		er.Raise(ValueUpdated{Value: val})
		return nil
	})
}

// Restore implements StateRestorer interface
// The private 'value' field is used to test internal state transformations during restoration
func (s *StateV2) Restore(schemaVersion core.SchemaVersion, restoreFunc func(state core.StatePtr) error) error {
	switch schemaVersion {
	case core.DefaultSchemaVersion:
		stateV1 := &StateV1{}
		err := restoreFunc(stateV1)
		if err != nil {
			return err
		}
		if stateV1.ValueV1 == "" {
			return errMigratedStateEmpty
		}
		s.ValueV2 = stateV1.ValueV1
	case StateV2SchemaVersion:
		err := restoreFunc(s)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("%w: %d", errUnsupportedSchemaVersion, schemaVersion)
	}
	s.value = strings.ToUpper(s.ValueV2)
	return nil
}

// Store implements StateStorer interface
func (s *StateV2) Store(storeFunc func(state core.StatePtr, schemaVersion core.SchemaVersion) error) error {
	return storeFunc(s, StateV2SchemaVersion)
}

// StateTestAggV1 is a test aggregate using StateV1 schema version
type StateTestAggV1 struct {
	core.Aggregate[StateV1]
}

// StorageOptions returns the storage options for this aggregate
func (s *StateTestAggV1) StorageOptions() []core.StorageOption {
	return []core.StorageOption{
		mongopkg.WithCollectionName("test_agg"),
		postgrespkg.WithTableName("test_agg"),
		redispkg.WithNamespace("test_agg"),
		sqlitepkg.WithTableName("test_agg"),
	}
}

func NewStateTestAggV1(id core.ID) *StateTestAggV1 {
	agg := &StateTestAggV1{}
	agg.Initialize(id, Created{})
	return agg
}

// StateTestAggV2 is a test aggregate using StateV2 schema version
type StateTestAggV2 struct {
	core.Aggregate[StateV2]
}

// StorageOptions returns the storage options for this aggregate
func (s *StateTestAggV2) StorageOptions() []core.StorageOption {
	return []core.StorageOption{
		mongopkg.WithCollectionName("test_agg"),
		postgrespkg.WithTableName("test_agg"),
		redispkg.WithNamespace("test_agg"),
		sqlitepkg.WithTableName("test_agg"),
	}
}

func NewStateTestAggV2(id core.ID) *StateTestAggV2 {
	agg := &StateTestAggV2{}
	agg.Initialize(id, Created{})
	return agg
}
