//go:build !integration

package core

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	errGuardError        = errors.New("guard error")
	errTestError         = errors.New("error")
	errInvalidStateType  = errors.New("invalid state type")
	errRestoreFailed     = errors.New("restore failed")
	errStateRestorerFailed = errors.New("state restorer failed")
)

var _ EventApplier = &testAggState{}
var _ Storer = &testAgg{}
var _ Restorer = &testAgg{}

type nestedEntity struct {
	MyMap    map[string]string
	MyString string
	MySlice  []string
}

type testAggState struct {
	MyMap    map[string]nestedEntity
	MyString string
	MySlice  []nestedEntity
	Removed  bool
}

func newTestAgg(id ID) *testAgg {
	agg := testAgg{}
	agg.Initialize(id, Created{})
	return &agg
}

type Created struct {
}

type ValueUpdated struct {
	value string
}

func (t *testAggState) Apply(event Event) {
	switch e := event.(type) {
	case Created:
		t.MySlice = make([]nestedEntity, 0)
		t.MyString = "created"
	case ValueUpdated:
		t.MyString = e.value
	case Tombstone:
		t.Removed = true
	default:
		PanicUnsupportedEvent(event)
	}
}

type testAgg struct {
	Aggregate[testAggState]
}

func (t *testAgg) StorageOptions() []StorageOption {
	return []StorageOption{}
}

const (
	guardErrorValue = "guard_error"
)

func (t *testAgg) SingleEventCommand(val string) (EventPack, error) {
	return t.ProcessCommand(func(s *testAggState, er EventRiser) error {
		if val == guardErrorValue {
			return errGuardError
		}
		er.Raise(ValueUpdated{val})
		return nil
	})
}

func (t *testAgg) MultipleEventsCommand(val string) (EventPack, error) {
	return t.ProcessCommand(func(s *testAggState, er EventRiser) error {
		evt := ValueUpdated{val}
		er.RaiseTrue(true, evt)
		if val == guardErrorValue {
			return errGuardError
		}
		return nil
	})
}

//nolint:errcheck
func (t *testAgg) SetError(err error) {
	_, _ = t.ProcessCommand(func(s *testAggState, er EventRiser) error {
		return err
	})
}

func TestAggregate(t *testing.T) {
	t.Run(`Given an aggregate
		When Remove is called
		Then Tombstone event is produced
		And state Removed set to true
	`, func(t *testing.T) {
		agg := newTestAgg("id")
		pack, err := agg.Remove()
		require.NoError(t, err)
		require.Equal(t, EventPack{Tombstone{}}, pack)
		require.True(t, agg.State().Removed)
	})

	t.Run(`Given aggregate with non-zero version
			And with non-empty state
			And events are not empty
			And error is not empty
			When Initialize is called
			Then reset state to after Created event is applied
			And events to Created
			And version to zero
			And error to nil		`, func(t *testing.T) {
		agg := testAgg{}
		agg.id = "id"
		agg.state.MyMap = map[string]nestedEntity{"val": {}}
		agg.events = append(agg.events, ValueUpdated{"val"})
		agg.SetError(errTestError)

		newID := ID("new-id")
		agg.Initialize(newID, Created{})

		require.Equal(t, Version(0), agg.version)
		require.Equal(t, 1, len(agg.events))
		require.Equal(t, testAggState{MyString: "created", MySlice: make([]nestedEntity, 0)}, agg.state)
		require.Nil(t, agg.Error())
	})

	t.Run(`Given a newly created aggregate
		When the allowed command is called
		Then event is produced
		And state is updated`, func(t *testing.T) {

		agg := newTestAgg("id")
		const val = "allowed_value"
		pack, err := agg.SingleEventCommand(val)
		require.NoError(t, err)
		evt, err := EventOfType[ValueUpdated](pack)
		require.NoError(t, err)
		require.Equal(t, ValueUpdated{val}, evt)
		require.Equal(t, val, agg.State().MyString)
	})

	t.Run(`Given a newly created aggregate
		When command is called and guard statement fails
		And the command hasn't produced events before the failure
		Then the error is returned
		And the aggregate is in invalid state`, func(t *testing.T) {
		agg := newTestAgg("id")
		pack, err := agg.SingleEventCommand(guardErrorValue)
		require.Error(t, err)
		require.True(t, IsEmpty(pack))
		require.NotNil(t, agg.Error())
		require.ErrorIs(t, agg.Error(), err)
	})

	t.Run(`Given a newly created aggregate
		When command is called and guard statement fails
		And the command has produced event before the failure
		Then the error is returned
		And the aggregate is in invalid state`, func(t *testing.T) {
		agg := newTestAgg("id")
		pack, err := agg.MultipleEventsCommand(guardErrorValue)
		require.Error(t, err)
		require.True(t, IsEmpty(pack))
		require.NotNil(t, agg.Error())
	})

	t.Run(`Given a newly created aggregate
		When Store is called
		And persistFunc doesn't return an error
		Then should pass correct valued into persistFunc
		And increment version
		And cleanup events
	`, func(t *testing.T) {
		agg := newTestAgg("id")
		var pState *testAggState
		var pEventPack EventPack
		var pVersion Version
		err := agg.Store(func(id ID, aggregate AggregatePtr, storageState StatePtr, ep EventPack, v Version, sv SchemaVersion) error {
			var ok bool
			pState, ok = storageState.(*testAggState)
			if !ok {
				return errInvalidStateType
			}
			pEventPack = ep
			pVersion = v
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, testAggState{MyString: "created", MySlice: make([]nestedEntity, 0)}, *pState)
		require.Equal(t, EventPack{Created{}}, pEventPack)
		require.Equal(t, Version(0), pVersion)
		require.Empty(t, agg.events)
		require.Equal(t, Version(1), agg.version)
		require.Equal(t, testAggState{MyString: "created", MySlice: make([]nestedEntity, 0)}, agg.State())
	})

	t.Run(`Given an aggregate without events
		When Store is called
		Then persistFunc shouldn't be called
		And aggregate version shouldn't be changed
	`, func(t *testing.T) {
		agg := testAgg{}
		persistFuncCalled := false
		err := agg.Store(func(id ID, aggregate AggregatePtr, storageState StatePtr, ep EventPack, v Version, sv SchemaVersion) error {
			persistFuncCalled = true
			return nil
		})
		require.NoError(t, err)
		require.False(t, persistFuncCalled)
		require.Equal(t, Version(0), agg.version)
	})

	t.Run(`Given a newly created aggregate
		When Store is called
		And persistFunc returns an error
		Then aggregate's state shouldn't be changed
	`, func(t *testing.T) {
		agg := newTestAgg("id")
		err := agg.Store(func(id ID, aggregate AggregatePtr, storageState StatePtr, ep EventPack, v Version, sv SchemaVersion) error {
			return errTestError
		})
		require.Error(t, err)
		require.Equal(t, testAggState{MyString: "created", MySlice: make([]nestedEntity, 0)}, agg.State())
		require.Equal(t, EventPack{Created{}}, agg.events)
		require.Equal(t, Version(0), agg.version)
	})

	t.Run(`Given an empty aggregate
			When Restore is called
			Then aggregate's state is restored from params of Restore
		`, func(t *testing.T) {
		agg := testAgg{}
		id := ID("id")
		state := testAggState{MyString: "created", MySlice: make([]nestedEntity, 0)}
		err := agg.Restore(id, Version(100), DefaultSchemaVersion, func(state StatePtr) error {
			s, ok := state.(*testAggState)
			if !ok {
				t.Fatalf("invalid state type")
			}
			s.MyString = "created"
			s.MySlice = make([]nestedEntity, 0)
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, state, agg.State())
		require.Empty(t, agg.events)
		require.Equal(t, Version(100), agg.version)
	})

	t.Run(`Given a newly created aggregate
		And Error is not nil
		When Restore is called
		Then aggregate's state is restored from params of Restore
		And Error is set to nil
	
		`, func(t *testing.T) {
		id := ID("id")
		agg := newTestAgg("id2")
		agg.SetError(errTestError)
		state := testAggState{MyString: "created", MySlice: make([]nestedEntity, 0)}
		err := agg.Restore(id, Version(100), DefaultSchemaVersion, func(state StatePtr) error {
			s, ok := state.(*testAggState)
			if !ok {
				t.Fatalf("invalid state type")
			}
			s.MyString = "created"
			s.MySlice = make([]nestedEntity, 0)
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, id, agg.ID())
		require.Equal(t, state, agg.State())
		require.Empty(t, agg.events)
		require.Equal(t, Version(100), agg.version)
		require.NoError(t, agg.Error())
	})

	t.Run(`Given an aggregate
		When Restore is called
		And restoreFunc returns an error
		Then the error should be returned
		And aggregate state should not be restored
	`, func(t *testing.T) {
		agg := testAgg{}
		agg.id = "original-id"
		agg.version = Version(10)
		err := agg.Restore("new-id", Version(100), DefaultSchemaVersion, func(state StatePtr) error {
			return errRestoreFailed
		})
		require.Error(t, err)
		require.ErrorIs(t, err, errRestoreFailed)
		require.Equal(t, ID("new-id"), agg.id)
		require.Equal(t, Version(100), agg.version)
	})
}

func TestVersionNext(t *testing.T) {
	t.Run(`Given a Version value
		When Next() is called
		Then it should return the incremented version
	`, func(t *testing.T) {
		testCases := []struct {
			name     string
			version  Version
			expected Version
		}{
			{"zero version", Version(0), Version(1)},
			{"version 1", Version(1), Version(2)},
			{"version 100", Version(100), Version(101)},
			{"max uint64", Version(18446744073709551615), Version(0)},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result := tc.version.Next()
				require.Equal(t, tc.expected, result)
			})
		}
	})
}

func TestRaiserRaisePack(t *testing.T) {
	t.Run(`Given an aggregate
		When RaisePack is called with multiple events
		Then all events should be raised
		And state should reflect all events
	`, func(t *testing.T) {
		agg := newTestAgg("id")
		pack, err := agg.ProcessCommand(func(s *testAggState, er EventRiser) error {
			er.RaisePack(EventPack{
				ValueUpdated{"first"},
				ValueUpdated{"second"},
				ValueUpdated{"third"},
			})
			return nil
		})
		require.NoError(t, err)
		require.Len(t, pack, 3)
		require.Equal(t, "third", agg.State().MyString)
	})
}

func TestRaiserRaiseNotEqual(t *testing.T) {
	t.Run(`Given an aggregate
		When RaiseNotEqual is called with different values
		Then event should be raised
	`, func(t *testing.T) {
		agg := newTestAgg("id")
		pack, err := agg.ProcessCommand(func(s *testAggState, er EventRiser) error {
			er.RaiseNotEqual("value1", "value2", ValueUpdated{"different"})
			return nil
		})
		require.NoError(t, err)
		require.Len(t, pack, 1)
		require.Equal(t, "different", agg.State().MyString)
	})

	t.Run(`Given an aggregate
		When RaiseNotEqual is called with equal values
		Then event should not be raised
	`, func(t *testing.T) {
		agg := newTestAgg("id")
		pack, err := agg.ProcessCommand(func(s *testAggState, er EventRiser) error {
			er.RaiseNotEqual("value", "value", ValueUpdated{"should not appear"})
			return nil
		})
		require.NoError(t, err)
		require.Empty(t, pack)
		require.Equal(t, "created", agg.State().MyString)
	})
}

func TestAggregateCheckErrorPanic(t *testing.T) {
	t.Run(`Given an aggregate with an error
		When any method that calls checkError is called
		Then it should panic
	`, func(t *testing.T) {
		agg := newTestAgg("id")
		agg.SetError(errTestError)

		require.PanicsWithValue(t, "aggregate state corrupted", func() {
			_ = agg.ID()
		})

		require.PanicsWithValue(t, "aggregate state corrupted", func() {
			_ = agg.State()
		})

		require.PanicsWithValue(t, "aggregate state corrupted", func() {
			_ = agg.Version()
		})

		require.PanicsWithValue(t, "aggregate state corrupted", func() {
			_ = agg.Events()
		})

		require.PanicsWithValue(t, "aggregate state corrupted", func() {
			_, err := agg.ProcessCommand(func(s *testAggState, er EventRiser) error {
				return nil
			})
			require.NoError(t, err)
		})

		require.PanicsWithValue(t, "aggregate state corrupted", func() {
			err := agg.Store(func(id ID, aggregate AggregatePtr, storageState StatePtr, ep EventPack, v Version, sv SchemaVersion) error {
				return nil
			})
			require.NoError(t, err)
		})
	})
}

type nonEventApplierState struct {
	Value string
}

func TestAggregateRaisePanic(t *testing.T) {
	t.Run(`Given an aggregate with state that doesn't implement EventApplier
		When an event is raised
		Then it should panic
	`, func(t *testing.T) {
		agg := Aggregate[nonEventApplierState]{}
		agg.id = "id"
		agg.version = 0
		agg.state = nonEventApplierState{Value: "test"}

		require.PanicsWithValue(t, "state must implement EventApplier", func() {
			agg.Initialize("id", Created{})
		})
	})
}

func TestAggregateInitializePanic(t *testing.T) {
	t.Run(`Given an aggregate that is already initialized
		When Initialize is called again
		Then it should panic
	`, func(t *testing.T) {
		agg := newTestAgg("id")
		agg.version = 1

		require.PanicsWithError(t, "aggregate is already initialized", func() {
			agg.Initialize("new-id", Created{})
		})
	})
}

type stateStorerTestState struct {
	Value string
}

func (s *stateStorerTestState) Apply(event Event) {
	switch e := event.(type) {
	case Created:
		s.Value = "created"
	case ValueUpdated:
		s.Value = e.value
	}
}

func (s *stateStorerTestState) Store(storeFunc func(state StatePtr, schemaVersion SchemaVersion) error) error {
	return storeFunc(s, SchemaVersion(2))
}

type stateStorerTestAgg struct {
	Aggregate[stateStorerTestState]
}

func (t *stateStorerTestAgg) StorageOptions() []StorageOption {
	return []StorageOption{}
}

func TestAggregateStoreWithStateStorer(t *testing.T) {
	t.Run(`Given an aggregate with state implementing StateStorer
		When Store is called
		Then StateStorer.Store should be called
		And custom schema version should be used
	`, func(t *testing.T) {
		agg := stateStorerTestAgg{}
		agg.Initialize("id", Created{})

		var receivedState StatePtr
		var receivedSchemaVersion SchemaVersion
		err := agg.Store(func(id ID, aggregate AggregatePtr, storageState StatePtr, ep EventPack, v Version, sv SchemaVersion) error {
			receivedState = storageState
			receivedSchemaVersion = sv
			return nil
		})
		require.NoError(t, err)
		require.NotNil(t, receivedState)
		require.Equal(t, SchemaVersion(2), receivedSchemaVersion)
		require.Equal(t, Version(1), agg.version)
		require.Empty(t, agg.events)
	})

	t.Run(`Given an aggregate with state implementing StateStorer
		When Store is called
		And StateStorer.Store returns an error
		Then the error should be returned
		And aggregate state should not be changed
	`, func(t *testing.T) {
		agg := stateStorerTestAgg{}
		agg.Initialize("id", Created{})
		initialVersion := agg.version
		initialEvents := len(agg.events)

		err := agg.Store(func(id ID, aggregate AggregatePtr, storageState StatePtr, ep EventPack, v Version, sv SchemaVersion) error {
			return errStoreFailed
		})
		require.Error(t, err)
		require.ErrorIs(t, err, errStoreFailed)
		require.Equal(t, initialVersion, agg.version)
		require.Equal(t, initialEvents, len(agg.events))
	})
}

type stateRestorerTestState struct {
	Value string
}

func (s *stateRestorerTestState) Apply(event Event) {
	switch e := event.(type) {
	case Created:
		s.Value = "created"
	case ValueUpdated:
		s.Value = e.value
	}
}

func (s *stateRestorerTestState) Restore(schemaVersion SchemaVersion, restoreFunc func(state StatePtr) error) error {
	return restoreFunc(s)
}

type stateRestorerTestAgg struct {
	Aggregate[stateRestorerTestState]
}

func (t *stateRestorerTestAgg) StorageOptions() []StorageOption {
	return []StorageOption{}
}

func TestAggregateRestoreWithStateRestorer(t *testing.T) {
	t.Run(`Given an aggregate with state implementing StateRestorer
		When Restore is called
		Then StateRestorer.Restore should be called
	`, func(t *testing.T) {
		agg := stateRestorerTestAgg{}
		restoreCalled := false
		err := agg.Restore("id", Version(5), SchemaVersion(3), func(state StatePtr) error {
			restoreCalled = true
			s, ok := state.(*stateRestorerTestState)
			require.True(t, ok)
			s.Value = "restored"
			return nil
		})
		require.NoError(t, err)
		require.True(t, restoreCalled)
		require.Equal(t, "restored", agg.State().Value)
		require.Equal(t, Version(5), agg.version)
		require.Empty(t, agg.events)
	})

	t.Run(`Given an aggregate with state implementing StateRestorer
		When Restore is called
		And StateRestorer.Restore returns an error
		Then the error should be returned
		And aggregate state should not be restored
	`, func(t *testing.T) {
		agg := stateRestorerTestAgg{}
		agg.id = "original-id"
		agg.version = Version(10)
		err := agg.Restore("new-id", Version(100), SchemaVersion(3), func(state StatePtr) error {
			return errStateRestorerFailed
		})
		require.Error(t, err)
		require.ErrorIs(t, err, errStateRestorerFailed)
		require.Equal(t, ID("new-id"), agg.id)
		require.Equal(t, Version(100), agg.version)
	})
}

func TestAggregateVersion(t *testing.T) {
	t.Run(`Given an aggregate
		When Version is called
		Then it should return the current version
	`, func(t *testing.T) {
		agg := newTestAgg("id")
		require.Equal(t, Version(0), agg.Version())

		agg.version = 10
		require.Equal(t, Version(10), agg.Version())

		agg.version = 100
		require.Equal(t, Version(100), agg.Version())
	})
}

func TestAggregateEvents(t *testing.T) {
	t.Run(`Given an aggregate
		When Events is called
		Then it should return the current events
	`, func(t *testing.T) {
		agg := newTestAgg("id")
		events := agg.Events()
		require.Len(t, events, 1)
		require.IsType(t, Created{}, events[0])

		pack, err := agg.SingleEventCommand("test")
		require.NoError(t, err)
		require.Len(t, pack, 1)

		events = agg.Events()
		require.Len(t, events, 2)
		require.IsType(t, Created{}, events[0])
		require.IsType(t, ValueUpdated{}, events[1])
	})
}

func TestPanicUnsupportedEvent(t *testing.T) {
	t.Run(`Given an unsupported event type
		When PanicUnsupportedEvent is called
		Then it should panic with event type information
	`, func(t *testing.T) {
		require.PanicsWithValue(t, "unsupported event core.ValueUpdated", func() {
			PanicUnsupportedEvent(ValueUpdated{"test"})
		})

		require.PanicsWithValue(t, "unsupported event string", func() {
			PanicUnsupportedEvent("test event")
		})

		require.PanicsWithValue(t, "unsupported event int", func() {
			PanicUnsupportedEvent(42)
		})
	})
}

func TestEventOfTypeErrors(t *testing.T) {
	t.Run(`Given an empty event pack
		When EventOfType is called
		Then it should return ErrNoEvents
	`, func(t *testing.T) {
		pack := EventPack{}
		_, err := EventOfType[ValueUpdated](pack)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrNoEvents)
	})

	t.Run(`Given an event pack with multiple events of the same type
		When EventOfType is called
		Then it should return ErrTooManyEvents
	`, func(t *testing.T) {
		pack := EventPack{
			ValueUpdated{"first"},
			ValueUpdated{"second"},
			ValueUpdated{"third"},
		}
		_, err := EventOfType[ValueUpdated](pack)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrTooManyEvents)
	})

	t.Run(`Given an event pack with one event of the requested type
		When EventOfType is called
		Then it should return the event successfully
	`, func(t *testing.T) {
		pack := EventPack{
			Created{},
			ValueUpdated{"test"},
		}
		evt, err := EventOfType[ValueUpdated](pack)
		require.NoError(t, err)
		require.Equal(t, ValueUpdated{"test"}, evt)
	})
}

//go:noinline
func Restore(r Restorer) {
	if err := r.Restore("id", 100, DefaultSchemaVersion, func(state StatePtr) error {
		s, ok := state.(*testAggState)
		if !ok {
			panic("invalid state type")
		}
		s.MyString = "created"
		return nil
	}); err != nil {
		panic(err)
	}
}

func BenchmarkAggregate(b *testing.B) {

	b.Run("command allocations", func(b *testing.B) {
		b.ReportAllocs()
		agg := newTestAgg("id")
		for i := 0; i < b.N; i++ {
			_, err := agg.MultipleEventsCommand("val")
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}
		}
	})

	b.Run("command restore allocations", func(b *testing.B) {
		b.ReportAllocs()

		agg := newTestAgg("id")
		r := Restorer(agg)
		for i := 0; i < b.N; i++ {
			Restore(r)
		}
	})

	b.Run("command store allocations", func(b *testing.B) {
		b.ReportAllocs()
		agg := newTestAgg("id")
		for i := 0; i < b.N; i++ {
			err := agg.Store(func(i ID, aggregate AggregatePtr, storageState StatePtr, ep EventPack, v Version, sv SchemaVersion) error {
				_, ok := storageState.(*testAggState)
				if !ok {
					return errInvalidStateType
				}
				return nil
			})
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}
		}
	})

}
