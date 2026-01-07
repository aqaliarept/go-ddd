//go:build !integration

package core_test

import (
	"errors"
	"strconv"
	"testing"

	"github.com/aqaliarept/go-ddd-kit/pkg/core"
	"github.com/stretchr/testify/require"
)

var (
	errGuardError          = errors.New("guard error")
	errTestError           = errors.New("error")
	errInvalidStateType    = errors.New("invalid state type")
	errRestoreFailed       = errors.New("restore failed")
	errStateRestorerFailed = errors.New("state restorer failed")
)

var _ core.EventApplier = &testAggState{}
var _ core.Storer = &testAgg{}
var _ core.Restorer = &testAgg{}

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

func newTestAgg(id core.ID) *testAgg {
	agg := testAgg{}
	agg.Initialize(id, Created{})
	return &agg
}

type Created struct {
}

type ValueUpdated struct {
	value string
}

func (t *testAggState) Apply(event core.Event) {
	switch e := event.(type) {
	case Created:
		t.MySlice = make([]nestedEntity, 0)
		t.MyString = "created"
	case ValueUpdated:
		t.MyString = e.value
	case core.Tombstone:
		t.Removed = true
	default:
		core.PanicUnsupportedEvent(event)
	}
}

type testAgg struct {
	core.Aggregate[testAggState]
}

func (t *testAgg) StorageOptions() []core.StorageOption {
	return []core.StorageOption{}
}

const (
	guardErrorValue = "guard_error"
)

func (t *testAgg) SingleEventCommand(val string) (core.EventPack, error) {
	return t.ProcessCommand(func(s *testAggState, er core.EventRiser) error {
		if val == guardErrorValue {
			return errGuardError
		}
		er.Raise(ValueUpdated{val})
		return nil
	})
}

func (t *testAgg) MultipleEventsCommand(val string) (core.EventPack, error) {
	return t.ProcessCommand(func(s *testAggState, er core.EventRiser) error {
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
	_, _ = t.ProcessCommand(func(s *testAggState, er core.EventRiser) error {
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
		require.Equal(t, core.EventPack{core.Tombstone{}}, pack)
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
		agg.SetError(errTestError)

		newID := core.ID("new-id")
		agg.Initialize(newID, Created{})

		require.Equal(t, core.Version(0), agg.Version())
		require.Equal(t, 1, len(agg.Events()))
		require.Equal(t, testAggState{MyString: "created", MySlice: make([]nestedEntity, 0)}, agg.State())
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
		evt, err := core.EventOfType[ValueUpdated](pack)
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
		require.True(t, core.IsEmpty(pack))
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
		require.True(t, core.IsEmpty(pack))
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
		var pEventPack core.EventPack
		var pVersion core.Version
		err := agg.Store(func(id core.ID, aggregate core.AggregatePtr, storageState core.StatePtr, ep core.EventPack, v core.Version, sv core.SchemaVersion) error {
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
		require.Equal(t, core.EventPack{Created{}}, pEventPack)
		require.Equal(t, core.Version(0), pVersion)
		require.Empty(t, agg.Events())
		require.Equal(t, core.Version(1), agg.Version())
		require.Equal(t, testAggState{MyString: "created", MySlice: make([]nestedEntity, 0)}, agg.State())
	})

	t.Run(`Given an aggregate without events
		When Store is called
		Then persistFunc shouldn't be called
		And aggregate version shouldn't be changed
	`, func(t *testing.T) {
		agg := testAgg{}
		persistFuncCalled := false
		err := agg.Store(func(id core.ID, aggregate core.AggregatePtr, storageState core.StatePtr, ep core.EventPack, v core.Version, sv core.SchemaVersion) error {
			persistFuncCalled = true
			return nil
		})
		require.NoError(t, err)
		require.False(t, persistFuncCalled)
		require.Equal(t, core.Version(0), agg.Version())
	})

	t.Run(`Given a newly created aggregate
		When Store is called
		And persistFunc returns an error
		Then aggregate's state shouldn't be changed
	`, func(t *testing.T) {
		agg := newTestAgg("id")
		err := agg.Store(func(id core.ID, aggregate core.AggregatePtr, storageState core.StatePtr, ep core.EventPack, v core.Version, sv core.SchemaVersion) error {
			return errTestError
		})
		require.Error(t, err)
		require.Equal(t, testAggState{MyString: "created", MySlice: make([]nestedEntity, 0)}, agg.State())
		require.Equal(t, core.EventPack{Created{}}, agg.Events())
		require.Equal(t, core.Version(0), agg.Version())
	})

	t.Run(`Given an empty aggregate
			When Restore is called
			Then aggregate's state is restored from params of Restore
		`, func(t *testing.T) {
		agg := testAgg{}
		id := core.ID("id")
		state := testAggState{MyString: "created", MySlice: make([]nestedEntity, 0)}
		err := agg.Restore(id, core.Version(100), core.DefaultSchemaVersion, func(state core.StatePtr) error {
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
		require.Empty(t, agg.Events())
		require.Equal(t, core.Version(100), agg.Version())
	})

	t.Run(`Given a newly created aggregate
		And Error is not nil
		When Restore is called
		Then aggregate's state is restored from params of Restore
		And Error is set to nil
	
		`, func(t *testing.T) {
		id := core.ID("id")
		agg := newTestAgg("id2")
		agg.SetError(errTestError)
		state := testAggState{MyString: "created", MySlice: make([]nestedEntity, 0)}
		err := agg.Restore(id, core.Version(100), core.DefaultSchemaVersion, func(state core.StatePtr) error {
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
		require.Empty(t, agg.Events())
		require.Equal(t, core.Version(100), agg.Version())
		require.NoError(t, agg.Error())
	})

	t.Run(`Given an aggregate
		When Restore is called
		And restoreFunc returns an error
		Then the error should be returned
		And aggregate state should not be restored
	`, func(t *testing.T) {
		agg := testAgg{}
		err := agg.Restore("new-id", core.Version(100), core.DefaultSchemaVersion, func(state core.StatePtr) error {
			return errRestoreFailed
		})
		require.Error(t, err)
		require.ErrorIs(t, err, errRestoreFailed)
		require.Equal(t, core.ID("new-id"), agg.ID())
		require.Equal(t, core.Version(100), agg.Version())
	})
}

func TestVersionNext(t *testing.T) {
	t.Run(`Given a Version value
		When Next() is called
		Then it should return the incremented version
	`, func(t *testing.T) {
		testCases := []struct {
			name     string
			version  core.Version
			expected core.Version
		}{
			{"zero version", core.Version(0), core.Version(1)},
			{"version 1", core.Version(1), core.Version(2)},
			{"version 100", core.Version(100), core.Version(101)},
			{"max uint64", core.Version(18446744073709551615), core.Version(0)},
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
		pack, err := agg.ProcessCommand(func(s *testAggState, er core.EventRiser) error {
			er.RaisePack(core.EventPack{
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
		pack, err := agg.ProcessCommand(func(s *testAggState, er core.EventRiser) error {
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
		pack, err := agg.ProcessCommand(func(s *testAggState, er core.EventRiser) error {
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
			_, err := agg.ProcessCommand(func(s *testAggState, er core.EventRiser) error {
				return nil
			})
			require.NoError(t, err)
		})

		require.PanicsWithValue(t, "aggregate state corrupted", func() {
			err := agg.Store(func(id core.ID, aggregate core.AggregatePtr, storageState core.StatePtr, ep core.EventPack, v core.Version, sv core.SchemaVersion) error {
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
		agg := core.Aggregate[nonEventApplierState]{}

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
		_, _ = agg.SingleEventCommand("test")

		require.PanicsWithError(t, "aggregate is already initialized", func() {
			agg.Initialize("new-id", Created{})
		})
	})
}

type stateStorerTestState struct {
	Value string
}

func (s *stateStorerTestState) Apply(event core.Event) {
	switch e := event.(type) {
	case Created:
		s.Value = "created"
	case ValueUpdated:
		s.Value = e.value
	}
}

func (s *stateStorerTestState) Store(storeFunc func(state core.StatePtr, schemaVersion core.SchemaVersion) error) error {
	return storeFunc(s, core.SchemaVersion(2))
}

type stateStorerTestAgg struct {
	core.Aggregate[stateStorerTestState]
}

func (t *stateStorerTestAgg) StorageOptions() []core.StorageOption {
	return []core.StorageOption{}
}

func TestAggregateStoreWithStateStorer(t *testing.T) {
	t.Run(`Given an aggregate with state implementing StateStorer
		When Store is called
		Then StateStorer.Store should be called
		And custom schema version should be used
	`, func(t *testing.T) {
		agg := stateStorerTestAgg{}
		agg.Initialize("id", Created{})

		var receivedState core.StatePtr
		var receivedSchemaVersion core.SchemaVersion
		err := agg.Store(func(id core.ID, aggregate core.AggregatePtr, storageState core.StatePtr, ep core.EventPack, v core.Version, sv core.SchemaVersion) error {
			receivedState = storageState
			receivedSchemaVersion = sv
			return nil
		})
		require.NoError(t, err)
		require.NotNil(t, receivedState)
		require.Equal(t, core.SchemaVersion(2), receivedSchemaVersion)
		require.Equal(t, core.Version(1), agg.Version())
		require.Empty(t, agg.Events())
	})

	t.Run(`Given an aggregate with state implementing StateStorer
		When Store is called
		And StateStorer.Store returns an error
		Then the error should be returned
		And aggregate state should not be changed
	`, func(t *testing.T) {
		agg := stateStorerTestAgg{}
		agg.Initialize("id", Created{})
		initialVersion := agg.Version()
		initialEvents := len(agg.Events())

		err := agg.Store(func(id core.ID, aggregate core.AggregatePtr, storageState core.StatePtr, ep core.EventPack, v core.Version, sv core.SchemaVersion) error {
			return errStateRestorerFailed
		})
		require.Error(t, err)
		require.ErrorIs(t, err, errStateRestorerFailed)
		require.Equal(t, initialVersion, agg.Version())
		require.Equal(t, initialEvents, len(agg.Events()))
	})
}

type stateRestorerTestState struct {
	Value string
}

func (s *stateRestorerTestState) Apply(event core.Event) {
	switch e := event.(type) {
	case Created:
		s.Value = "created"
	case ValueUpdated:
		s.Value = e.value
	}
}

func (s *stateRestorerTestState) Restore(schemaVersion core.SchemaVersion, restoreFunc func(state core.StatePtr) error) error {
	return restoreFunc(s)
}

type stateRestorerTestAgg struct {
	core.Aggregate[stateRestorerTestState]
}

func (t *stateRestorerTestAgg) StorageOptions() []core.StorageOption {
	return []core.StorageOption{}
}

func TestAggregateRestoreWithStateRestorer(t *testing.T) {
	t.Run(`Given an aggregate with state implementing StateRestorer
		When Restore is called
		Then StateRestorer.Restore should be called
	`, func(t *testing.T) {
		agg := stateRestorerTestAgg{}
		restoreCalled := false
		err := agg.Restore("id", core.Version(5), core.SchemaVersion(3), func(state core.StatePtr) error {
			restoreCalled = true
			s, ok := state.(*stateRestorerTestState)
			require.True(t, ok)
			s.Value = "restored"
			return nil
		})
		require.NoError(t, err)
		require.True(t, restoreCalled)
		require.Equal(t, "restored", agg.State().Value)
		require.Equal(t, core.Version(5), agg.Version())
		require.Empty(t, agg.Events())
	})

	t.Run(`Given an aggregate with state implementing StateRestorer
		When Restore is called
		And StateRestorer.Restore returns an error
		Then the error should be returned
		And aggregate state should not be restored
	`, func(t *testing.T) {
		agg := stateRestorerTestAgg{}
		err := agg.Restore("new-id", core.Version(100), core.SchemaVersion(3), func(state core.StatePtr) error {
			return errStateRestorerFailed
		})
		require.Error(t, err)
		require.ErrorIs(t, err, errStateRestorerFailed)
		require.Equal(t, core.ID("new-id"), agg.ID())
		require.Equal(t, core.Version(100), agg.Version())
	})
}

func TestAggregateVersion(t *testing.T) {
	t.Run(`Given an aggregate
		When Version is called
		Then it should return the current version
	`, func(t *testing.T) {
		agg := newTestAgg("id")
		require.Equal(t, core.Version(0), agg.Version())

		for i := range 10 {
			_, _ = agg.SingleEventCommand("test" + strconv.Itoa(i))
			agg.Store(func(id core.ID, aggregate core.AggregatePtr, storageState core.StatePtr, ep core.EventPack, v core.Version, sv core.SchemaVersion) error {
				return nil
			})
		}
		require.Equal(t, core.Version(10), agg.Version())
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
		require.PanicsWithValue(t, "unsupported event core_test.ValueUpdated", func() {
			core.PanicUnsupportedEvent(ValueUpdated{"test"})
		})

		require.PanicsWithValue(t, "unsupported event string", func() {
			core.PanicUnsupportedEvent("test event")
		})

		require.PanicsWithValue(t, "unsupported event int", func() {
			core.PanicUnsupportedEvent(42)
		})
	})
}

func TestEventOfTypeErrors(t *testing.T) {
	t.Run(`Given an empty event pack
		When EventOfType is called
		Then it should return ErrNoEvents
	`, func(t *testing.T) {
		pack := core.EventPack{}
		_, err := core.EventOfType[ValueUpdated](pack)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrNoEvents)
	})

	t.Run(`Given an event pack with multiple events of the same type
		When EventOfType is called
		Then it should return ErrTooManyEvents
	`, func(t *testing.T) {
		pack := core.EventPack{
			ValueUpdated{"first"},
			ValueUpdated{"second"},
			ValueUpdated{"third"},
		}
		_, err := core.EventOfType[ValueUpdated](pack)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrTooManyEvents)
	})

	t.Run(`Given an event pack with one event of the requested type
		When EventOfType is called
		Then it should return the event successfully
	`, func(t *testing.T) {
		pack := core.EventPack{
			Created{},
			ValueUpdated{"test"},
		}
		evt, err := core.EventOfType[ValueUpdated](pack)
		require.NoError(t, err)
		require.Equal(t, ValueUpdated{"test"}, evt)
	})
}

//go:noinline
func Restore(r core.Restorer) {
	if err := r.Restore("id", 100, core.DefaultSchemaVersion, func(state core.StatePtr) error {
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
		r := core.Restorer(agg)
		for i := 0; i < b.N; i++ {
			Restore(r)
		}
	})

	b.Run("command store allocations", func(b *testing.B) {
		b.ReportAllocs()
		agg := newTestAgg("id")
		for i := 0; i < b.N; i++ {
			err := agg.Store(func(i core.ID, aggregate core.AggregatePtr, storageState core.StatePtr, ep core.EventPack, v core.Version, sv core.SchemaVersion) error {
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
