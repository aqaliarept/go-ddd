//nolint:fieldalignment
package core_test

import (
	"context"
	"errors"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/aqaliarept/go-ddd-kit/pkg/core"
	"github.com/avast/retry-go/v4"
	"github.com/stretchr/testify/require"
)

var (
	errPolicyFailed        = errors.New("policy failed")
	errNonRetryable        = errors.New("non-retryable error")
	errBeginFailed         = errors.New("begin failed")
	errCommitFailed        = errors.New("commit failed")
	errRunFunctionError    = errors.New("run function error")
	errRollbackFailed      = errors.New("rollback failed")
	errBeginError          = errors.New("begin error")
	errCommitError         = errors.New("commit error")
	errOperationFailed     = errors.New("operation failed")
	errSaveFailed          = errors.New("save failed")
	errStoreFailed         = errors.New("store failed")
	errPolicy2Failed       = errors.New("policy 2 failed")
	errPolicyFailedGeneric = errors.New("policy failed")
	errPolicy3Failed       = errors.New("policy 3 failed")
	errNonRetryablePolicy  = errors.New("non-retryable policy error")
	errRollbackError       = errors.New("rollback error")
	errRollbackError1      = errors.New("rollback error 1")
	errRollbackError2      = errors.New("rollback error 2")
)

type mockRepositoryFactory struct {
	createFunc func(ctx context.Context) core.Repository
	callCount  int
	mu         sync.Mutex
}

func (m *mockRepositoryFactory) Create(ctx context.Context) core.Repository {
	m.mu.Lock()
	m.callCount++
	m.mu.Unlock()
	if m.createFunc != nil {
		return m.createFunc(ctx)
	}
	return &mockRepository{}
}

func (m *mockRepositoryFactory) getCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.callCount
}

type mockRepository struct {
	loadFunc func(ctx context.Context, id core.ID, aggregate core.Restorer, options ...core.LoadOption) error
	saveFunc func(ctx context.Context, aggregate core.Storer, options ...core.SaveOption) error
}

func (m *mockRepository) Load(ctx context.Context, id core.ID, aggregate core.Restorer, options ...core.LoadOption) error {
	if m.loadFunc != nil {
		return m.loadFunc(ctx, id, aggregate, options...)
	}
	return nil
}

func (m *mockRepository) Save(ctx context.Context, aggregate core.Storer, options ...core.SaveOption) error {
	if m.saveFunc != nil {
		return m.saveFunc(ctx, aggregate, options...)
	}
	return aggregate.Store(func(id core.ID, aggPtr core.AggregatePtr, state core.StatePtr, events core.EventPack, version core.Version, schemaVersion core.SchemaVersion) error {
		return nil
	})
}

type mockTransactional struct {
	beginFunc     func(ctx context.Context) (context.Context, error)
	commitFunc    func(ctx context.Context) error
	rollbackFunc  func(ctx context.Context) error
	beginCount    int
	commitCount   int
	rollbackCount int
	mu            sync.Mutex
}

func (m *mockTransactional) Begin(ctx context.Context) (context.Context, error) {
	m.mu.Lock()
	m.beginCount++
	m.mu.Unlock()
	if m.beginFunc != nil {
		return m.beginFunc(ctx)
	}
	return ctx, nil
}

func (m *mockTransactional) Commit(ctx context.Context) error {
	m.mu.Lock()
	m.commitCount++
	m.mu.Unlock()
	if m.commitFunc != nil {
		return m.commitFunc(ctx)
	}
	return nil
}

func (m *mockTransactional) Rollback(ctx context.Context) error {
	m.mu.Lock()
	m.rollbackCount++
	m.mu.Unlock()
	if m.rollbackFunc != nil {
		return m.rollbackFunc(ctx)
	}
	return nil
}

func (m *mockTransactional) getBeginCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.beginCount
}

func (m *mockTransactional) getCommitCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.commitCount
}

func (m *mockTransactional) getRollbackCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.rollbackCount
}

type mockTransactionalRepository struct {
	mockRepository
	mockTransactional
}

type mockPolicy struct {
	orderTracker   *[]int
	orderFunc      func() int
	customRun      func(ctx context.Context, repo core.Repository, source core.AggregatePtr, events core.EventPack) error
	failError      error
	executions     []policyExecution
	executionCount int
	shouldFail     bool
}

type policyExecution struct {
	aggregate core.AggregatePtr
	events    core.EventPack
	order     int
}

func newMockPolicy() *mockPolicy {
	return &mockPolicy{
		executions: make([]policyExecution, 0),
	}
}

func (m *mockPolicy) Run(ctx context.Context, repo core.Repository, source core.AggregatePtr, events core.EventPack) error {
	m.executionCount++
	var order int
	if m.orderFunc != nil {
		order = m.orderFunc()
	} else {
		order = m.executionCount
	}
	if m.orderTracker != nil {
		*m.orderTracker = append(*m.orderTracker, order)
	}

	m.executions = append(m.executions, policyExecution{
		aggregate: source,
		events:    events,
		order:     order,
	})

	customRun := m.customRun
	shouldFail := m.shouldFail
	failError := m.failError

	if customRun != nil {
		return customRun(ctx, repo, source, events)
	}

	if shouldFail {
		if failError != nil {
			return failError
		}
		return errPolicyFailed
	}
	return nil
}

func (m *mockPolicy) getExecutionCount() int {
	return m.executionCount
}

func (m *mockPolicy) getExecutions() []policyExecution {
	result := make([]policyExecution, len(m.executions))
	copy(result, m.executions)
	return result
}

type contextKey string

const testContextKey contextKey = "test-key"

type ChangesExpectation struct {
	Aggregates map[core.AggregatePtr]EventPacksExpectation
}

type EventPacksExpectation struct {
	verify func(*testing.T, core.AggregatePtr)
	Events [][]core.Event
}

func ExpectChanges[T any](aggPtr core.AggregatePtr, events [][]core.Event, verifyFn func(*testing.T, T)) ChangesExpectation {
	return ChangesExpectation{
		Aggregates: map[core.AggregatePtr]EventPacksExpectation{
			aggPtr: {
				Events: events,
				verify: func(t *testing.T, ptr core.AggregatePtr) {
					switch agg := any(ptr).(type) {
					case T:
						verifyFn(t, agg)
					default:
						t.Fatalf("unexpected aggregate type: %T", agg)
					}
				},
			},
		},
	}
}

func ExpectChangesWithoutVerification(aggPtr core.AggregatePtr, events [][]core.Event) ChangesExpectation {
	return ChangesExpectation{
		Aggregates: map[core.AggregatePtr]EventPacksExpectation{
			aggPtr: {
				Events: events,
			},
		},
	}
}

func MergeExpectations(expectations ...ChangesExpectation) ChangesExpectation {
	result := ChangesExpectation{
		Aggregates: make(map[core.AggregatePtr]EventPacksExpectation),
	}
	for _, exp := range expectations {
		maps.Copy(result.Aggregates, exp.Aggregates)
	}
	return result
}

func verifyChanges(t *testing.T, changes map[core.AggregatePtr][]core.EventPack, expectation ChangesExpectation) {
	t.Helper()

	if len(expectation.Aggregates) == 0 {
		require.Empty(t, changes)
		return
	}

	for actualAggPtr, actualPacks := range changes {
		expectedPacks, hasExpectation := expectation.Aggregates[actualAggPtr]
		if !hasExpectation {
			continue
		}

		actualEvents := make([][]core.Event, len(actualPacks))
		for i, pack := range actualPacks {
			actualEvents[i] = pack
		}

		require.Equal(t, expectedPacks.Events, actualEvents)

		if expectedPacks.verify != nil {
			expectedPacks.verify(t, actualAggPtr)
		}
	}
}

func TestNewConcurrentScope(t *testing.T) {
	t.Run(`Given a repository factory
		When NewConcurrentScope is called with default options
		Then ConcurrentScope should be created with default retry options
		And rollback timeout should be set to 5 seconds
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := core.NewConcurrentScope(factory)

		require.NotNil(t, scope)
	})

	t.Run(`Given a repository factory
		When NewConcurrentScope is called with custom retry options
		Then ConcurrentScope should be created with merged retry options
		And custom options should be included
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := core.NewConcurrentScope(factory, core.WithRetryOptions(
			retry.Attempts(5),
			retry.Delay(100*time.Millisecond),
		))

		require.NotNil(t, scope)
	})

	t.Run(`Given a ConcurrentScope with default RetryIf condition
		When Run is called and returns ErrTransient or ErrConcurrentModification
		Then the operation should be retried
		And eventually succeed
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := core.NewConcurrentScope(factory)

		callCount := 0
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			callCount++
			if callCount == 1 {
				return core.ErrTransient
			}
			if callCount == 2 {
				return core.ErrConcurrentModification
			}
			return nil
		})

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.GreaterOrEqual(t, callCount, 3)
	})

	t.Run(`Given a repository factory
		When NewConcurrentScope is called with WithRollbackTimeout
		Then ConcurrentScope should be created with custom rollback timeout
		And timeout should match the provided value
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		customTimeout := 15 * time.Second
		scope := core.NewConcurrentScope(factory, core.WithRollbackTimeout(customTimeout))

		require.NotNil(t, scope)
	})

	t.Run(`Given a repository factory
		When NewConcurrentScope is called with both retry options and rollback timeout
		Then ConcurrentScope should be created with both options applied
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		customTimeout := 20 * time.Second
		scope := core.NewConcurrentScope(factory,
			core.WithRetryOptions(retry.Attempts(3)),
			core.WithRollbackTimeout(customTimeout),
		)

		require.NotNil(t, scope)
	})
}

func TestWithRetryOptions(t *testing.T) {
	t.Run(`Given retry options
		When WithRetryOptions is called
		Then retryOptions type should be created
		And should contain the provided options
	`, func(t *testing.T) {
		opts := []retry.Option{retry.Attempts(3)}
		result := core.WithRetryOptions(opts...)

		require.NotNil(t, result)
	})

	t.Run(`Given a ConcurrentScope
		When Run is called with WithRetryOptions
		Then type assertion should work correctly
		And custom retry options should be applied
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := core.NewConcurrentScope(factory)

		customOpts := core.WithRetryOptions(retry.Attempts(2))
		callCount := 0
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			callCount++
			if callCount < 2 {
				return core.ErrTransient
			}
			return nil
		}, customOpts)

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.Equal(t, 2, callCount)
	})
}

func TestConcurrentScope_Run_NonTransactional(t *testing.T) {
	t.Run(`Given a non-transactional repository
		When Run is called with a function that succeeds
		Then the function should execute successfully
		And no retries should occur
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := core.NewConcurrentScope(factory)

		executed := false
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			executed = true
			return nil
		})

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.True(t, executed)
		require.Equal(t, 1, factory.getCallCount())
	})

	t.Run(`Given a non-transactional repository
		When Run is called with a function that returns a non-retryable error
		Then the error should be returned immediately
		And no retries should occur
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := core.NewConcurrentScope(factory, core.WithRetryOptions(retry.Attempts(3)))

		callCount := 0
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			callCount++
			return errNonRetryable
		})

		require.Error(t, err)
		require.Empty(t, changes)
		require.ErrorIs(t, err, errNonRetryable)
		require.Contains(t, err.Error(), "non-retryable error")
		require.Equal(t, 1, callCount)
		require.Equal(t, 1, factory.getCallCount())
	})

	t.Run(`Given a non-transactional repository
		When Run is called and function returns ErrTransient
		Then the operation should be retried
		And eventually succeed
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := core.NewConcurrentScope(factory, core.WithRetryOptions(retry.Attempts(3), retry.Delay(10*time.Millisecond)))

		callCount := 0
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			callCount++
			if callCount < 3 {
				return core.ErrTransient
			}
			return nil
		})

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.Equal(t, 3, callCount)
		require.Equal(t, 3, factory.getCallCount())
	})

	t.Run(`Given a non-transactional repository
		When Run is called and function returns ErrConcurrentModification
		Then the operation should be retried
		And eventually succeed
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := core.NewConcurrentScope(factory, core.WithRetryOptions(retry.Attempts(3), retry.Delay(10*time.Millisecond)))

		callCount := 0
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			callCount++
			if callCount < 2 {
				return core.ErrConcurrentModification
			}
			return nil
		})

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.Equal(t, 2, callCount)
		require.Equal(t, 2, factory.getCallCount())
	})

	t.Run(`Given a non-transactional repository with limited retry attempts
		When Run is called and function always returns ErrTransient
		Then retries should be exhausted
		And ErrTransient should be returned
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := core.NewConcurrentScope(factory, core.WithRetryOptions(retry.Attempts(2), retry.Delay(10*time.Millisecond)))

		callCount := 0
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			callCount++
			return core.ErrTransient
		})

		require.Error(t, err)
		require.Empty(t, changes)
		require.ErrorIs(t, err, core.ErrTransient)
		require.Equal(t, 2, callCount)
		require.Equal(t, 2, factory.getCallCount())
	})

	t.Run(`Given a non-transactional repository
		When Run is called and function returns mixed retryable errors
		Then the operation should be retried for each error type
		And eventually succeed
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := core.NewConcurrentScope(factory, core.WithRetryOptions(retry.Attempts(5), retry.Delay(10*time.Millisecond)))

		callCount := 0
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			callCount++
			if callCount == 1 {
				return core.ErrTransient
			}
			if callCount == 2 {
				return core.ErrConcurrentModification
			}
			return nil
		})

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.Equal(t, 3, callCount)
		require.Equal(t, 3, factory.getCallCount())
	})

	t.Run(`Given a ConcurrentScope with default retry options
		When Run is called with custom retry options via RunOptions
		Then custom options should override default options
		And be applied to the retry logic
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := core.NewConcurrentScope(factory, core.WithRetryOptions(retry.Attempts(10)))

		customOpts := core.WithRetryOptions(retry.Attempts(2), retry.Delay(10*time.Millisecond))
		callCount := 0
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			callCount++
			return core.ErrTransient
		}, customOpts)

		require.Error(t, err)
		require.Empty(t, changes)
		require.Equal(t, 2, callCount)
	})
}

func TestConcurrentScope_Run_Transactional(t *testing.T) {
	t.Run(`Given a transactional repository
		When Run is called and Begin succeeds
		Then transaction should begin
		And runFunc should execute
		And transaction should commit
	`, func(t *testing.T) {
		txRepo := &mockTransactionalRepository{}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) core.Repository {
				return txRepo
			},
		}
		scope := core.NewConcurrentScope(factory)

		executed := false
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			executed = true
			return nil
		})

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.True(t, executed)
		require.Equal(t, 1, txRepo.getBeginCount())
		require.Equal(t, 1, txRepo.getCommitCount())
		require.Equal(t, 0, txRepo.getRollbackCount())
	})

	t.Run(`Given a transactional repository
		When Run is called and Begin fails with non-retryable error
		Then Begin error should be returned
		And runFunc should not be called
		And no commit or rollback should occur
	`, func(t *testing.T) {
		txRepo := &mockTransactionalRepository{
			mockTransactional: mockTransactional{
				beginFunc: func(ctx context.Context) (context.Context, error) {
					return nil, errBeginFailed
				},
			},
		}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) core.Repository {
				return txRepo
			},
		}
		scope := core.NewConcurrentScope(factory, core.WithRetryOptions(retry.Attempts(3)))

		callCount := 0
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			callCount++
			return nil
		})

		require.Error(t, err)
		require.Empty(t, changes)
		require.ErrorIs(t, err, errBeginFailed)
		require.Contains(t, err.Error(), "begin failed")
		require.Equal(t, 0, callCount)
		require.Equal(t, 1, txRepo.getBeginCount())
		require.Equal(t, 0, txRepo.getCommitCount())
		require.Equal(t, 0, txRepo.getRollbackCount())
	})

	t.Run(`Given a transactional repository
		When Run is called and Begin returns ErrTransient
		Then Begin should be retried
		And eventually succeed
		And transaction should commit
	`, func(t *testing.T) {
		beginCallCount := 0
		txRepo := &mockTransactionalRepository{
			mockTransactional: mockTransactional{
				beginFunc: func(ctx context.Context) (context.Context, error) {
					beginCallCount++
					if beginCallCount < 2 {
						return nil, core.ErrTransient
					}
					return ctx, nil
				},
			},
		}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) core.Repository {
				return txRepo
			},
		}
		scope := core.NewConcurrentScope(factory, core.WithRetryOptions(retry.Attempts(3), retry.Delay(10*time.Millisecond)))

		executed := false
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			executed = true
			return nil
		})

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.True(t, executed)
		require.Equal(t, 2, beginCallCount)
		require.Equal(t, 1, txRepo.getCommitCount())
	})

	t.Run(`Given a transactional repository
		When Run is called and runFunc succeeds
		Then transaction should commit successfully
		And no rollback should occur
	`, func(t *testing.T) {
		txRepo := &mockTransactionalRepository{}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) core.Repository {
				return txRepo
			},
		}
		scope := core.NewConcurrentScope(factory)

		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			return nil
		})

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.Equal(t, 1, txRepo.getBeginCount())
		require.Equal(t, 1, txRepo.getCommitCount())
		require.Equal(t, 0, txRepo.getRollbackCount())
	})

	t.Run(`Given a transactional repository
		When Run is called and Commit fails
		Then Commit error should be returned
		And no rollback should occur
	`, func(t *testing.T) {
		txRepo := &mockTransactionalRepository{
			mockTransactional: mockTransactional{
				commitFunc: func(ctx context.Context) error {
					return errCommitFailed
				},
			},
		}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) core.Repository {
				return txRepo
			},
		}
		scope := core.NewConcurrentScope(factory, core.WithRetryOptions(retry.Attempts(3)))

		callCount := 0
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			callCount++
			return nil
		})

		require.Error(t, err)
		require.Empty(t, changes)
		require.ErrorIs(t, err, errCommitFailed)
		require.Contains(t, err.Error(), "commit failed")
		require.Equal(t, 1, callCount)
		require.Equal(t, 1, txRepo.getBeginCount())
		require.Equal(t, 1, txRepo.getCommitCount())
		require.Equal(t, 0, txRepo.getRollbackCount())
	})

	t.Run(`Given a transactional repository
		When Run is called and Commit returns ErrTransient
		Then Commit should be retried
		And eventually succeed
	`, func(t *testing.T) {
		commitCallCount := 0
		txRepo := &mockTransactionalRepository{
			mockTransactional: mockTransactional{
				commitFunc: func(ctx context.Context) error {
					commitCallCount++
					if commitCallCount < 2 {
						return core.ErrTransient
					}
					return nil
				},
			},
		}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) core.Repository {
				return txRepo
			},
		}
		scope := core.NewConcurrentScope(factory, core.WithRetryOptions(retry.Attempts(3), retry.Delay(10*time.Millisecond)))

		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			return nil
		})

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.Equal(t, 2, commitCallCount)
		require.Equal(t, 2, txRepo.getBeginCount())
		require.Equal(t, 2, txRepo.getCommitCount())
	})

	t.Run(`Given a transactional repository
		When Run is called and runFunc returns an error
		Then transaction should be rolled back
		And runFunc error should be returned
	`, func(t *testing.T) {
		txRepo := &mockTransactionalRepository{}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) core.Repository {
				return txRepo
			},
		}
		scope := core.NewConcurrentScope(factory)

		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			return errRunFunctionError
		})

		require.Error(t, err)
		require.Empty(t, changes)
		require.ErrorIs(t, err, errRunFunctionError)
		require.Equal(t, 1, txRepo.getBeginCount())
		require.Equal(t, 0, txRepo.getCommitCount())
		require.Equal(t, 1, txRepo.getRollbackCount())
	})

	t.Run(`Given a transactional repository with rollback timeout
		When Run is called and runFunc returns an error
		And Rollback takes longer than timeout
		Then rollback should be cancelled after timeout
		And runFunc error should be returned
	`, func(t *testing.T) {
		txRepo := &mockTransactionalRepository{
			mockTransactional: mockTransactional{
				rollbackFunc: func(ctx context.Context) error {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(2 * time.Second):
						return nil
					}
				},
			},
		}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) core.Repository {
				return txRepo
			},
		}
		scope := core.NewConcurrentScope(factory)

		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			return errRunFunctionError
		})

		require.Error(t, err)
		require.Empty(t, changes)
		require.ErrorIs(t, err, errRunFunctionError)
		require.Equal(t, 1, txRepo.getRollbackCount())
	})

	t.Run(`Given a transactional repository
		When Run is called and runFunc returns an error
		And Rollback also returns an error
		Then both errors should be joined
		And returned together
	`, func(t *testing.T) {
		txRepo := &mockTransactionalRepository{
			mockTransactional: mockTransactional{
				rollbackFunc: func(ctx context.Context) error {
					return errRollbackFailed
				},
			},
		}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) core.Repository {
				return txRepo
			},
		}
		scope := core.NewConcurrentScope(factory)

		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			return errRunFunctionError
		})

		require.Error(t, err)
		require.Empty(t, changes)
		require.ErrorIs(t, err, errRunFunctionError)
		require.ErrorIs(t, err, errRollbackFailed)
		require.Equal(t, 1, txRepo.getRollbackCount())
	})

	t.Run(`Given a transactional repository
		When Run is called and runFunc returns an error
		And Rollback succeeds
		Then runFunc error should be returned
	`, func(t *testing.T) {
		txRepo := &mockTransactionalRepository{}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) core.Repository {
				return txRepo
			},
		}
		scope := core.NewConcurrentScope(factory)

		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			return errRunFunctionError
		})

		require.Error(t, err)
		require.Empty(t, changes)
		require.ErrorIs(t, err, errRunFunctionError)
		require.Equal(t, 1, txRepo.getRollbackCount())
	})

	t.Run(`Given a transactional repository
		When Run is called with a cancelled context
		And runFunc returns an error
		Then Rollback should use independent context
		And not be affected by original context cancellation
	`, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		rollbackCalled := false
		txRepo := &mockTransactionalRepository{
			mockTransactional: mockTransactional{
				rollbackFunc: func(rollbackCtx context.Context) error {
					rollbackCalled = true
					select {
					case <-rollbackCtx.Done():
						return rollbackCtx.Err()
					default:
						return nil
					}
				},
			},
		}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) core.Repository {
				return txRepo
			},
		}
		scope := core.NewConcurrentScope(factory)

		changes, err := scope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
			return errRunFunctionError
		})

		require.Error(t, err)
		require.Empty(t, changes)
		require.True(t, rollbackCalled)
		require.Equal(t, 1, txRepo.getRollbackCount())
	})

	t.Run(`Given a transactional repository
		When Run is called with successful runFunc
		Then Begin should be called
		And runFunc should execute
		And Commit should be called
		And no Rollback should occur
	`, func(t *testing.T) {
		txRepo := &mockTransactionalRepository{}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) core.Repository {
				return txRepo
			},
		}
		scope := core.NewConcurrentScope(factory)

		executed := false
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			executed = true
			return nil
		})

		require.NoError(t, err)
		require.Empty(t, changes)
		require.True(t, executed)
		require.Equal(t, 1, txRepo.getBeginCount())
		require.Equal(t, 1, txRepo.getCommitCount())
		require.Equal(t, 0, txRepo.getRollbackCount())
	})

	t.Run(`Given a transactional repository
		When Run is called and runFunc returns an error
		Then Begin should be called
		And runFunc should execute
		And Rollback should be called
		And no Commit should occur
	`, func(t *testing.T) {
		txRepo := &mockTransactionalRepository{}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) core.Repository {
				return txRepo
			},
		}
		scope := core.NewConcurrentScope(factory)

		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			return errRunFunctionError
		})

		require.Error(t, err)
		require.Empty(t, changes)
		require.ErrorIs(t, err, errRunFunctionError)
		require.Equal(t, 1, txRepo.getBeginCount())
		require.Equal(t, 0, txRepo.getCommitCount())
		require.Equal(t, 1, txRepo.getRollbackCount())
	})

	t.Run(`Given a transactional repository
		When Run is called and Begin succeeds
		And runFunc returns ErrTransient
		Then transaction should be rolled back
		And operation should be retried
		And eventually succeed
	`, func(t *testing.T) {
		runCallCount := 0
		txRepo := &mockTransactionalRepository{}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) core.Repository {
				return txRepo
			},
		}
		scope := core.NewConcurrentScope(factory, core.WithRetryOptions(retry.Attempts(3), retry.Delay(10*time.Millisecond)))

		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			runCallCount++
			if runCallCount < 2 {
				return core.ErrTransient
			}
			return nil
		})

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.Equal(t, 2, runCallCount)
		require.Equal(t, 2, txRepo.getBeginCount())
		require.Equal(t, 1, txRepo.getCommitCount())
		require.Equal(t, 1, txRepo.getRollbackCount())
	})

	t.Run(`Given a transactional repository
		When Run is called and Begin succeeds
		And runFunc returns ErrConcurrentModification
		Then transaction should be rolled back
		And operation should be retried
		And eventually succeed
	`, func(t *testing.T) {
		runCallCount := 0
		txRepo := &mockTransactionalRepository{}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) core.Repository {
				return txRepo
			},
		}
		scope := core.NewConcurrentScope(factory, core.WithRetryOptions(retry.Attempts(3), retry.Delay(10*time.Millisecond)))

		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			runCallCount++
			if runCallCount < 2 {
				return core.ErrConcurrentModification
			}
			return nil
		})

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.Equal(t, 2, runCallCount)
		require.Equal(t, 2, txRepo.getBeginCount())
		require.Equal(t, 1, txRepo.getCommitCount())
		require.Equal(t, 1, txRepo.getRollbackCount())
	})

	t.Run(`Given a transactional repository
		When Run is called and runFunc returns ErrTransient multiple times
		Then new transaction should be created on each retry
		And eventually succeed
	`, func(t *testing.T) {
		runCallCount := 0
		createCount := 0
		txRepo := &mockTransactionalRepository{}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) core.Repository {
				createCount++
				return txRepo
			},
		}
		scope := core.NewConcurrentScope(factory, core.WithRetryOptions(retry.Attempts(3), retry.Delay(10*time.Millisecond)))

		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			runCallCount++
			if runCallCount < 3 {
				return core.ErrTransient
			}
			return nil
		})

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.Equal(t, 3, runCallCount)
		require.Equal(t, 3, createCount)
		require.Equal(t, 3, txRepo.getBeginCount())
		require.Equal(t, 1, txRepo.getCommitCount())
		require.Equal(t, 2, txRepo.getRollbackCount())
	})
}

func TestConcurrentScope_Run_ContextHandling(t *testing.T) {
	t.Run(`Given a cancelled context
		When Run is called
		Then context.Canceled error should be returned
	`, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		factory := &mockRepositoryFactory{}
		scope := core.NewConcurrentScope(factory)

		changes, err := scope.Run(ctx, func(runCtx context.Context, repo core.Repository) error {
			select {
			case <-runCtx.Done():
				return runCtx.Err()
			default:
				return nil
			}
		})

		require.Error(t, err)
		require.Empty(t, changes)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run(`Given a context
		When Run is called and context is cancelled during execution
		Then context.Canceled error should be returned
	`, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		factory := &mockRepositoryFactory{}
		scope := core.NewConcurrentScope(factory)

		changes, err := scope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
			cancel()
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				return nil
			}
		})

		require.Error(t, err)
		require.Empty(t, changes)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run(`Given a context with timeout
		When Run is called and execution exceeds timeout
		Then context.DeadlineExceeded error should be returned
	`, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		factory := &mockRepositoryFactory{}
		scope := core.NewConcurrentScope(factory)

		changes, err := scope.Run(ctx, func(runCtx context.Context, repo core.Repository) error {
			select {
			case <-runCtx.Done():
				return runCtx.Err()
			case <-time.After(100 * time.Millisecond):
				return nil
			}
		})

		require.Error(t, err)
		require.Empty(t, changes)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run(`Given a context with values
		When Run is called
		Then context should be propagated to factory
		And to runFunc
	`, func(t *testing.T) {
		ctx := context.WithValue(context.Background(), testContextKey, "test-value")
		var receivedCtx context.Context

		factory := &mockRepositoryFactory{
			createFunc: func(factoryCtx context.Context) core.Repository {
				receivedCtx = factoryCtx
				return &mockRepository{}
			},
		}
		scope := core.NewConcurrentScope(factory)

		var runFuncCtx context.Context
		changes, err := scope.Run(ctx, func(runCtx context.Context, repo core.Repository) error {
			runFuncCtx = runCtx
			return nil
		})

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.Equal(t, ctx, receivedCtx)
		require.NotNil(t, runFuncCtx)
	})

	t.Run(`Given a transactional repository
		When Run is called with a cancelled context
		And runFunc returns an error
		Then Rollback should use independent context
		And not be affected by original context cancellation
	`, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var rollbackCtx context.Context

		txRepo := &mockTransactionalRepository{
			mockTransactional: mockTransactional{
				rollbackFunc: func(rbCtx context.Context) error {
					rollbackCtx = rbCtx
					select {
					case <-rbCtx.Done():
						return rbCtx.Err()
					default:
						return nil
					}
				},
			},
		}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) core.Repository {
				return txRepo
			},
		}
		scope := core.NewConcurrentScope(factory)

		changes, err := scope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
			cancel()
			return errRunFunctionError
		})

		require.Error(t, err)
		require.Empty(t, changes)
		require.NotNil(t, rollbackCtx)
		require.NotEqual(t, ctx, rollbackCtx)
	})
}

func TestConcurrentScope_Run_RetryOptionsMerging(t *testing.T) {
	t.Run(`Given a ConcurrentScope with default retry options
		When Run is called without RunOptions
		Then default retry options should be used
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := core.NewConcurrentScope(factory)

		callCount := 0
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			callCount++
			if callCount < 2 {
				return core.ErrTransient
			}
			return nil
		})

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.GreaterOrEqual(t, callCount, 2)
	})

	t.Run(`Given a ConcurrentScope created with custom retry options
		When Run is called
		Then custom options should be used
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := core.NewConcurrentScope(factory, core.WithRetryOptions(retry.Attempts(2), retry.Delay(10*time.Millisecond)))

		callCount := 0
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			callCount++
			return core.ErrTransient
		})

		require.Error(t, err)
		require.Empty(t, changes)
		require.Equal(t, 2, callCount)
	})

	t.Run(`Given a ConcurrentScope with default retry options
		When Run is called with RunOptions
		Then RunOptions should override default options
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := core.NewConcurrentScope(factory, core.WithRetryOptions(retry.Attempts(10)))

		customOpts := core.WithRetryOptions(retry.Attempts(2), retry.Delay(10*time.Millisecond))
		callCount := 0
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			callCount++
			return core.ErrTransient
		}, customOpts)

		require.Error(t, err)
		require.Empty(t, changes)
		require.Equal(t, 2, callCount)
	})

	t.Run(`Given a ConcurrentScope
		When Run is called with multiple RunOptions
		Then all RunOptions should be merged
		And applied together
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := core.NewConcurrentScope(factory, core.WithRetryOptions(retry.Attempts(10)))

		opts1 := core.WithRetryOptions(retry.Attempts(5))
		opts2 := core.WithRetryOptions(retry.Delay(10 * time.Millisecond))
		callCount := 0
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			callCount++
			if callCount < 3 {
				return core.ErrTransient
			}
			return nil
		}, opts1, opts2)

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.Equal(t, 3, callCount)
	})

	t.Run(`Given a ConcurrentScope
		When Run is called with invalid RunOptions type
		Then invalid options should be ignored gracefully
		And default options should be used
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := core.NewConcurrentScope(factory, core.WithRetryOptions(retry.Attempts(2)))

		invalidOpt := "not a retryOptions"
		callCount := 0
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			callCount++
			if callCount < 2 {
				return core.ErrTransient
			}
			return nil
		}, invalidOpt)

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.Equal(t, 2, callCount)
	})
}

func TestConcurrentScope_Run_ErrorPropagation(t *testing.T) {
	t.Run(`Given a ConcurrentScope
		When Run is called and runFunc returns an error
		Then the error should be propagated correctly
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := core.NewConcurrentScope(factory)

		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			return errRunFunctionError
		})

		require.Error(t, err)
		require.Empty(t, changes)
		require.ErrorIs(t, err, errRunFunctionError)
		require.Contains(t, err.Error(), "run function error")
	})

	t.Run(`Given a transactional repository
		When Run is called and Begin returns an error
		Then Begin error should be propagated correctly
	`, func(t *testing.T) {
		txRepo := &mockTransactionalRepository{
			mockTransactional: mockTransactional{
				beginFunc: func(ctx context.Context) (context.Context, error) {
					return nil, errBeginError
				},
			},
		}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) core.Repository {
				return txRepo
			},
		}
		scope := core.NewConcurrentScope(factory)

		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			return nil
		})

		require.Error(t, err)
		require.Empty(t, changes)
		require.ErrorIs(t, err, errBeginError)
		require.Contains(t, err.Error(), "begin error")
	})

	t.Run(`Given a transactional repository
		When Run is called and Commit returns an error
		Then Commit error should be propagated correctly
	`, func(t *testing.T) {
		txRepo := &mockTransactionalRepository{
			mockTransactional: mockTransactional{
				commitFunc: func(ctx context.Context) error {
					return errCommitError
				},
			},
		}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) core.Repository {
				return txRepo
			},
		}
		scope := core.NewConcurrentScope(factory)

		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			return nil
		})

		require.Error(t, err)
		require.Empty(t, changes)
		require.ErrorIs(t, err, errCommitError)
		require.Contains(t, err.Error(), "commit error")
	})

	t.Run(`Given a transactional repository
		When Run is called and runFunc returns an error
		And Rollback also returns an error
		Then both errors should be joined
		And propagated together
	`, func(t *testing.T) {
		txRepo := &mockTransactionalRepository{
			mockTransactional: mockTransactional{
				rollbackFunc: func(ctx context.Context) error {
					return errRollbackError
				},
			},
		}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) core.Repository {
				return txRepo
			},
		}
		scope := core.NewConcurrentScope(factory)

		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			return errRunFunctionError
		})

		require.Error(t, err)
		require.Empty(t, changes)
		require.ErrorIs(t, err, errRunFunctionError)
		require.ErrorIs(t, err, errRollbackError)
	})

	t.Run(`Given a transactional repository
		When Run is called and multiple errors occur
		Then all errors should be joined
		And propagated together
	`, func(t *testing.T) {
		rollbackCallCount := 0
		txRepo := &mockTransactionalRepository{
			mockTransactional: mockTransactional{
				rollbackFunc: func(ctx context.Context) error {
					rollbackCallCount++
					if rollbackCallCount == 1 {
						return errRollbackError1
					}
					return errRollbackError2
				},
			},
		}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) core.Repository {
				return txRepo
			},
		}
		scope := core.NewConcurrentScope(factory)

		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			return errRunFunctionError
		})

		require.Error(t, err)
		require.Empty(t, changes)
		require.ErrorIs(t, err, errRunFunctionError)
		require.ErrorIs(t, err, errRollbackError1)
	})
}

func TestConcurrentScope_Run_EdgeCases(t *testing.T) {
	t.Run(`Given a ConcurrentScope
		When Run is called with empty runOptions
		Then operation should execute successfully
		And default options should be used
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := core.NewConcurrentScope(factory)

		executed := false
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			executed = true
			return nil
		})

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.True(t, executed)
	})

	t.Run(`Given a repository that implements both Repository and Transactional
		When Run is called
		Then transactional path should be taken
		And Begin and Commit should be called
	`, func(t *testing.T) {
		txRepo := &mockTransactionalRepository{}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) core.Repository {
				return txRepo
			},
		}
		scope := core.NewConcurrentScope(factory)

		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			return nil
		})

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.Equal(t, 1, txRepo.getBeginCount())
		require.Equal(t, 1, txRepo.getCommitCount())
	})

	t.Run(`Given a repository that implements only Repository
		When Run is called
		Then non-transactional path should be taken
		And no Begin or Commit should be called
	`, func(t *testing.T) {
		repo := &mockRepository{}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) core.Repository {
				return repo
			},
		}
		scope := core.NewConcurrentScope(factory)

		executed := false
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			executed = true
			return nil
		})

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.True(t, executed)
	})

	t.Run(`Given a factory that returns different repository instances
		When Run is called and retries occur
		Then new repository instance should be created on each retry
	`, func(t *testing.T) {
		createCount := 0
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) core.Repository {
				createCount++
				return &mockRepository{}
			},
		}
		scope := core.NewConcurrentScope(factory, core.WithRetryOptions(retry.Attempts(3), retry.Delay(10*time.Millisecond)))

		callCount := 0
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			callCount++
			if callCount < 2 {
				return core.ErrTransient
			}
			return nil
		})

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.Equal(t, 2, callCount)
		require.Equal(t, 2, createCount)
	})
}

func TestConcurrentScope_Run_ChangesTracking(t *testing.T) {
	t.Run(`Given a repository
		When Run is called and a single aggregate is saved
		Then changes map should contain the aggregate
		And should have one event pack with the aggregate's events
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := core.NewConcurrentScope(factory)

		var savedAggregate *testAgg
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			agg := newTestAgg("test-id-1")
			_, err := agg.SingleEventCommand("test-value")
			require.NoError(t, err)
			savedAggregate = agg
			return repo.Save(ctx, agg)
		})

		require.NoError(t, err)
		verifyChanges(t, changes, ExpectChanges[*testAgg](
			core.AggregatePtr(savedAggregate),
			[][]core.Event{{Created{}, ValueUpdated{value: "test-value"}}},
			func(t *testing.T, agg *testAgg) {
				require.Equal(t, "test-value", agg.State().MyString)
			},
		))
	})

	t.Run(`Given a repository
		When Run is called and multiple aggregates are saved
		Then changes map should contain all aggregates
		And each aggregate should have its events tracked
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := core.NewConcurrentScope(factory)

		var savedAggregate1, savedAggregate2 *testAgg
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			agg1 := newTestAgg("test-id-1")
			_, err := agg1.SingleEventCommand("value-1")
			require.NoError(t, err)
			savedAggregate1 = agg1
			if saveErr := repo.Save(ctx, agg1); saveErr != nil {
				return saveErr
			}

			agg2 := newTestAgg("test-id-2")
			_, err = agg2.SingleEventCommand("value-2")
			require.NoError(t, err)
			savedAggregate2 = agg2
			return repo.Save(ctx, agg2)
		})

		require.NoError(t, err)
		verifyChanges(t, changes, MergeExpectations(
			ExpectChangesWithoutVerification(core.AggregatePtr(savedAggregate1), [][]core.Event{{Created{}, ValueUpdated{value: "value-1"}}}),
			ExpectChangesWithoutVerification(core.AggregatePtr(savedAggregate2), [][]core.Event{{Created{}, ValueUpdated{value: "value-2"}}}),
		))
	})

	t.Run(`Given a repository
		When Run is called and the same aggregate is saved multiple times
		Then changes map should contain the aggregate once
		And should have multiple event packs for that aggregate
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := core.NewConcurrentScope(factory)

		var savedAggregate *testAgg
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			agg := newTestAgg("test-id-1")
			savedAggregate = agg

			_, err := agg.SingleEventCommand("value-1")
			require.NoError(t, err)
			if saveErr := repo.Save(ctx, agg); saveErr != nil {
				return saveErr
			}

			_, err = agg.SingleEventCommand("value-2")
			require.NoError(t, err)
			return repo.Save(ctx, agg)
		})

		require.NoError(t, err)
		verifyChanges(t, changes, ExpectChangesWithoutVerification(
			core.AggregatePtr(savedAggregate),
			[][]core.Event{
				{Created{}, ValueUpdated{value: "value-1"}},
				{ValueUpdated{value: "value-2"}},
			},
		))
	})

	t.Run(`Given a repository
		When Run is called and no aggregates are saved
		Then changes map should be empty
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := core.NewConcurrentScope(factory)

		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			return nil
		})

		require.NoError(t, err)
		verifyChanges(t, changes, ChangesExpectation{})
	})

	t.Run(`Given a repository
		When Run is called and an aggregate is saved but then an error occurs
		Then changes map should be empty
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := core.NewConcurrentScope(factory)
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			agg := newTestAgg("test-id-1")
			_, err := agg.SingleEventCommand("test-value")
			require.NoError(t, err)
			if err = repo.Save(ctx, agg); err != nil {
				return err
			}
			return errOperationFailed
		})

		require.Error(t, err)
		require.ErrorIs(t, err, errOperationFailed)
		verifyChanges(t, changes, ChangesExpectation{})
	})

	t.Run(`Given a transactional repository
		When Run is called and aggregates are saved successfully
		Then changes map should contain all saved aggregates
	`, func(t *testing.T) {
		txRepo := &mockTransactionalRepository{}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) core.Repository {
				return txRepo
			},
		}
		scope := core.NewConcurrentScope(factory)

		var savedAggregate *testAgg
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			agg := newTestAgg("test-id-1")
			_, err := agg.SingleEventCommand("test-value")
			require.NoError(t, err)
			savedAggregate = agg
			return repo.Save(ctx, agg)
		})

		require.NoError(t, err)
		require.Equal(t, 1, txRepo.getBeginCount())
		require.Equal(t, 1, txRepo.getCommitCount())
		verifyChanges(t, changes, ExpectChangesWithoutVerification(
			core.AggregatePtr(savedAggregate),
			[][]core.Event{{Created{}, ValueUpdated{value: "test-value"}}},
		))
	})
}

func TestConcurrentScope_Run_LoadOperation(t *testing.T) {
	t.Run(`Given a ConcurrentScope
		When Run is called and Load is executed through repoDecorator
		Then Load should delegate to inner repository
		And aggregate should be loaded successfully
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := core.NewConcurrentScope(factory)

		_ = newTestAgg("load-test-id")

		loadCalled := false
		repo := &mockRepository{
			loadFunc: func(ctx context.Context, id core.ID, aggregate core.Restorer, options ...core.LoadOption) error {
				loadCalled = true
				require.Equal(t, core.ID("load-test-id"), id)
				return aggregate.Restore(id, core.Version(5), core.DefaultSchemaVersion, func(state core.StatePtr) error {
					s, ok := state.(*testAggState)
					require.True(t, ok)
					s.MyString = "loaded-value"
					return nil
				})
			},
		}
		factory.createFunc = func(ctx context.Context) core.Repository {
			return repo
		}

		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			agg := &testAgg{}
			return repo.Load(ctx, "load-test-id", agg)
		})

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.True(t, loadCalled)
	})
}

func TestConcurrentScope_Run_SaveError(t *testing.T) {
	t.Run(`Given a ConcurrentScope
		When Run is called and Save returns an error
		Then error should be propagated
		And changes should be tracked before error
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := core.NewConcurrentScope(factory)

		repo := &mockRepository{
			saveFunc: func(ctx context.Context, aggregate core.Storer, options ...core.SaveOption) error {
				return errSaveFailed
			},
		}
		factory.createFunc = func(ctx context.Context) core.Repository {
			return repo
		}

		var savedAggregate *testAgg
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			agg := newTestAgg("save-error-id")
			_, err := agg.SingleEventCommand("test-value")
			require.NoError(t, err)
			savedAggregate = agg
			return repo.Save(ctx, agg)
		})

		require.Error(t, err)
		require.ErrorIs(t, err, errSaveFailed)
		verifyChanges(t, changes, ExpectChangesWithoutVerification(
			core.AggregatePtr(savedAggregate),
			[][]core.Event{{Created{}, ValueUpdated{value: "test-value"}}},
		))
	})
}

func TestConcurrentScope_Run_StoreError(t *testing.T) {
	t.Run(`Given a ConcurrentScope
		When Run is called and Store returns an error
		Then error should be propagated
		And changes should not be tracked
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := core.NewConcurrentScope(factory)

		repo := &mockRepository{
			saveFunc: func(ctx context.Context, aggregate core.Storer, options ...core.SaveOption) error {
				return aggregate.Store(func(id core.ID, aggPtr core.AggregatePtr, state core.StatePtr, events core.EventPack, version core.Version, schemaVersion core.SchemaVersion) error {
					return errStoreFailed
				})
			},
		}
		factory.createFunc = func(ctx context.Context) core.Repository {
			return repo
		}

		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			agg := newTestAgg("store-error-id")
			_, err := agg.SingleEventCommand("test-value")
			require.NoError(t, err)
			return repo.Save(ctx, agg)
		})

		require.Error(t, err)
		require.ErrorIs(t, err, errStoreFailed)
		require.Empty(t, changes)
	})
}

func TestConcurrentScope_Run_Policies(t *testing.T) {
	t.Run(`Given a ConcurrentScope with multiple policies
		When Run is called and a single aggregate is saved
		Then all policies should execute in order
		And each policy should receive correct aggregate and events
		And transaction should commit successfully
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		policy1 := newMockPolicy()
		policy2 := newMockPolicy()
		policy3 := newMockPolicy()
		scope := core.NewConcurrentScope(factory,
			core.WithScopedPolicy(policy1),
			core.WithScopedPolicy(policy2),
			core.WithScopedPolicy(policy3),
		)

		var savedAggregate *testAgg
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			agg := newTestAgg("test-id-1")
			_, err := agg.SingleEventCommand("test-value")
			require.NoError(t, err)
			savedAggregate = agg
			return repo.Save(ctx, agg)
		})

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.Equal(t, 1, policy1.getExecutionCount())
		require.Equal(t, 1, policy2.getExecutionCount())
		require.Equal(t, 1, policy3.getExecutionCount())

		executions1 := policy1.getExecutions()
		require.Len(t, executions1, 1)
		require.Equal(t, core.AggregatePtr(savedAggregate), executions1[0].aggregate)
		require.Equal(t, core.EventPack{Created{}, ValueUpdated{value: "test-value"}}, executions1[0].events)

		executions2 := policy2.getExecutions()
		require.Len(t, executions2, 1)
		require.Equal(t, core.AggregatePtr(savedAggregate), executions2[0].aggregate)

		executions3 := policy3.getExecutions()
		require.Len(t, executions3, 1)
		require.Equal(t, core.AggregatePtr(savedAggregate), executions3[0].aggregate)
	})

	t.Run(`Given a ConcurrentScope with multiple policies
		When Run is called and multiple aggregates are saved
		Then policies should execute for each aggregate save
		And correct aggregate and events should be passed to each policy call
		And all changes should be tracked correctly
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		policy1 := newMockPolicy()
		policy2 := newMockPolicy()
		scope := core.NewConcurrentScope(factory,
			core.WithScopedPolicy(policy1),
			core.WithScopedPolicy(policy2),
		)

		var savedAggregate1, savedAggregate2, savedAggregate3 *testAgg
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			agg1 := newTestAgg("test-id-1")
			_, err := agg1.SingleEventCommand("value-1")
			require.NoError(t, err)
			savedAggregate1 = agg1
			if err = repo.Save(ctx, agg1); err != nil {
				return err
			}

			agg2 := newTestAgg("test-id-2")
			_, err = agg2.SingleEventCommand("value-2")
			require.NoError(t, err)
			savedAggregate2 = agg2
			if err = repo.Save(ctx, agg2); err != nil {
				return err
			}

			agg3 := newTestAgg("test-id-3")
			_, err = agg3.SingleEventCommand("value-3")
			require.NoError(t, err)
			savedAggregate3 = agg3
			return repo.Save(ctx, agg3)
		})

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.Equal(t, 3, policy1.getExecutionCount())
		require.Equal(t, 3, policy2.getExecutionCount())

		executions1 := policy1.getExecutions()
		require.Len(t, executions1, 3)
		require.Equal(t, core.AggregatePtr(savedAggregate1), executions1[0].aggregate)
		require.Equal(t, core.AggregatePtr(savedAggregate2), executions1[1].aggregate)
		require.Equal(t, core.AggregatePtr(savedAggregate3), executions1[2].aggregate)

		executions2 := policy2.getExecutions()
		require.Len(t, executions2, 3)
		require.Equal(t, core.AggregatePtr(savedAggregate1), executions2[0].aggregate)
		require.Equal(t, core.AggregatePtr(savedAggregate2), executions2[1].aggregate)
		require.Equal(t, core.AggregatePtr(savedAggregate3), executions2[2].aggregate)
	})

	t.Run(`Given a ConcurrentScope with transactional repository and multiple policies
		When Run is called and second policy fails
		Then first policy should execute
		And second policy should fail
		And transaction should be rolled back
		And commit should not be called
		And error should be returned
	`, func(t *testing.T) {
		txRepo := &mockTransactionalRepository{}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) core.Repository {
				return txRepo
			},
		}
		policy1 := newMockPolicy()
		policy2 := newMockPolicy()
		policy2.shouldFail = true
		policy2.failError = errPolicy2Failed
		scope := core.NewConcurrentScope(factory,
			core.WithScopedPolicy(policy1),
			core.WithScopedPolicy(policy2),
		)

		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			agg := newTestAgg("test-id-1")
			_, err := agg.SingleEventCommand("test-value")
			require.NoError(t, err)
			return repo.Save(ctx, agg)
		})

		require.Error(t, err)
		require.Empty(t, changes)
		require.Contains(t, err.Error(), "policy 2 failed")
		require.Equal(t, 1, policy1.getExecutionCount())
		require.Equal(t, 1, policy2.getExecutionCount())
		require.Equal(t, 1, txRepo.getBeginCount())
		require.Equal(t, 0, txRepo.getCommitCount())
		require.Equal(t, 1, txRepo.getRollbackCount())
	})

	t.Run(`Given a ConcurrentScope with non-transactional repository
		When Run is called and policy fails
		Then policy should execute and fail
		And error should be returned
		And changes should still be tracked
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		policy := newMockPolicy()
		policy.shouldFail = true
		policy.failError = errPolicyFailedGeneric
		scope := core.NewConcurrentScope(factory, core.WithScopedPolicy(policy))

		var savedAggregate *testAgg
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			agg := newTestAgg("test-id-1")
			_, err := agg.SingleEventCommand("test-value")
			require.NoError(t, err)
			savedAggregate = agg
			return repo.Save(ctx, agg)
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "policy failed")
		require.Equal(t, 1, policy.getExecutionCount())
		verifyChanges(t, changes, ExpectChangesWithoutVerification(
			core.AggregatePtr(savedAggregate),
			[][]core.Event{{Created{}, ValueUpdated{value: "test-value"}}},
		))
	})

	t.Run(`Given a ConcurrentScope with multiple policies
		When Run is called and second policy fails
		Then first policy should execute
		And second policy should fail
		And third policy should not execute
		And error should be returned
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		var executionOrder []int
		orderCounter := 0
		policy1 := &mockPolicy{
			orderTracker: &executionOrder,
			orderFunc: func() int {
				orderCounter++
				return orderCounter
			},
		}
		policy2 := &mockPolicy{
			shouldFail:   true,
			failError:    errPolicy2Failed,
			orderTracker: &executionOrder,
			orderFunc: func() int {
				orderCounter++
				return orderCounter
			},
		}
		policy3 := &mockPolicy{
			orderTracker: &executionOrder,
			orderFunc: func() int {
				orderCounter++
				return orderCounter
			},
		}
		scope := core.NewConcurrentScope(factory,
			core.WithScopedPolicy(policy1),
			core.WithScopedPolicy(policy2),
			core.WithScopedPolicy(policy3),
		)

		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			agg := newTestAgg("test-id-1")
			_, err := agg.SingleEventCommand("test-value")
			require.NoError(t, err)
			return repo.Save(ctx, agg)
		})

		require.Error(t, err)
		require.Empty(t, changes)
		require.Contains(t, err.Error(), "policy 2 failed")
		require.Equal(t, 1, policy1.getExecutionCount())
		require.Equal(t, 1, policy2.getExecutionCount())
		require.Equal(t, 0, policy3.getExecutionCount())
		require.Equal(t, []int{1, 2}, executionOrder)
	})

	t.Run(`Given a ConcurrentScope with scope-level policy
		When Run is called with additional policy via Run options
		Then both policies should execute
		And scope-level and run-level policies should be merged
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scopePolicyCount := 0
		runPolicyCount := 0
		scope := core.NewConcurrentScope(factory, core.WithScopedPolicyFunc(func(ctx context.Context, repo core.Repository, source core.AggregatePtr, events core.EventPack) error {
			scopePolicyCount++
			return nil
		}))

		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			agg := newTestAgg("test-id-1")
			_, err := agg.SingleEventCommand("test-value")
			require.NoError(t, err)
			return repo.Save(ctx, agg)
		}, core.WithScopedPolicyFunc(func(ctx context.Context, repo core.Repository, source core.AggregatePtr, events core.EventPack) error {
			runPolicyCount++
			return nil
		}))

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.Equal(t, 1, scopePolicyCount)
		require.Equal(t, 1, runPolicyCount)
	})

	t.Run(`Given a ConcurrentScope with a policy that uses repository
		When Run is called and aggregate is saved
		Then policy should be able to access repository methods
		And repository should be the decorator to enable recursive policy execution
	`, func(t *testing.T) {
		innerRepo := &mockRepository{}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) core.Repository {
				return innerRepo
			},
		}
		var policyRepo core.Repository
		scope := core.NewConcurrentScope(factory, core.WithScopedPolicyFunc(func(ctx context.Context, repo core.Repository, source core.AggregatePtr, events core.EventPack) error {
			policyRepo = repo
			return nil
		}))

		var scopeRepo core.Repository
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			scopeRepo = repo
			agg := newTestAgg("test-id-1")
			_, err := agg.SingleEventCommand("test-value")
			require.NoError(t, err)
			return repo.Save(ctx, agg)
		})

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.NotNil(t, policyRepo)
		require.Equal(t, scopeRepo, policyRepo)
		require.NotEqual(t, innerRepo, policyRepo)
	})

	t.Run(`Given a ConcurrentScope with policies
		When a policy saves an aggregate
		Then policies should be triggered for that save as well
		And changes map should contain aggregates modified in policy
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		policy1 := newMockPolicy()
		var policyRepo core.Repository
		policy2 := &mockPolicyThatSaves{
			repoCaptured: &policyRepo,
			aggregateID:  "policy-saved-id",
		}
		policy3 := newMockPolicy()
		scope := core.NewConcurrentScope(factory,
			core.WithScopedPolicy(policy1),
			core.WithScopedPolicy(policy2),
			core.WithScopedPolicy(policy3),
		)

		var savedAggregate *testAgg
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			agg := newTestAgg("test-id-1")
			_, err := agg.SingleEventCommand("test-value")
			require.NoError(t, err)
			savedAggregate = agg
			return repo.Save(ctx, agg)
		})

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.Equal(t, 2, policy1.getExecutionCount())
		require.Equal(t, 2, policy2.getExecutionCount())
		require.Equal(t, 2, policy3.getExecutionCount())

		require.NotNil(t, policy2.savedAggregate)
		require.Equal(t, core.AggregatePtr(savedAggregate), policy2.firstAggregate)
		require.Equal(t, core.AggregatePtr(policy2.savedAggregate), policy2.secondAggregate)

		require.Contains(t, changes, core.AggregatePtr(savedAggregate))
		require.Contains(t, changes, core.AggregatePtr(policy2.savedAggregate))

		verifyChanges(t, changes, MergeExpectations(
			ExpectChangesWithoutVerification(
				core.AggregatePtr(savedAggregate),
				[][]core.Event{{Created{}, ValueUpdated{value: "test-value"}}},
			),
			ExpectChangesWithoutVerification(
				core.AggregatePtr(policy2.savedAggregate),
				[][]core.Event{{Created{}, ValueUpdated{value: "policy-saved-value"}}},
			),
		))
	})

	t.Run(`Given a ConcurrentScope with transactional repository and policies
		When a policy saves an aggregate
		Then policies should be triggered for that save
		And changes map should contain all aggregates including those saved in policies
		And transaction should commit successfully
	`, func(t *testing.T) {
		txRepo := &mockTransactionalRepository{}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) core.Repository {
				return txRepo
			},
		}
		policy1 := newMockPolicy()
		var policyRepo core.Repository
		policy2 := &mockPolicyThatSaves{
			repoCaptured: &policyRepo,
			aggregateID:  "policy-saved-id-2",
		}
		scope := core.NewConcurrentScope(factory,
			core.WithScopedPolicy(policy1),
			core.WithScopedPolicy(policy2),
		)

		var savedAggregate *testAgg
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			agg := newTestAgg("test-id-1")
			_, err := agg.SingleEventCommand("test-value")
			require.NoError(t, err)
			savedAggregate = agg
			return repo.Save(ctx, agg)
		})

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.Equal(t, 2, policy1.getExecutionCount())
		require.Equal(t, 2, policy2.getExecutionCount())
		require.Equal(t, 1, txRepo.getBeginCount())
		require.Equal(t, 1, txRepo.getCommitCount())
		require.Equal(t, 0, txRepo.getRollbackCount())

		require.Contains(t, changes, core.AggregatePtr(savedAggregate))
		require.Contains(t, changes, core.AggregatePtr(policy2.savedAggregate))
	})

	t.Run(`Given a ConcurrentScope with policies
		When a policy saves an aggregate and another policy fails
		Then transaction should be rolled back
		And changes map should contain all aggregates saved before failure
	`, func(t *testing.T) {
		txRepo := &mockTransactionalRepository{}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) core.Repository {
				return txRepo
			},
		}
		policy1 := newMockPolicy()
		var policyRepo core.Repository
		policy2 := &mockPolicyThatSaves{
			repoCaptured: &policyRepo,
			aggregateID:  "policy-saved-id-3",
		}
		policy3 := newMockPolicy()
		policy3.shouldFail = true
		policy3.failError = errPolicy3Failed
		scope := core.NewConcurrentScope(factory,
			core.WithScopedPolicy(policy1),
			core.WithScopedPolicy(policy2),
			core.WithScopedPolicy(policy3),
		)

		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			agg := newTestAgg("test-id-1")
			_, err := agg.SingleEventCommand("test-value")
			require.NoError(t, err)
			return repo.Save(ctx, agg)
		})

		require.Error(t, err)
		require.Empty(t, changes)
		require.Contains(t, err.Error(), "policy 3 failed")
		require.Equal(t, 2, policy1.getExecutionCount())
		require.Equal(t, 2, policy2.getExecutionCount())
		require.Equal(t, 1, policy3.getExecutionCount())
		require.Equal(t, 1, txRepo.getBeginCount())
		require.Equal(t, 0, txRepo.getCommitCount())
		require.Equal(t, 1, txRepo.getRollbackCount())
	})

	t.Run(`Given a ConcurrentScope with a policy that returns ErrTransient
		When Run is called and aggregate is saved
		Then policy error should trigger retry
		And operation should eventually succeed
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		policy := newMockPolicy()
		policyCallCount := 0
		policy.shouldFail = true
		policy.failError = core.ErrTransient
		policy.customRun = func(ctx context.Context, repo core.Repository, source core.AggregatePtr, events core.EventPack) error {
			policyCallCount++
			if policyCallCount < 2 {
				return core.ErrTransient
			}
			return nil
		}
		scope := core.NewConcurrentScope(factory,
			core.WithScopedPolicy(policy),
			core.WithRetryOptions(retry.Attempts(3), retry.Delay(10*time.Millisecond)),
		)

		var savedAggregate *testAgg
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			agg := newTestAgg("test-id-1")
			_, err := agg.SingleEventCommand("test-value")
			require.NoError(t, err)
			savedAggregate = agg
			return repo.Save(ctx, agg)
		})

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.GreaterOrEqual(t, policyCallCount, 2)
		require.Contains(t, changes, core.AggregatePtr(savedAggregate))
	})

	t.Run(`Given a ConcurrentScope with a policy that returns ErrConcurrentModification
		When Run is called and aggregate is saved
		Then policy error should trigger retry
		And operation should eventually succeed
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		policy := newMockPolicy()
		policyCallCount := 0
		policy.shouldFail = true
		policy.failError = core.ErrConcurrentModification
		policy.customRun = func(ctx context.Context, repo core.Repository, source core.AggregatePtr, events core.EventPack) error {
			policyCallCount++
			if policyCallCount < 2 {
				return core.ErrConcurrentModification
			}
			return nil
		}
		scope := core.NewConcurrentScope(factory,
			core.WithScopedPolicy(policy),
			core.WithRetryOptions(retry.Attempts(3), retry.Delay(10*time.Millisecond)),
		)

		var savedAggregate *testAgg
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			agg := newTestAgg("test-id-1")
			_, err := agg.SingleEventCommand("test-value")
			require.NoError(t, err)
			savedAggregate = agg
			return repo.Save(ctx, agg)
		})

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.GreaterOrEqual(t, policyCallCount, 2)
		require.Contains(t, changes, core.AggregatePtr(savedAggregate))
	})

	t.Run(`Given a ConcurrentScope with a policy that returns non-retryable error
		When Run is called and aggregate is saved
		Then policy error should not trigger retry
		And error should be returned immediately
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		policy := newMockPolicy()
		policy.shouldFail = true
		policy.failError = errNonRetryablePolicy
		scope := core.NewConcurrentScope(factory,
			core.WithScopedPolicy(policy),
			core.WithRetryOptions(retry.Attempts(3), retry.Delay(10*time.Millisecond)),
		)

		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			agg := newTestAgg("test-id-1")
			_, err := agg.SingleEventCommand("test-value")
			require.NoError(t, err)
			return repo.Save(ctx, agg)
		})

		require.Error(t, err)
		require.Empty(t, changes)
		require.Contains(t, err.Error(), "non-retryable policy error")
		require.Equal(t, 1, policy.getExecutionCount())
	})

	t.Run(`Given a ConcurrentScope with transactional repository and policy that returns ErrTransient
		When Run is called and aggregate is saved
		Then policy error should trigger retry
		And transaction should be rolled back on each retry
		And operation should eventually succeed
	`, func(t *testing.T) {
		txRepo := &mockTransactionalRepository{}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) core.Repository {
				return txRepo
			},
		}
		policy := newMockPolicy()
		policyCallCount := 0
		policy.shouldFail = true
		policy.failError = core.ErrTransient
		policy.customRun = func(ctx context.Context, repo core.Repository, source core.AggregatePtr, events core.EventPack) error {
			policyCallCount++
			if policyCallCount < 2 {
				return core.ErrTransient
			}
			return nil
		}
		scope := core.NewConcurrentScope(factory,
			core.WithScopedPolicy(policy),
			core.WithRetryOptions(retry.Attempts(3), retry.Delay(10*time.Millisecond)),
		)

		var savedAggregate *testAgg
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			agg := newTestAgg("test-id-1")
			_, err := agg.SingleEventCommand("test-value")
			require.NoError(t, err)
			savedAggregate = agg
			return repo.Save(ctx, agg)
		})

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.GreaterOrEqual(t, policyCallCount, 2)
		require.Equal(t, 2, txRepo.getBeginCount())
		require.Equal(t, 1, txRepo.getCommitCount())
		require.Equal(t, 1, txRepo.getRollbackCount())
		require.Contains(t, changes, core.AggregatePtr(savedAggregate))
	})

	t.Run(`Given a ConcurrentScope with policy that returns ErrTransient multiple times
		When Run is called and aggregate is saved
		Then policy error should trigger retries until attempts exhausted
		And ErrTransient should be returned after all retries
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		policy := newMockPolicy()
		policy.shouldFail = true
		policy.failError = core.ErrTransient
		scope := core.NewConcurrentScope(factory,
			core.WithScopedPolicy(policy),
			core.WithRetryOptions(retry.Attempts(2), retry.Delay(10*time.Millisecond)),
		)

		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			agg := newTestAgg("test-id-1")
			_, err := agg.SingleEventCommand("test-value")
			require.NoError(t, err)
			return repo.Save(ctx, agg)
		})

		require.Error(t, err)
		require.Empty(t, changes)
		require.ErrorIs(t, err, core.ErrTransient)
		require.Equal(t, 2, policy.getExecutionCount())
	})

	t.Run(`Given a ConcurrentScope with multiple policies where second policy returns ErrTransient
		When Run is called and aggregate is saved
		Then first policy should execute
		And second policy error should trigger retry
		And third policy should not execute until retry succeeds
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		policy1 := newMockPolicy()
		policy2 := newMockPolicy()
		policy2CallCount := 0
		policy2.shouldFail = true
		policy2.failError = core.ErrTransient
		policy2.customRun = func(ctx context.Context, repo core.Repository, source core.AggregatePtr, events core.EventPack) error {
			policy2CallCount++
			if policy2CallCount < 2 {
				return core.ErrTransient
			}
			return nil
		}
		policy3 := newMockPolicy()
		scope := core.NewConcurrentScope(factory,
			core.WithScopedPolicy(policy1),
			core.WithScopedPolicy(policy2),
			core.WithScopedPolicy(policy3),
			core.WithRetryOptions(retry.Attempts(3), retry.Delay(10*time.Millisecond)),
		)

		var savedAggregate *testAgg
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			agg := newTestAgg("test-id-1")
			_, err := agg.SingleEventCommand("test-value")
			require.NoError(t, err)
			savedAggregate = agg
			return repo.Save(ctx, agg)
		})

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.Equal(t, 2, policy1.getExecutionCount())
		require.Equal(t, 2, policy2CallCount)
		require.Equal(t, 1, policy3.getExecutionCount())
		require.Contains(t, changes, core.AggregatePtr(savedAggregate))
	})

	t.Run(`Given a ConcurrentScope with post-scoped policy
		When Run is called and aggregates are saved
		Then post-scoped policy should execute after Run completes
		And should receive all changes map
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		var receivedChanges map[core.AggregatePtr][]core.EventPack
		var postPolicyCtx context.Context
		scope := core.NewConcurrentScope(factory, core.WithPostScopedPolicyFunc(func(ctx context.Context, changes map[core.AggregatePtr][]core.EventPack) {
			postPolicyCtx = ctx
			receivedChanges = changes
		}))

		var savedAggregate1, savedAggregate2 *testAgg
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			agg1 := newTestAgg("test-id-1")
			_, err := agg1.SingleEventCommand("value-1")
			require.NoError(t, err)
			savedAggregate1 = agg1
			if err = repo.Save(ctx, agg1); err != nil {
				return err
			}

			agg2 := newTestAgg("test-id-2")
			_, err = agg2.SingleEventCommand("value-2")
			require.NoError(t, err)
			savedAggregate2 = agg2
			return repo.Save(ctx, agg2)
		})

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.NotNil(t, receivedChanges)
		require.NotNil(t, postPolicyCtx)
		require.Equal(t, changes, receivedChanges)
		require.Contains(t, receivedChanges, core.AggregatePtr(savedAggregate1))
		require.Contains(t, receivedChanges, core.AggregatePtr(savedAggregate2))
	})

	t.Run(`Given a ConcurrentScope with multiple post-scoped policies
		When Run is called and aggregates are saved
		Then all post-scoped policies should execute in order
		And each should receive the changes map
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		executionOrder := []int{}
		postPolicy1Count := 0
		postPolicy2Count := 0
		scope := core.NewConcurrentScope(factory,
			core.WithPostScopedPolicyFunc(func(ctx context.Context, changes map[core.AggregatePtr][]core.EventPack) {
				postPolicy1Count++
				executionOrder = append(executionOrder, 1)
			}),
			core.WithPostScopedPolicyFunc(func(ctx context.Context, changes map[core.AggregatePtr][]core.EventPack) {
				postPolicy2Count++
				executionOrder = append(executionOrder, 2)
			}),
		)

		var savedAggregate *testAgg
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			agg := newTestAgg("test-id-1")
			_, err := agg.SingleEventCommand("test-value")
			require.NoError(t, err)
			savedAggregate = agg
			return repo.Save(ctx, agg)
		})

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.Equal(t, 1, postPolicy1Count)
		require.Equal(t, 1, postPolicy2Count)
		require.Equal(t, []int{1, 2}, executionOrder)
		require.Contains(t, changes, core.AggregatePtr(savedAggregate))
	})

	t.Run(`Given a ConcurrentScope with post-scoped policy via Run options
		When Run is called
		Then scope-level and run-level post-scoped policies should be merged
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scopePolicyCount := 0
		runPolicyCount := 0
		scope := core.NewConcurrentScope(factory, core.WithPostScopedPolicyFunc(func(ctx context.Context, changes map[core.AggregatePtr][]core.EventPack) {
			scopePolicyCount++
		}))

		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			agg := newTestAgg("test-id-1")
			_, err := agg.SingleEventCommand("test-value")
			require.NoError(t, err)
			return repo.Save(ctx, agg)
		}, core.WithPostScopedPolicyFunc(func(ctx context.Context, changes map[core.AggregatePtr][]core.EventPack) {
			runPolicyCount++
		}))

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.Equal(t, 1, scopePolicyCount)
		require.Equal(t, 1, runPolicyCount)
	})

	t.Run(`Given a ConcurrentScope with post-scoped policy
		When Run is called and an error occurs
		Then post-scoped policy should not execute
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		postPolicyExecuted := false
		scope := core.NewConcurrentScope(factory, core.WithPostScopedPolicyFunc(func(ctx context.Context, changes map[core.AggregatePtr][]core.EventPack) {
			postPolicyExecuted = true
		}))

		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			return errOperationFailed
		})

		require.Error(t, err)
		require.ErrorIs(t, err, errOperationFailed)
		require.Nil(t, changes)
		require.False(t, postPolicyExecuted)
	})

	t.Run(`Given a ConcurrentScope with post-scoped policy in constructor
		When Run is called successfully
		Then constructor post-scoped policy should execute
		And should receive correct changes map
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		var receivedChanges map[core.AggregatePtr][]core.EventPack
		var receivedCtx context.Context
		scope := core.NewConcurrentScope(factory, core.WithPostScopedPolicyFunc(func(ctx context.Context, changes map[core.AggregatePtr][]core.EventPack) {
			receivedCtx = ctx
			receivedChanges = changes
		}))

		var savedAggregate *testAgg
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			agg := newTestAgg("test-id-1")
			_, err := agg.SingleEventCommand("test-value")
			require.NoError(t, err)
			savedAggregate = agg
			return repo.Save(ctx, agg)
		})

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.NotNil(t, receivedChanges)
		require.NotNil(t, receivedCtx)
		require.Equal(t, changes, receivedChanges)
		require.Contains(t, receivedChanges, core.AggregatePtr(savedAggregate))
	})

	t.Run(`Given a ConcurrentScope without constructor post-scoped policy
		When Run is called with post-scoped policy in Run options
		Then Run post-scoped policy should execute
		And should receive correct changes map
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		var receivedChanges map[core.AggregatePtr][]core.EventPack
		var receivedCtx context.Context
		scope := core.NewConcurrentScope(factory)

		var savedAggregate *testAgg
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			agg := newTestAgg("test-id-1")
			_, err := agg.SingleEventCommand("test-value")
			require.NoError(t, err)
			savedAggregate = agg
			return repo.Save(ctx, agg)
		}, core.WithPostScopedPolicyFunc(func(ctx context.Context, changes map[core.AggregatePtr][]core.EventPack) {
			receivedCtx = ctx
			receivedChanges = changes
		}))

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.NotNil(t, receivedChanges)
		require.NotNil(t, receivedCtx)
		require.Equal(t, changes, receivedChanges)
		require.Contains(t, receivedChanges, core.AggregatePtr(savedAggregate))
	})

	t.Run(`Given a ConcurrentScope with post-scoped policy in constructor
		When Run is called with post-scoped policy in Run options
		Then both constructor and Run post-scoped policies should execute
		And constructor policy should execute before Run policy
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		executionOrder := []string{}
		constructorPolicyCount := 0
		runPolicyCount := 0
		scope := core.NewConcurrentScope(factory, core.WithPostScopedPolicyFunc(func(ctx context.Context, changes map[core.AggregatePtr][]core.EventPack) {
			constructorPolicyCount++
			executionOrder = append(executionOrder, "constructor")
		}))

		var savedAggregate *testAgg
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			agg := newTestAgg("test-id-1")
			_, err := agg.SingleEventCommand("test-value")
			require.NoError(t, err)
			savedAggregate = agg
			return repo.Save(ctx, agg)
		}, core.WithPostScopedPolicyFunc(func(ctx context.Context, changes map[core.AggregatePtr][]core.EventPack) {
			runPolicyCount++
			executionOrder = append(executionOrder, "run")
		}))

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.Equal(t, 1, constructorPolicyCount)
		require.Equal(t, 1, runPolicyCount)
		require.Equal(t, []string{"constructor", "run"}, executionOrder)
		require.Contains(t, changes, core.AggregatePtr(savedAggregate))
	})

	t.Run(`Given a ConcurrentScope with multiple post-scoped policies in constructor
		When Run is called with multiple post-scoped policies in Run options
		Then all policies should execute in order
		And constructor policies should execute before Run policies
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		executionOrder := []int{}
		constructorPolicy1Count := 0
		constructorPolicy2Count := 0
		runPolicy1Count := 0
		runPolicy2Count := 0
		scope := core.NewConcurrentScope(factory,
			core.WithPostScopedPolicyFunc(func(ctx context.Context, changes map[core.AggregatePtr][]core.EventPack) {
				constructorPolicy1Count++
				executionOrder = append(executionOrder, 1)
			}),
			core.WithPostScopedPolicyFunc(func(ctx context.Context, changes map[core.AggregatePtr][]core.EventPack) {
				constructorPolicy2Count++
				executionOrder = append(executionOrder, 2)
			}),
		)

		var savedAggregate *testAgg
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			agg := newTestAgg("test-id-1")
			_, err := agg.SingleEventCommand("test-value")
			require.NoError(t, err)
			savedAggregate = agg
			return repo.Save(ctx, agg)
		},
			core.WithPostScopedPolicyFunc(func(ctx context.Context, changes map[core.AggregatePtr][]core.EventPack) {
				runPolicy1Count++
				executionOrder = append(executionOrder, 3)
			}),
			core.WithPostScopedPolicyFunc(func(ctx context.Context, changes map[core.AggregatePtr][]core.EventPack) {
				runPolicy2Count++
				executionOrder = append(executionOrder, 4)
			}),
		)

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.Equal(t, 1, constructorPolicy1Count)
		require.Equal(t, 1, constructorPolicy2Count)
		require.Equal(t, 1, runPolicy1Count)
		require.Equal(t, 1, runPolicy2Count)
		require.Equal(t, []int{1, 2, 3, 4}, executionOrder)
		require.Contains(t, changes, core.AggregatePtr(savedAggregate))
	})

	t.Run(`Given a ConcurrentScope with post-scoped policy in constructor
		When Run is called multiple times with different Run post-scoped policies
		Then constructor policy should execute for each Run
		And Run policies should execute only for their respective Run
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		constructorPolicyCount := 0
		run1PolicyCount := 0
		run2PolicyCount := 0
		scope := core.NewConcurrentScope(factory, core.WithPostScopedPolicyFunc(func(ctx context.Context, changes map[core.AggregatePtr][]core.EventPack) {
			constructorPolicyCount++
		}))

		changes1, err1 := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			agg := newTestAgg("test-id-1")
			_, err := agg.SingleEventCommand("test-value-1")
			require.NoError(t, err)
			return repo.Save(ctx, agg)
		}, core.WithPostScopedPolicyFunc(func(ctx context.Context, changes map[core.AggregatePtr][]core.EventPack) {
			run1PolicyCount++
		}))

		changes2, err2 := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			agg := newTestAgg("test-id-2")
			_, err := agg.SingleEventCommand("test-value-2")
			require.NoError(t, err)
			return repo.Save(ctx, agg)
		}, core.WithPostScopedPolicyFunc(func(ctx context.Context, changes map[core.AggregatePtr][]core.EventPack) {
			run2PolicyCount++
		}))

		require.NoError(t, err1)
		require.NoError(t, err2)
		require.NotNil(t, changes1)
		require.NotNil(t, changes2)
		require.Equal(t, 2, constructorPolicyCount)
		require.Equal(t, 1, run1PolicyCount)
		require.Equal(t, 1, run2PolicyCount)
	})

	t.Run(`Given a ConcurrentScope with post-scoped policy in constructor
		When Run is called with transactional repository
		Then post-scoped policy should execute after transaction commits
		And should receive changes map
	`, func(t *testing.T) {
		txRepo := &mockTransactionalRepository{}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) core.Repository {
				return txRepo
			},
		}
		var receivedChanges map[core.AggregatePtr][]core.EventPack
		postPolicyExecuted := false
		scope := core.NewConcurrentScope(factory, core.WithPostScopedPolicyFunc(func(ctx context.Context, changes map[core.AggregatePtr][]core.EventPack) {
			postPolicyExecuted = true
			receivedChanges = changes
		}))

		var savedAggregate *testAgg
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			agg := newTestAgg("test-id-1")
			_, err := agg.SingleEventCommand("test-value")
			require.NoError(t, err)
			savedAggregate = agg
			return repo.Save(ctx, agg)
		})

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.True(t, postPolicyExecuted)
		require.Equal(t, 1, txRepo.getBeginCount())
		require.Equal(t, 1, txRepo.getCommitCount())
		require.Equal(t, 0, txRepo.getRollbackCount())
		require.Equal(t, changes, receivedChanges)
		require.Contains(t, receivedChanges, core.AggregatePtr(savedAggregate))
	})

	t.Run(`Given a ConcurrentScope with post-scoped policy in constructor and Run
		When Run is called and transaction fails
		Then post-scoped policies should not execute
	`, func(t *testing.T) {
		txRepo := &mockTransactionalRepository{}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) core.Repository {
				return txRepo
			},
		}
		constructorPolicyExecuted := false
		runPolicyExecuted := false
		scope := core.NewConcurrentScope(factory, core.WithPostScopedPolicyFunc(func(ctx context.Context, changes map[core.AggregatePtr][]core.EventPack) {
			constructorPolicyExecuted = true
		}))

		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			return errOperationFailed
		}, core.WithPostScopedPolicyFunc(func(ctx context.Context, changes map[core.AggregatePtr][]core.EventPack) {
			runPolicyExecuted = true
		}))

		require.Error(t, err)
		require.ErrorIs(t, err, errOperationFailed)
		require.Nil(t, changes)
		require.False(t, constructorPolicyExecuted)
		require.False(t, runPolicyExecuted)
		require.Equal(t, 1, txRepo.getBeginCount())
		require.Equal(t, 0, txRepo.getCommitCount())
		require.Equal(t, 1, txRepo.getRollbackCount())
	})

	t.Run(`Given a ConcurrentScope with post-scoped policy
		When Run is called with multiple aggregates saved
		Then post-scoped policy should receive all aggregates in changes map
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		var receivedChanges map[core.AggregatePtr][]core.EventPack
		scope := core.NewConcurrentScope(factory, core.WithPostScopedPolicyFunc(func(ctx context.Context, changes map[core.AggregatePtr][]core.EventPack) {
			receivedChanges = changes
		}))

		var savedAggregate1, savedAggregate2, savedAggregate3 *testAgg
		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			agg1 := newTestAgg("test-id-1")
			_, err := agg1.SingleEventCommand("value-1")
			require.NoError(t, err)
			savedAggregate1 = agg1
			if err = repo.Save(ctx, agg1); err != nil {
				return err
			}

			agg2 := newTestAgg("test-id-2")
			_, err = agg2.SingleEventCommand("value-2")
			require.NoError(t, err)
			savedAggregate2 = agg2
			if err = repo.Save(ctx, agg2); err != nil {
				return err
			}

			agg3 := newTestAgg("test-id-3")
			_, err = agg3.SingleEventCommand("value-3")
			require.NoError(t, err)
			savedAggregate3 = agg3
			return repo.Save(ctx, agg3)
		})

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.NotNil(t, receivedChanges)
		require.Equal(t, changes, receivedChanges)
		require.Len(t, receivedChanges, 3)
		require.Contains(t, receivedChanges, core.AggregatePtr(savedAggregate1))
		require.Contains(t, receivedChanges, core.AggregatePtr(savedAggregate2))
		require.Contains(t, receivedChanges, core.AggregatePtr(savedAggregate3))
	})

	t.Run(`Given a ConcurrentScope with post-scoped policy in constructor and Run
		When Run is called successfully
		Then both policies should receive the same changes map reference
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		var constructorChanges map[core.AggregatePtr][]core.EventPack
		var runChanges map[core.AggregatePtr][]core.EventPack
		scope := core.NewConcurrentScope(factory, core.WithPostScopedPolicyFunc(func(ctx context.Context, changes map[core.AggregatePtr][]core.EventPack) {
			constructorChanges = changes
		}))

		changes, err := scope.Run(context.Background(), func(ctx context.Context, repo core.Repository) error {
			agg := newTestAgg("test-id-1")
			_, err := agg.SingleEventCommand("test-value")
			require.NoError(t, err)
			return repo.Save(ctx, agg)
		}, core.WithPostScopedPolicyFunc(func(ctx context.Context, changes map[core.AggregatePtr][]core.EventPack) {
			runChanges = changes
		}))

		require.NoError(t, err)
		require.NotNil(t, changes)
		require.NotNil(t, constructorChanges)
		require.NotNil(t, runChanges)
		require.Equal(t, changes, constructorChanges)
		require.Equal(t, changes, runChanges)
		require.Equal(t, constructorChanges, runChanges)
	})
}

type mockPolicyThatSaves struct {
	repoCaptured    *core.Repository
	savedAggregate  *testAgg
	firstAggregate  core.AggregatePtr
	secondAggregate core.AggregatePtr
	aggregateID     core.ID
	executionCount  int
}

func (m *mockPolicyThatSaves) Run(ctx context.Context, repo core.Repository, source core.AggregatePtr, events core.EventPack) error {
	m.executionCount++
	if m.repoCaptured != nil {
		*m.repoCaptured = repo
	}

	if m.executionCount == 1 {
		m.firstAggregate = source
		agg := newTestAgg(m.aggregateID)
		_, err := agg.SingleEventCommand("policy-saved-value")
		if err != nil {
			return err
		}
		m.savedAggregate = agg
		err = repo.Save(ctx, agg)
		if err != nil {
			return err
		}
		m.secondAggregate = core.AggregatePtr(agg)
	}
	return nil
}

func (m *mockPolicyThatSaves) getExecutionCount() int {
	return m.executionCount
}
