package core

import (
	"context"
	"errors"
	"time"

	"github.com/avast/retry-go/v4"
)

const defaultRollbackTimeout = 5 * time.Second

// ConcurrentScope manages repository operations with automatic retry, transaction handling, and change tracking.
// It provides a safe way to execute repository operations that may need retries on transient errors
// or concurrent modification conflicts.
//
// IMPORTANT: Repositories should never be used directly. They must only be accessed through ConcurrentScope.
// Direct repository usage bypasses essential features like automatic retries, transaction management, and change tracking.
//
// Example:
//   repoFactory := redispkg.NewRepositoryFactory(redisClient)
//   scope := core.NewConcurrentScope(repoFactory)
//
//   changes, err := scope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
//       session := &domain.Session{}
//       err := repo.Load(ctx, sessionID, session)
//       if err != nil {
//           return err
//       }
//
//       events, err := session.ProcessRequest(now)
//       if err != nil {
//           return err
//       }
//
//       return repo.Save(ctx, session, redis.WithExpiration(24*time.Hour))
//   })
//
//   if err != nil {
//       return err
//   }
//
//   // Extract events from changes map for side effects
//   for aggPtr, eventPacks := range changes {
//       for _, pack := range eventPacks {
//           refreshEvent, err := core.EventOfType[domain.RefreshQueued](pack)
//           if err == nil {
//               // Queue refresh operation
//           }
//       }
//   }
type ConcurrentScope struct {
	factory         RepositoryFactory
	retryOpts       []retry.Option
	rollbackTimeout time.Duration
}

// NewConcurrentScope creates a new ConcurrentScope with the given repository factory.
// By default, the scope will automatically retry operations on ErrTransient or ErrConcurrentModification errors.
// The default rollback timeout is 5 seconds.
//
// Options can be provided to customize retry behavior and rollback timeout:
//   - WithRetryOptions: Configure retry attempts, delays, and conditions
//   - WithRollbackTimeout: Set custom timeout for transaction rollback
//
// Example with default options:
//   repoFactory := redispkg.NewRepositoryFactory(redisClient)
//   scope := core.NewConcurrentScope(repoFactory)
//
// Example with custom retry options:
//   scope := core.NewConcurrentScope(repoFactory,
//       core.WithRetryOptions(
//           retry.Attempts(5),
//           retry.Delay(100*time.Millisecond),
//       ),
//   )
//
// Example with custom rollback timeout:
//   scope := core.NewConcurrentScope(repoFactory,
//       core.WithRollbackTimeout(10*time.Second),
//   )
func NewConcurrentScope(factory RepositoryFactory, runOptions ...RunOptions) *ConcurrentScope {
	retryIf := retry.RetryIf(func(err error) bool { return errors.Is(err, ErrTransient) || errors.Is(err, ErrConcurrentModification) })
	retryOpts := []retry.Option{retryIf}
	rollbackTimeout := defaultRollbackTimeout
	for _, opt := range runOptions {
		if opts, ok := opt.(retryOptions); ok {
			retryOpts = append(retryOpts, opts.retryOptions...)
		}
		if opts, ok := opt.(rollbackTimeoutOption); ok {
			rollbackTimeout = opts.rollbackTimeout
		}
	}
	return &ConcurrentScope{
		factory:         factory,
		retryOpts:       retryOpts,
		rollbackTimeout: rollbackTimeout,
	}
}

// RunOptions is a type for options that can be passed to NewConcurrentScope or Run.
// Use WithRetryOptions and WithRollbackTimeout to create options.
type RunOptions any

type retryOptions struct {
	retryOptions []retry.Option
}

type rollbackTimeoutOption struct {
	rollbackTimeout time.Duration
}

// WithRollbackTimeout sets a custom timeout for transaction rollback operations.
// If a transaction needs to be rolled back, this timeout ensures the rollback operation
// doesn't block indefinitely, even if the original context is cancelled.
//
// Example:
//   scope := core.NewConcurrentScope(repoFactory,
//       core.WithRollbackTimeout(10*time.Second),
//   )
func WithRollbackTimeout(timeout time.Duration) RunOptions {
	return rollbackTimeoutOption{rollbackTimeout: timeout}
}

// WithRetryOptions configures retry behavior for the scope.
// These options are merged with the default retry configuration that retries on
// ErrTransient and ErrConcurrentModification errors.
//
// Example:
//   scope := core.NewConcurrentScope(repoFactory,
//       core.WithRetryOptions(
//           retry.Attempts(5),
//           retry.Delay(100*time.Millisecond),
//           retry.MaxDelay(1*time.Second),
//       ),
//   )
//
// Retry options can also be provided per-run:
//   changes, err := scope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
//       // Repository operations
//   }, core.WithRetryOptions(retry.Attempts(3)))
func WithRetryOptions(opts ...retry.Option) RunOptions {
	return retryOptions{retryOptions: opts}
}

type repoDecorator struct {
	inner   Repository
	changes map[AggregatePtr][]EventPack
}

var _ Repository = (*repoDecorator)(nil)

func (c *repoDecorator) Load(ctx context.Context, id ID, restorer Restorer, options ...LoadOption) error {
	return c.inner.Load(ctx, id, restorer, options...)
}

func (c *repoDecorator) Save(ctx context.Context, storer Storer, options ...SaveOption) error {
	decoratedStorer := &storerDecorator{
		Storer: storer,
	}
	err := c.inner.Save(ctx, decoratedStorer, options...)
	if err != nil {
		return err
	}
	aggregatePtr := AggregatePtr(storer)
	c.changes[aggregatePtr] = append(c.changes[aggregatePtr], decoratedStorer.pack)
	return nil
}

type storerDecorator struct {
	Storer
	pack EventPack
}

var _ Storer = (*storerDecorator)(nil)

func (c *storerDecorator) Store(storeFunc func(id ID, aggregate AggregatePtr, storageState StatePtr, events EventPack, version Version, schemaVersion SchemaVersion) error) error {
	return c.Storer.Store(func(id ID, aggregate AggregatePtr, storageState StatePtr, events EventPack, version Version, schemaVersion SchemaVersion) error {
		err := storeFunc(id, aggregate, storageState, events, version, schemaVersion)
		if err != nil {
			return err
		}
		c.pack = events
		return nil
	})
}

// Run executes a function with repository access, handling transactions, retries, and change tracking.
//
// The function receives a context and a Repository instance. The context may be enriched with
// transaction information for transactional repositories. Always use the context parameter
// from the lambda function for all operations inside the Run block.
//
// Features:
//   - Automatic retries on ErrTransient or ErrConcurrentModification errors
//   - Transaction management for repositories implementing Transactional interface
//   - Change tracking: returns a map of all aggregates modified during execution
//   - Automatic rollback on errors with configurable timeout
//
// The returned changes map contains all aggregates that were saved during execution,
// grouped by aggregate pointer, with their event packs for each save operation.
//
// IMPORTANT: Always reuse the ctx parameter from the lambda function for all operations
// inside the Run block. The context is enriched with scope-specific data (such as transaction
// references for transactional repositories). Using the original context instead of the lambda's
// context will cause operations to execute outside the scope's transaction, leading to incorrect behavior.
//
// Example:
//   changes, err := scope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
//       session := &domain.Session{}
//       err := repo.Load(ctx, sessionID, session)
//       if err != nil {
//           return err
//       }
//
//       events, err := session.ProcessRequest(now)
//       if err != nil {
//           return err
//       }
//
//       return repo.Save(ctx, session, redis.WithExpiration(24*time.Hour))
//   })
//
//   if err != nil {
//       return err
//   }
//
//   // Process events from changes
//   for aggPtr, eventPacks := range changes {
//       for _, pack := range eventPacks {
//           refreshEvent, err := core.EventOfType[domain.RefreshQueued](pack)
//           if err == nil {
//               worker.QueueRefresh(refreshEvent.RefreshToken)
//           }
//       }
//   }
//
// Example with per-run retry options:
//   changes, err := scope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
//       // Repository operations
//   }, core.WithRetryOptions(retry.Attempts(3), retry.Delay(50*time.Millisecond)))
func (c *ConcurrentScope) Run(ctx context.Context, runFunc func(ctx context.Context, repo Repository) error, runOptions ...RunOptions) (map[AggregatePtr][]EventPack, error) {
	retryOpts := c.retryOpts
	for _, opt := range runOptions {
		if opts, ok := opt.(retryOptions); ok {
			retryOpts = append(retryOpts, opts.retryOptions...)
		}
	}
	var changes map[AggregatePtr][]EventPack
	return changes, retry.Do(
		func() error {
			changes = make(map[AggregatePtr][]EventPack)
			repo := &repoDecorator{
				changes: changes,
				inner:   c.factory.Create(ctx),
			}
			if transactional, ok := repo.inner.(Transactional); ok {
				var err error
				ctx, err = transactional.Begin(ctx)
				if err != nil {
					return err
				}
				err = runFunc(ctx, repo)
				if err != nil {
					rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.rollbackTimeout)
					defer cancel()
					rollbackErr := transactional.Rollback(rollbackCtx)
					return errors.Join(err, rollbackErr)
				}
				return transactional.Commit(ctx)
			}
			return runFunc(ctx, repo)
		},
		retryOpts...,
	)
}
