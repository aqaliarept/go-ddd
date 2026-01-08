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
//
//	repoFactory := redispkg.NewRepositoryFactory(redisClient)
//	scope := core.NewConcurrentScope(repoFactory)
//
//	changes, err := scope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
//	    session := &domain.Session{}
//	    err := repo.Load(ctx, sessionID, session)
//	    if err != nil {
//	        return err
//	    }
//
//	    events, err := session.ProcessRequest(now)
//	    if err != nil {
//	        return err
//	    }
//
//	    return repo.Save(ctx, session, redis.WithExpiration(24*time.Hour))
//	})
//
//	if err != nil {
//	    return err
//	}
//
//	// Extract events from changes map for side effects
//	for aggPtr, eventPacks := range changes {
//	    for _, pack := range eventPacks {
//	        refreshEvent, err := core.EventOfType[domain.RefreshQueued](pack)
//	        if err == nil {
//	            // Queue refresh operation
//	        }
//	    }
//	}
type ConcurrentScope struct {
	factory         RepositoryFactory
	retryOpts       []retry.Option
	policies        []ScopedPolicy
	postPolicies    []PostScopedPolicy
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
//
//	repoFactory := redispkg.NewRepositoryFactory(redisClient)
//	scope := core.NewConcurrentScope(repoFactory)
//
// Example with custom retry options:
//
//	scope := core.NewConcurrentScope(repoFactory,
//	    core.WithRetryOptions(
//	        retry.Attempts(5),
//	        retry.Delay(100*time.Millisecond),
//	    ),
//	)
//
// Example with custom rollback timeout:
//
//	scope := core.NewConcurrentScope(repoFactory,
//	    core.WithRollbackTimeout(10*time.Second),
//	)
func NewConcurrentScope(factory RepositoryFactory, runOptions ...RunOptions) *ConcurrentScope {
	retryIf := retry.RetryIf(func(err error) bool { return errors.Is(err, ErrTransient) || errors.Is(err, ErrConcurrentModification) })
	initialRetryOpts := []retry.Option{retryIf}
	opts := processRunOptions(initialRetryOpts, defaultRollbackTimeout, []ScopedPolicy{}, []PostScopedPolicy{}, runOptions...)
	return &ConcurrentScope{
		factory:         factory,
		retryOpts:       opts.retryOpts,
		rollbackTimeout: opts.rollbackTimeout,
		policies:        opts.policies,
		postPolicies:    opts.postPolicies,
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

type processedRunOptions struct {
	retryOpts       []retry.Option
	policies        []ScopedPolicy
	postPolicies    []PostScopedPolicy
	rollbackTimeout time.Duration
}

func processRunOptions(initialRetryOpts []retry.Option, initialRollbackTimeout time.Duration, initialPolicies []ScopedPolicy, initialPostPolicies []PostScopedPolicy, runOptions ...RunOptions) processedRunOptions {
	retryOpts := append(make([]retry.Option, 0, len(initialRetryOpts)), initialRetryOpts...)
	policies := append(make([]ScopedPolicy, 0, len(initialPolicies)), initialPolicies...)
	postPolicies := append(make([]PostScopedPolicy, 0, len(initialPostPolicies)), initialPostPolicies...)
	rollbackTimeout := initialRollbackTimeout
	for _, opt := range runOptions {
		if opts, ok := opt.(retryOptions); ok {
			retryOpts = append(retryOpts, opts.retryOptions...)
		}
		if opts, ok := opt.(rollbackTimeoutOption); ok {
			rollbackTimeout = opts.rollbackTimeout
		}
		if opts, ok := opt.(policyOption); ok {
			policies = append(policies, opts.policy)
		}
		if opts, ok := opt.(postScopedPolicyOption); ok {
			postPolicies = append(postPolicies, opts.policy)
		}
	}
	return processedRunOptions{
		retryOpts:       retryOpts,
		rollbackTimeout: rollbackTimeout,
		policies:        policies,
		postPolicies:    postPolicies,
	}
}

// ScopedPolicy is a policy that executes immediately after an aggregate is saved,
// within the same transaction or scope. Policies receive the repository, aggregate pointer,
// and events that were saved, allowing them to perform side effects or additional operations.
//
// If a policy returns an error, the save operation fails and the transaction is rolled back
// (if applicable). Policy errors that are ErrTransient or ErrConcurrentModification will
// trigger retries according to the scope's retry configuration.
//
// Policies execute in the order they are added, and if a policy saves an aggregate,
// all policies are triggered recursively for that save as well.
type ScopedPolicy interface {
	Run(ctx context.Context, repo Repository, source AggregatePtr, events EventPack) error
}

// WithScopedPolicy adds a scoped policy to the ConcurrentScope or Run options.
// The policy will execute after each aggregate save operation within the scope.
//
// Example:
//
//	scope := core.NewConcurrentScope(factory,
//	    core.WithScopedPolicy(myPolicy),
//	)
func WithScopedPolicy(policy ScopedPolicy) RunOptions {
	return policyOption{policy: policy}
}

// WithScopedPolicyFunc adds a scoped policy using a function literal.
// This is a convenience function for creating policies inline without implementing
// the ScopedPolicy interface.
//
// Example:
//
//	scope := core.NewConcurrentScope(factory,
//	    core.WithScopedPolicyFunc(func(ctx context.Context, repo core.Repository, source core.AggregatePtr, events core.EventPack) error {
//	        // Policy logic here
//	        return nil
//	    }),
//	)
func WithScopedPolicyFunc(policyFunc func(ctx context.Context, repo Repository, source AggregatePtr, events EventPack) error) RunOptions {
	return policyOption{policy: scopedPolicyFunc(policyFunc)}
}

type scopedPolicyFunc func(ctx context.Context, repo Repository, source AggregatePtr, events EventPack) error

func (f scopedPolicyFunc) Run(ctx context.Context, repo Repository, source AggregatePtr, events EventPack) error {
	return f(ctx, repo, source, events)
}

type policyOption struct {
	policy ScopedPolicy
}

// PostScopedPolicy is a policy that executes after the Run function completes successfully.
// Post-scoped policies receive the complete changes map containing all aggregates that were
// saved during the execution, grouped by aggregate pointer with their event packs.
//
// Post-scoped policies execute only if Run completes without errors. If Run fails or is retried,
// post-scoped policies do not execute. They run after transaction commit (if applicable).
//
// Post-scoped policies are useful for side effects that should happen only after all operations
// are committed, such as sending notifications or triggering external workflows.
// Such kind of policies have at-most-once execution guarantee.
type PostScopedPolicy interface {
	Run(ctx context.Context, changes map[AggregatePtr][]EventPack)
}

// WithPostScopedPolicy adds a post-scoped policy to the ConcurrentScope or Run options.
// The policy will execute after Run completes successfully, receiving all changes made during execution.
//
// Example:
//
//	scope := core.NewConcurrentScope(factory,
//	    core.WithPostScopedPolicy(myPostPolicy),
//	)
func WithPostScopedPolicy(policy PostScopedPolicy) RunOptions {
	return postScopedPolicyOption{policy: policy}
}

// WithPostScopedPolicyFunc adds a post-scoped policy using a function literal.
// This is a convenience function for creating post-scoped policies inline without implementing
// the PostScopedPolicy interface.
//
// Example:
//
//	scope := core.NewConcurrentScope(factory,
//	    core.WithPostScopedPolicyFunc(func(ctx context.Context, changes map[core.AggregatePtr][]core.EventPack) {
//	        // Process all changes after execution completes
//	        for aggPtr, eventPacks := range changes {
//	            // Handle changes
//	        }
//	    }),
//	)
func WithPostScopedPolicyFunc(policyFunc func(ctx context.Context, changes map[AggregatePtr][]EventPack)) RunOptions {
	return postScopedPolicyOption{policy: postScopedPolicyFunc(policyFunc)}
}

type postScopedPolicyFunc func(ctx context.Context, changes map[AggregatePtr][]EventPack)

func (f postScopedPolicyFunc) Run(ctx context.Context, changes map[AggregatePtr][]EventPack) {
	f(ctx, changes)
}

type postScopedPolicyOption struct {
	policy PostScopedPolicy
}

// WithRollbackTimeout sets a custom timeout for transaction rollback operations.
// If a transaction needs to be rolled back, this timeout ensures the rollback operation
// doesn't block indefinitely, even if the original context is cancelled.
//
// Example:
//
//	scope := core.NewConcurrentScope(repoFactory,
//	    core.WithRollbackTimeout(10*time.Second),
//	)
func WithRollbackTimeout(timeout time.Duration) RunOptions {
	return rollbackTimeoutOption{rollbackTimeout: timeout}
}

// WithRetryOptions configures retry behavior for the scope.
// These options are merged with the default retry configuration that retries on
// ErrTransient and ErrConcurrentModification errors.
//
// Example:
//
//	scope := core.NewConcurrentScope(repoFactory,
//	    core.WithRetryOptions(
//	        retry.Attempts(5),
//	        retry.Delay(100*time.Millisecond),
//	        retry.MaxDelay(1*time.Second),
//	    ),
//	)
//
// Retry options can also be provided per-run:
//
//	changes, err := scope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
//	    // Repository operations
//	}, core.WithRetryOptions(retry.Attempts(3)))
func WithRetryOptions(opts ...retry.Option) RunOptions {
	return retryOptions{retryOptions: opts}
}

type repoDecorator struct {
	inner    Repository
	changes  map[AggregatePtr][]EventPack
	policies []ScopedPolicy
}

var _ Repository = (*repoDecorator)(nil)

func (r *repoDecorator) Load(ctx context.Context, id ID, restorer Restorer, options ...LoadOption) error {
	return r.inner.Load(ctx, id, restorer, options...)
}

func (r *repoDecorator) Save(ctx context.Context, storer Storer, options ...SaveOption) error {
	decoratedStorer := &storerDecorator{
		Storer: storer,
		pack:   nil,
	}
	err := r.inner.Save(ctx, decoratedStorer, options...)
	if err != nil {
		return err
	}

	aggregatePtr := AggregatePtr(storer)
	r.changes[aggregatePtr] = append(r.changes[aggregatePtr], decoratedStorer.pack)
	for _, policy := range r.policies {
		err = policy.Run(ctx, r, aggregatePtr, decoratedStorer.pack)
		if err != nil {
			return err
		}
	}
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
//
//	changes, err := scope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
//	    session := &domain.Session{}
//	    err := repo.Load(ctx, sessionID, session)
//	    if err != nil {
//	        return err
//	    }
//
//	    events, err := session.ProcessRequest(now)
//	    if err != nil {
//	        return err
//	    }
//
//	    return repo.Save(ctx, session, redis.WithExpiration(24*time.Hour))
//	})
//
//	if err != nil {
//	    return err
//	}
//
//	// Process events from changes
//	for aggPtr, eventPacks := range changes {
//	    for _, pack := range eventPacks {
//	        refreshEvent, err := core.EventOfType[domain.RefreshQueued](pack)
//	        if err == nil {
//	            worker.QueueRefresh(refreshEvent.RefreshToken)
//	        }
//	    }
//	}
//
// Example with per-run retry options:
//
//	changes, err := scope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
//	    // Repository operations
//	}, core.WithRetryOptions(retry.Attempts(3), retry.Delay(50*time.Millisecond)))
func (c *ConcurrentScope) Run(ctx context.Context, runFunc func(ctx context.Context, repo Repository) error, runOptions ...RunOptions) (map[AggregatePtr][]EventPack, error) {
	opts := processRunOptions(c.retryOpts, c.rollbackTimeout, c.policies, c.postPolicies, runOptions...)
	var changes map[AggregatePtr][]EventPack
	err := retry.Do(
		func() error {
			sctx := ctx
			changes = make(map[AggregatePtr][]EventPack)
			repo := &repoDecorator{
				changes:  changes,
				policies: opts.policies,
				inner:    c.factory.Create(sctx),
			}
			if transactional, ok := repo.inner.(Transactional); ok {
				var err error
				sctx, err = transactional.Begin(sctx)
				if err != nil {
					return err
				}
				err = runFunc(sctx, repo)
				if err != nil {
					rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(sctx), opts.rollbackTimeout)
					defer cancel()
					rollbackErr := transactional.Rollback(rollbackCtx)
					return errors.Join(err, rollbackErr)
				}
				return transactional.Commit(sctx)
			}
			return runFunc(sctx, repo)
		},
		opts.retryOpts...,
	)
	if err != nil {
		return nil, err
	}
	for _, policy := range opts.postPolicies {
		policy.Run(ctx, changes)
	}
	return changes, nil
}
