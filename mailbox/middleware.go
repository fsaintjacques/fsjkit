package mailbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/fsaintjacques/fsjkit/tx"
)

type (
	// ConsumeMiddleware is a function that wraps a ConsumeFn. It is used to
	// implement middleware that can be applied to the ConsumeFn.
	ConsumeMiddleware = func(ConsumeFn) ConsumeFn
)

type (
	// RetryPolicy controls how a message is retried with the WithRetryConsume middleware.
	// It exposes, at a high level, the following:
	//   - the backoff function
	//   - the final function, invoked after all retries have been exhausted
	//   - the maximum number of attempts
	//   - the timeout for each attempt
	//	 - it recovers from panics and return an error
	RetryPolicy struct {
		// Backoff is the function that returns the duration to wait before
		// retrying. The argument is the number of retries and the message. If
		// the function returns 0, it retries immediately. By default, it uses
		// an exponential backoff similar to Kubernetes, e.g. 10s, 20s, 40s, ...,
		// capped at 5 minutes.
		Backoff func(int, Message) time.Duration
		// Final is invoked after all retries have been exhausted. For example,
		// it could be used to move the message to a dead-letter queue, or it could
		// be used to log the message. If the function returns an error, the
		// message is retried. By default, it swallows the error. Note that this
		// function is not recovered from.
		Final ConsumeFn
		// MaxAttempts is the maximum number of attempts before giving up. If the
		// value not strictly positive, it is set to DefaultMaxAttempts.
		MaxAttempts int
		// AttemptTimeout is the timeout for each attempt. If the value is not
		// strictly positive, it is set to DefaultAttemptTimeout.
		AttemptTimeout time.Duration
	}
)

// DefaultMaxAttempts is the default maximum number of attempts.
const (
	DefaultMaxAttempts    = 5
	DefaultAttemptTimeout = 10 * time.Second
)

// WithRetryConsume returns a ConsumeMiddleware that wraps the ConsumeFn with
// a retry policy. See RetryPolicy for more details.
func WithRetryPolicyConsume(p RetryPolicy) ConsumeMiddleware {
	if p.MaxAttempts < 1 {
		p.MaxAttempts = DefaultMaxAttempts
	}

	if p.AttemptTimeout < 1 {
		p.AttemptTimeout = DefaultAttemptTimeout
	}

	if p.Backoff == nil {
		p.Backoff = ExponentialBackoff
	}

	if p.Final == nil {
		// Swallow the error such that the message is not retried.
		p.Final = func(context.Context, Message) error { return nil }
	}

	wait := func(ctx context.Context, backoff time.Duration) error {
		timer := time.NewTimer(backoff)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}

	return func(fn ConsumeFn) ConsumeFn {
		// Ensure the consume function does not panic and a timeout is enforced.
		run := func(ctx context.Context, msg Message) (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("consume function panicked: %v", r)
				}
			}()

			ctx, cancel := context.WithTimeout(ctx, p.AttemptTimeout)
			defer cancel()

			return fn(ctx, msg)
		}

		return func(ctx context.Context, msg Message) error {
			for i := 0; i < p.MaxAttempts; i++ {
				if backoff := p.Backoff(i, msg); i > 0 && backoff > 0 {
					if err := wait(ctx, backoff); err != nil {
						return err
					}
				}

				if err := run(ctx, msg); err == nil {
					return nil
				}
			}
			return p.Final(ctx, msg)
		}
	}
}

const MaxExponentialBackoff = 5 * time.Minute

// Follows kubernetes exponential backoff, i.e. 10s, 20s, 40s, ..., capped at 5 minutes.
func ExponentialBackoff(i int, _ Message) time.Duration {
	const maxShift = 32
	return min(10*time.Second*(1<<min(i, maxShift)), MaxExponentialBackoff)
}

var (
	// ErrNoTx is returned when the transaction is not found in the context.
	ErrNoTx = errors.New("no transaction found in context")
)

// WithMoveToMailbox returns a ConsumeFn that moves the message to the mailbox.
// This can be paired with WithRetryConsume to implement a dead-letter queue when
// this ConsumeFn is used as the Final function. In order to use this ConsumeFn,
// the consumer's transactor must be configured with recursive transactions.
// The enqueueing of the message is done in the same transaction as the
// processing of the message. This ensures that the message is not lost if the
// transaction is rolled back.
func WithMoveToMailbox(m Mailbox) ConsumeFn {
	return func(ctx context.Context, msg Message) error {
		// Extract the transaction from the context. This requires that the
		// consumer's transactor is configured with recursive transactions.
		txn, found := tx.FromContext(ctx)
		if !found {
			return ErrNoTx
		}

		if err := m.Put(ctx, txn, msg); err != nil {
			return fmt.Errorf("m.Put: %w", err)
		}

		return nil
	}
}
