package mailbox

import (
	"context"
	"time"
)

type (
	// ConsumeMiddleware is a function that wraps a ConsumeFn. It is used to
	// implement middleware that can be applied to the ConsumeFn.
	ConsumeMiddleware = func(ConsumeFn) ConsumeFn
)

// WithTimeoutConsume returns a ConsumeMiddleware that wraps the ConsumeFn with
// a timeout. If the timeout is reached, the context is canceled and the
// ConsumeFn is interrupted. The ConsumeFn is responsible to respect the context
// and return quickly.
func WithTimeoutConsume(timeout time.Duration) ConsumeMiddleware {
	return func(fn ConsumeFn) ConsumeFn {
		return func(ctx context.Context, msg Message) error {
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			return fn(ctx, msg)
		}
	}
}

type (
	// RetryPolicy controls how a message is retried with the WithRetryConsume middleware.
	// It exposes, at a high level, the following:
	//   - the maximum number of retries
	//   - the backoff function
	//   - the final function, invoked after all retries have been exhausted
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
		// message is retried. By default, it swallows the error.
		Final ConsumeFn
		// MaxAttempts is the maximum number of attempts before giving up. If the
		// value not strictly positive, it is set to DefaultMaxAttempts.
		MaxAttempts int
	}
)

// DefaultMaxAttempts is the default maximum number of attempts.
const DefaultMaxAttempts = 5

// WithRetryConsume returns a ConsumeMiddleware that wraps the ConsumeFn with
// a retry policy. See RetryPolicy for more details.
func WithRetryPolicyConsume(p RetryPolicy) ConsumeMiddleware {
	if p.MaxAttempts < 1 {
		p.MaxAttempts = DefaultMaxAttempts
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
		return func(ctx context.Context, msg Message) error {
			for i := 0; i < p.MaxAttempts; i++ {
				if err := fn(ctx, msg); err == nil {
					return nil
				}

				if backoff := p.Backoff(i, msg); backoff > 0 {
					if err := wait(ctx, backoff); err != nil {
						return err
					}
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
