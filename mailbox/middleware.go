package mailbox

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"log/slog"
	"time"

	"github.com/fsaintjacques/fsjkit/tx"
)

type (
	// ConsumeMiddleware is a function that wraps a ConsumeFn. It is used to
	// implement middleware that can be applied to the ConsumeFn.
	ConsumeMiddleware = func(ConsumeFn) ConsumeFn
)

type (
	// ObservabilityPolicy controls how a message is logged with the
	// WithObservabilityConsume middleware. It also supports recording metrics.
	// It exposes, at a high level, the following:
	//   - the logger used to log errors
	//   - the attributes to log
	//   - whether successful message consumption is logged
	//   - the expvar map used to record metrics
	ObservabilityPolicy struct {
		// Metrics is the expvar map used to record metrics. If nil, no metrics
		// are recorded. Two metrics are recorded:
		//   - messages_consumed_total: the number of messages successfully consumed
		//   - messages_consumed_failed_total: the number of messages that failed to be consumed
		// The metrics keys are statically defined by MessageConsumedKey and
		// MessageConsumedFailedKey.
		Metrics *expvar.Map
		// Logger is the logger used to log events. If nil, no logging is done.
		// By default, only errors are logged. See LogSuccess for more details.
		Logger *slog.Logger
		// Attrs is the function that returns the attributes to log. If nil, no
		// attributes are extracted and logged. See slog's documentation for more
		// details on attributes.
		Attrs func(context.Context, Message) []any
		// If set, successful message consumption is logged. Errors are always
		// logged. If nil, no logging is done.
		LogSuccess bool
	}
)

const (
	// MessageConsumedKey is the key used to record the number of messages
	// successfully consumed.
	MessageConsumedKey = "messages_consumed_total"
	// MessageConsumedFailedKey is the key used to record the number of
	// messages that failed to be consumed.
	MessageConsumedFailedKey = "messages_consumed_failed_total"
)

// WithObservabilityConsume returns a ConsumeMiddleware that wraps the ConsumeFn
// with observability, notably logging and metrics. See ObservabilityPolicy for
// more details. Ideally, this middleware should immediately wrap the ConsumeFn
// such that retry attempts are also logged.
func WithObservabilityConsume(p ObservabilityPolicy) ConsumeMiddleware {
	var (
		successCount = new(expvar.Int)
		failureCount = new(expvar.Int)
	)

	if p.Metrics != nil {
		p.Metrics.Set(MessageConsumedKey, successCount)
		p.Metrics.Set(MessageConsumedFailedKey, failureCount)
	}

	if p.Attrs == nil {
		p.Attrs = func(context.Context, Message) []any { return nil }
	}

	return func(fn ConsumeFn) ConsumeFn {
		return func(ctx context.Context, msg Message) (err error) {
			err = fn(ctx, msg)

			if p.Logger != nil {
				if err != nil {
					var body = fmt.Sprintf("message consumption failed: %s: %v", msg.ID, err)
					p.Logger.ErrorContext(ctx, body, p.Attrs(ctx, msg)...)
				} else if p.LogSuccess {
					var body = fmt.Sprintf("message consumption succeeded: %s", msg.ID)
					p.Logger.InfoContext(ctx, body, p.Attrs(ctx, msg)...)
				}
			}

			if p.Metrics != nil {
				if err != nil {
					failureCount.Add(1)
				} else {
					successCount.Add(1)
				}
			}

			return
		}
	}
}

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

type (
	// Filter is a function that returns true if the message should be
	// given to this consumer.
	MatchFn func(Message) bool

	// Route is a pair of MatchFn and ConsumeFn. They define the routing
	// rules for a message.
	Route struct {
		Match   MatchFn
		Consume ConsumeFn
	}
)

var (
	// ErrNoRouteMatch is returned when no route matches the message.
	ErrNoRouteMatch = errors.New("no route matches the message")
)

// RoutingConsumer returns a ConsumeFn that routes the message to the first
// route that matches the message according to the order of the routes.
// If no route matches the message, it returns ErrNoRouteMatch.
func RoutingConsumer(routes ...Route) ConsumeFn {
	return func(ctx context.Context, msg Message) error {
		for _, route := range routes {
			if route.Match(msg) {
				return route.Consume(ctx, msg)
			}
		}
		return ErrNoRouteMatch
	}
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
