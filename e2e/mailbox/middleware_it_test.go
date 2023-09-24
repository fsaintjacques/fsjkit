package mailboxtest

import (
	"context"
	"database/sql"
	"expvar"
	"log/slog"
	"testing"
	"time"

	"github.com/fsaintjacques/fsjkit/mailbox"
	"github.com/fsaintjacques/fsjkit/tx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsumeMiddleware(t *testing.T) {
	var (
		opts     = tx.TransactorOptions{}
		noop     = func(ctx context.Context, msg mailbox.Message) error { return nil }
		failing  = func(ctx context.Context, msg mailbox.Message) error { return assert.AnError }
		blocking = func(ctx context.Context, msg mailbox.Message) error { <-ctx.Done(); return ctx.Err() }
		noDelay  = func(int, mailbox.Message) time.Duration { return 0 }
		ctx      = context.Background()
		msg      = mailbox.Message{ID: "a-message"}
	)

	t.Run("WithRetryPolicyConsume", func(t *testing.T) {
		t.Run("ExponentialBackoff", func(t *testing.T) {
			assert.Equal(t, 10*time.Second, mailbox.ExponentialBackoff(0, msg))
			assert.Equal(t, 20*time.Second, mailbox.ExponentialBackoff(1, msg))
			assert.Equal(t, 40*time.Second, mailbox.ExponentialBackoff(2, msg))
			assert.Equal(t, 80*time.Second, mailbox.ExponentialBackoff(3, msg))
			assert.Equal(t, 160*time.Second, mailbox.ExponentialBackoff(4, msg))
			assert.Equal(t, 300*time.Second, mailbox.ExponentialBackoff(5, msg))
			assert.Equal(t, 300*time.Second, mailbox.ExponentialBackoff(1000, msg))
			assert.Equal(t, 300*time.Second, mailbox.ExponentialBackoff(1000000, msg))
		})
		t.Run("EnsureRetriesIsRespected", func(t *testing.T) {
			retries := 0
			failing := func(ctx context.Context, msg mailbox.Message) error { retries++; return assert.AnError }
			policy := mailbox.RetryPolicy{MaxAttempts: 10, Backoff: noDelay}
			consume := mailbox.WithRetryPolicyConsume(policy)(failing)
			assert.NoError(t, consume(ctx, msg))
			assert.Equal(t, 10, retries)
		})
		t.Run("EnsureMessageIsPassed", func(t *testing.T) {
			assertIsMessage := func(m mailbox.Message) { assert.Equal(t, m.ID, msg.ID) }
			policy := mailbox.RetryPolicy{
				MaxAttempts: 2,
				Backoff:     func(_ int, m mailbox.Message) time.Duration { assertIsMessage(m); return 0 },
				Final:       func(_ context.Context, m mailbox.Message) error { assertIsMessage(m); return nil },
			}
			consume := mailbox.WithRetryPolicyConsume(policy)(func(_ context.Context, m mailbox.Message) error { assertIsMessage(m); return assert.AnError })
			assert.NoError(t, consume(ctx, msg))
		})
		t.Run("DefaultRetries", func(t *testing.T) {
			policy := mailbox.RetryPolicy{Backoff: noDelay}
			consume := mailbox.WithRetryPolicyConsume(policy)(failing)
			assert.NoError(t, consume(ctx, msg))
		})
		t.Run("ExistsOnSuccess", func(t *testing.T) {
			// This would cause the test to timeout if the the retry policy is not respected.
			policy := mailbox.RetryPolicy{Backoff: func(i int, _ mailbox.Message) time.Duration { return time.Hour }}
			consume := mailbox.WithRetryPolicyConsume(policy)(noop)
			assert.NoError(t, consume(ctx, msg))
		})
		t.Run("ContextCancellationAndAttemptTimeoutAreRespected", func(t *testing.T) {
			var failed bool
			policy := mailbox.RetryPolicy{
				MaxAttempts: 1, AttemptTimeout: time.Millisecond, Backoff: noDelay,
				Final: func(_ context.Context, _ mailbox.Message) error { failed = true; return nil },
			}
			consume := mailbox.WithRetryPolicyConsume(policy)(blocking)
			// If the context is not respected, this would cause the test to timeout.
			assert.NoError(t, consume(ctx, msg))
			assert.True(t, failed)
		})
		t.Run("FinalIsInvoked", func(t *testing.T) {
			var invoked bool
			policy := mailbox.RetryPolicy{
				MaxAttempts: 2, Backoff: noDelay,
				Final: func(_ context.Context, _ mailbox.Message) error { invoked = true; return nil },
			}
			consume := mailbox.WithRetryPolicyConsume(policy)(failing)
			assert.NoError(t, consume(ctx, msg))
			assert.True(t, invoked)
		})
	})

	t.Run("WithMoveToMailbox", func(t *testing.T) {
		var (
			db, err    = pg.Open("pgx")
			t1, t2     = createMailboxTable(t, db), createMailboxTable(t, db)
			mbox       = mailbox.NewMailbox(t1)
			deadletter = mailbox.NewMailbox(t2)
		)
		require.NoError(t, err)

		put := func(msg mailbox.Message) {
			require.NoError(t, tx.NewTransactor(db, opts).InTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
				return mbox.Put(ctx, tx, msg)
			}))
		}

		t.Run("MoveToMailbox", func(t *testing.T) {
			transactor := tx.NewTransactor(db, opts)
			c, err := mailbox.NewConsumer(ctx, transactor, t1, mailbox.WithMoveToMailbox(deadletter))
			require.NoError(t, err)

			// Put a message in t1
			put(mailbox.Message{ID: "move-me"})
			// Pop the message in t1 and move it to t2
			require.NoError(t, c.Consume(ctx))

			consume := func(ctx context.Context, msg mailbox.Message) error { assert.Equal(t, msg.ID, "move-me"); return nil }
			c, err = mailbox.NewConsumer(ctx, transactor, t2, consume)
			require.NoError(t, err)
			// Ensure the message is in t2 by consuming it.
			require.NoError(t, c.Consume(ctx))
		})
	})

	t.Run("WithObservabilityConsume", func(t *testing.T) {
		var (
			handler  = &testHandler{}
			logger   = slog.New(handler)
			vars     = new(expvar.Map)
			consumer = func(ctx context.Context, msg mailbox.Message) error {
				if msg.ID == "fail" {
					return assert.AnError
				}
				return nil
			}
			failMsg = mailbox.Message{ID: "fail"}
		)

		t.Run("NoLoggerNoVars", func(t *testing.T) {
			policy := mailbox.ObservabilityPolicy{}
			consume := mailbox.WithObservabilityConsume(policy)(consumer)
			assert.NoError(t, consume(ctx, msg))
			assert.ErrorIs(t, consume(ctx, failMsg), assert.AnError)
		})
		t.Run("WithLogger", func(t *testing.T) {
			policy := mailbox.ObservabilityPolicy{Logger: logger}
			consume := mailbox.WithObservabilityConsume(policy)(consumer)
			assert.NoError(t, consume(ctx, msg))
			// By default, it doesn't log success.
			assert.Empty(t, handler.invocations)
			assert.ErrorIs(t, consume(ctx, failMsg), assert.AnError)
			assert.Len(t, handler.invocations, 1)

			handler.invocations = nil
			policy.LogSuccess = true
			consume = mailbox.WithObservabilityConsume(policy)(consumer)
			assert.NoError(t, consume(ctx, msg))
			assert.NoError(t, consume(ctx, msg))
			assert.ErrorIs(t, consume(ctx, failMsg), assert.AnError)
			assert.ErrorIs(t, consume(ctx, failMsg), assert.AnError)
			assert.Len(t, handler.invocations, 4)
		})
		t.Run("WithVars", func(t *testing.T) {
			policy := mailbox.ObservabilityPolicy{Metrics: vars}
			consume := mailbox.WithObservabilityConsume(policy)(consumer)
			assert.NoError(t, consume(ctx, msg))
			assert.ErrorIs(t, consume(ctx, failMsg), assert.AnError)
			assert.Equal(t, int64(1), vars.Get(mailbox.MessageConsumedKey).(*expvar.Int).Value())
			assert.Equal(t, int64(1), vars.Get(mailbox.MessageConsumedFailedKey).(*expvar.Int).Value())
		})
		t.Run("WithEverything", func(t *testing.T) {
			handler.invocations = nil
			policy := mailbox.ObservabilityPolicy{Logger: logger, Metrics: vars, LogSuccess: true}
			consume := mailbox.WithObservabilityConsume(policy)(consumer)
			assert.NoError(t, consume(ctx, msg))
			assert.NoError(t, consume(ctx, msg))
			assert.ErrorIs(t, consume(ctx, failMsg), assert.AnError)
			assert.ErrorIs(t, consume(ctx, failMsg), assert.AnError)
			assert.Len(t, handler.invocations, 4)
			assert.Equal(t, int64(2), vars.Get(mailbox.MessageConsumedKey).(*expvar.Int).Value())
			assert.Equal(t, int64(2), vars.Get(mailbox.MessageConsumedFailedKey).(*expvar.Int).Value())
		})
	})

	t.Run("RoutingConsumer", func(t *testing.T) {
		counting := func(str string) (mailbox.Route, *int) {
			count := new(int)
			return mailbox.Route{
				Match: func(msg mailbox.Message) bool { return msg.ID == str },
				Consume: func(ctx context.Context, msg mailbox.Message) error {
					*count++
					return nil
				},
			}, count
		}

		t.Run("ReturnsErrNoRouteMatch", func(t *testing.T) {
			assert.ErrorIs(t, mailbox.RoutingConsumer()(ctx, mailbox.Message{}), mailbox.ErrNoRouteMatch)
		})
		t.Run("BasicMatch", func(t *testing.T) {
			var (
				one, c1   = counting("one")
				two, c2   = counting("two")
				three, c3 = counting("three")
				router    = mailbox.RoutingConsumer(one, two, three)
			)

			assert.NoError(t, router(ctx, mailbox.Message{ID: "one"}))
			assert.NoError(t, router(ctx, mailbox.Message{ID: "two"}))
			assert.NoError(t, router(ctx, mailbox.Message{ID: "two"}))
			assert.NoError(t, router(ctx, mailbox.Message{ID: "three"}))
			assert.NoError(t, router(ctx, mailbox.Message{ID: "three"}))
			assert.NoError(t, router(ctx, mailbox.Message{ID: "three"}))
			assert.ErrorIs(t, router(ctx, mailbox.Message{ID: "four"}), mailbox.ErrNoRouteMatch)

			assert.Equal(t, *c1, 1)
			assert.Equal(t, *c2, 2)
			assert.Equal(t, *c3, 3)
		})

		t.Run("OrderOfRoutes", func(t *testing.T) {
			var (
				one, c1 = counting("one")
				two, c2 = counting("one")
				router  = mailbox.RoutingConsumer(one, two)
			)

			assert.NoError(t, router(ctx, mailbox.Message{ID: "one"}))
			assert.NoError(t, router(ctx, mailbox.Message{ID: "one"}))
			assert.NoError(t, router(ctx, mailbox.Message{ID: "one"}))

			assert.Equal(t, *c1, 3)
			assert.Equal(t, *c2, 0)
		})
	})

	t.Run("DeadLetterExample", func(t *testing.T) {
		var (
			db, err    = pg.Open("pgx")
			t1, t2     = createMailboxTable(t, db), createMailboxTable(t, db)
			mbox       = mailbox.NewMailbox(t1)
			deadletter = mailbox.NewMailbox(t2)
			policy     = mailbox.RetryPolicy{
				MaxAttempts:    3,
				AttemptTimeout: 1 * time.Millisecond,
				Backoff:        noDelay,
				Final:          mailbox.WithMoveToMailbox(deadletter),
			}
			// This is a blocking consume function that will block until the context is cancelled. After the all uns
			// attempts, the message will be moved to the deadletter mailbox.
			consume = mailbox.WithRetryPolicyConsume(policy)(blocking)
		)
		require.NoError(t, err)

		// Put a message in t1
		require.NoError(t, tx.NewTransactor(db, opts).InTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
			return mbox.Put(ctx, tx, mailbox.Message{ID: "to-deadletter"})
		}))

		transactor := tx.NewTransactor(db, opts)
		c, err := mailbox.NewConsumer(ctx, transactor, t1, consume)
		require.NoError(t, err)
		assert.NoError(t, c.Consume(ctx))
		assert.ErrorIs(t, c.Consume(ctx), mailbox.ErrNoMessage)

		var consumed bool
		// Ensure the message is in deadletter by consuming it.
		consume = func(ctx context.Context, msg mailbox.Message) error {
			consumed = true
			assert.Equal(t, msg.ID, "to-deadletter")
			return nil
		}
		c, err = mailbox.NewConsumer(ctx, transactor, t2, consume)
		require.NoError(t, err)
		require.NoError(t, c.Consume(ctx))
		require.ErrorIs(t, c.Consume(ctx), mailbox.ErrNoMessage)
		assert.True(t, consumed)
	})
}

type testHandler struct {
	invocations []slog.Record
}

func (h *testHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }
func (h *testHandler) Handle(_ context.Context, r slog.Record) error {
	h.invocations = append(h.invocations, r)
	return nil
}
func (h *testHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *testHandler) WithGroup(_ string) slog.Handler      { return h }
