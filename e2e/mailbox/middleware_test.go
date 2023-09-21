package mailboxtest

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/fsaintjacques/fsjkit/mailbox"
	"github.com/fsaintjacques/fsjkit/tx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsumeMiddleware(t *testing.T) {
	var (
		noop     = func(ctx context.Context, msg mailbox.Message) error { return nil }
		failing  = func(ctx context.Context, msg mailbox.Message) error { return assert.AnError }
		blocking = func(ctx context.Context, msg mailbox.Message) error { <-ctx.Done(); return ctx.Err() }
		noDelay  = func(int, mailbox.Message) time.Duration { return 0 }
		ctx      = context.Background()
		msg      = mailbox.Message{ID: "a-message"}
	)

	t.Run("WithTimeoutConsume", func(t *testing.T) {
		consume := mailbox.WithTimeoutConsume(1)(blocking)
		assert.ErrorIs(t, consume(ctx, msg), context.DeadlineExceeded)
	})

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
		t.Run("ContextCancellationRespected", func(t *testing.T) {
			policy := mailbox.RetryPolicy{MaxAttempts: 100}
			consume := mailbox.WithTimeoutConsume(1 * time.Millisecond)(mailbox.WithRetryPolicyConsume(policy)(blocking))
			// If the context is not respected, this would cause the test to timeout.
			assert.ErrorIs(t, consume(ctx, msg), context.DeadlineExceeded)
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
			require.NoError(t, tx.NewTransactor(db).InTx(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
				return mbox.Put(ctx, tx, msg)
			}))
		}

		t.Run("MoveToMailbox", func(t *testing.T) {
			transactor := tx.NewTransactor(db)
			processor, err := mailbox.NewProcessor(context.Background(), transactor, t1, mailbox.WithMoveToMailbox(deadletter))
			require.NoError(t, err)

			// Put a message in t1
			put(mailbox.Message{ID: "move-me"})
			// Pop the message in t1 and move it to t2
			require.NoError(t, processor.Process(ctx))

			consume := func(ctx context.Context, msg mailbox.Message) error { assert.Equal(t, msg.ID, "move-me"); return nil }
			processor, err = mailbox.NewProcessor(context.Background(), transactor, t2, consume)
			require.NoError(t, err)
			// Ensure the message is in t2 by consuming it.
			require.NoError(t, processor.Process(ctx))
		})
	})

	t.Run("DeadLetterExample", func(t *testing.T) {
		var (
			db, err    = pg.Open("pgx")
			t1, t2     = createMailboxTable(t, db), createMailboxTable(t, db)
			mbox       = mailbox.NewMailbox(t1)
			deadletter = mailbox.NewMailbox(t2)
			policy     = mailbox.RetryPolicy{
				MaxAttempts: 3,
				Backoff:     noDelay,
				Final:       mailbox.WithMoveToMailbox(deadletter),
			}
			// This is a blocking consume function that will block until the context is cancelled. After the all uns
			// attempts, the message will be moved to the deadletter mailbox.
			consume = mailbox.WithRetryPolicyConsume(policy)(mailbox.WithTimeoutConsume(1 * time.Millisecond)(blocking))
		)
		require.NoError(t, err)

		// Put a message in t1
		require.NoError(t, tx.NewTransactor(db).InTx(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
			return mbox.Put(ctx, tx, mailbox.Message{ID: "to-deadletter"})
		}))

		transactor := tx.NewTransactor(db)
		p, err := mailbox.NewProcessor(context.Background(), transactor, t1, consume)
		require.NoError(t, err)
		assert.NoError(t, p.Process(ctx))
		assert.ErrorIs(t, p.Process(ctx), mailbox.ErrNoMessage)

		var consumed bool
		// Ensure the message is in deadletter by consuming it.
		consume = func(ctx context.Context, msg mailbox.Message) error {
			consumed = true
			assert.Equal(t, msg.ID, "to-deadletter")
			return nil
		}
		p, err = mailbox.NewProcessor(context.Background(), transactor, t2, consume)
		require.NoError(t, err)
		require.NoError(t, p.Process(ctx))
		require.ErrorIs(t, p.Process(ctx), mailbox.ErrNoMessage)
		assert.True(t, consumed)
	})
}
