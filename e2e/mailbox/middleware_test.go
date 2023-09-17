package mailboxtest

import (
	"context"
	"testing"
	"time"

	"github.com/fsaintjacques/fsjkit/mailbox"
	"github.com/stretchr/testify/assert"
)

func TestConsumeMiddleware(t *testing.T) {
	var (
		noop       = func(ctx context.Context, msg mailbox.Message) error { return nil }
		failing    = func(ctx context.Context, msg mailbox.Message) error { return assert.AnError }
		ctxConsume = func(ctx context.Context, msg mailbox.Message) error {
			<-ctx.Done()
			return ctx.Err()
		}
		noDelay = func(int, mailbox.Message) time.Duration { return 0 }
		ctx     = context.Background()
		msg     = mailbox.Message{ID: "a-message"}
	)

	t.Run("WithTimeoutConsume", func(t *testing.T) {
		consume := mailbox.WithTimeoutConsume(1)(ctxConsume)
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
			consume := mailbox.WithTimeoutConsume(1 * time.Millisecond)(mailbox.WithRetryPolicyConsume(policy)(ctxConsume))
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
}
