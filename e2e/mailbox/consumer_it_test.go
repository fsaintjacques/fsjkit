package mailboxtest

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/fsaintjacques/fsjkit/mailbox"
	"github.com/fsaintjacques/fsjkit/tx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsumer(t *testing.T) {
	t.Parallel()

	var (
		ctx        = context.Background()
		db, err    = pg.Open()
		transactor = tx.NewTransactor(db, tx.TransactorOptions{})
		table      = createMailboxTable(t, db)
		m          = mailbox.NewMailbox(table)
	)

	require.NoError(t, err)

	var (
		noop = func(context.Context, mailbox.Message) error { return nil }
		put  = func(id string) {
			tx, err := db.Begin()
			assert.NoError(t, err)
			assert.NoError(t, m.Put(ctx, tx, mailbox.Message{ID: id}))
			assert.NoError(t, tx.Commit())
		}
		truncate = func() {
			tx, err := db.Begin()
			assert.NoError(t, err)
			_, err = tx.ExecContext(ctx, "TRUNCATE TABLE \""+table+"\"")
			assert.NoError(t, err)
			assert.NoError(t, tx.Commit())
		}
		anError = errors.New("an error")
		failing = func(context.Context, mailbox.Message) error { return anError }
	)

	t.Run("NewConsumer", func(t *testing.T) {
		t.Run("ShouldErrorOnNonExistingTable", func(t *testing.T) {
			_, err := mailbox.NewConsumer(ctx, transactor, "notfound", noop)
			assert.Error(t, err)
		})
		t.Run("ShouldErrorOnInvalidSchema", func(t *testing.T) {
			db.ExecContext(ctx, "CREATE TABLE bad (id VARCHAR PRIMARY KEY)")
			_, err := mailbox.NewConsumer(ctx, transactor, "bad", noop)
			assert.Error(t, err)
		})
		t.Run("Success", func(t *testing.T) {
			c, err := mailbox.NewConsumer(ctx, transactor, table, noop)
			assert.NotNil(t, c)
			assert.NoError(t, err)
		})

		truncate()
	})

	t.Run("Consumer.Consume", func(t *testing.T) {
		c, err := mailbox.NewConsumer(ctx, transactor, table, noop)
		require.NoError(t, err)

		t.Run("ReturnsErrNoMessageWhenEmpty", func(t *testing.T) {
			assert.ErrorIs(t, mailbox.ErrNoMessage, c.Consume(ctx))
			put("a-message")
			assert.NoError(t, c.Consume(ctx))
			assert.ErrorIs(t, mailbox.ErrNoMessage, c.Consume(ctx))
		})
		t.Run("WithPayloadAndMetadata", func(t *testing.T) {
			var (
				metadata = map[string]string{"foo": "bar", "baz": "qux"}
				payload  = []byte("payload")
				id       = "an-id"
				consume  = func(ctx context.Context, msg mailbox.Message) error {
					assert.Equal(t, id, msg.ID)
					assert.Equal(t, metadata, msg.Metadata)
					assert.Equal(t, payload, msg.Payload)
					return nil
				}
				c, err = mailbox.NewConsumer(ctx, transactor, table, consume)
			)
			require.NoError(t, err)
			tx, err := db.Begin()
			require.NoError(t, err)
			require.NoError(t, m.Put(ctx, tx, mailbox.Message{ID: id, Metadata: metadata, Payload: payload}))
			require.NoError(t, tx.Commit())
			assert.NoError(t, c.Consume(ctx))
		})
		t.Run("EnsureMessageIsConsumed", func(t *testing.T) {
			put("a-message")
			assert.NoError(t, c.Consume(ctx))
			assert.ErrorIs(t, mailbox.ErrNoMessage, c.Consume(ctx))
		})

		c, err = mailbox.NewConsumer(ctx, transactor, table, failing)
		require.NoError(t, err)

		t.Run("WrapsConsumeError", func(t *testing.T) {
			put("a-message")
			assert.ErrorIs(t, c.Consume(ctx), anError)
		})

		truncate()
	})

	t.Run("Consumer.Size", func(t *testing.T) {
		const max = 10
		c, err := mailbox.NewConsumer(ctx, transactor, table, noop)
		require.NoError(t, err)

		size, err := c.Size(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(0), size)

		for i := 0; i < max; i++ {
			put(fmt.Sprintf("%d", i))
		}

		size, err = c.Size(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(max), size)

		for i := 0; i < max; i++ {
			require.NoError(t, c.Consume(ctx))
			size, err = c.Size(ctx)
			require.NoError(t, err)
			assert.Equal(t, int64(max-(i+1)), size)
		}
	})

	truncate()
}
