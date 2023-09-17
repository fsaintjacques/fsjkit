package mboxtest

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/fsaintjacques/fsjkit/mbox"
	"github.com/fsaintjacques/fsjkit/tx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessor(t *testing.T) {
	t.Parallel()

	var (
		ctx        = context.Background()
		db, err    = pg.Open("pgx")
		transactor = tx.NewTransactor(db)
		table      = createMailboxTable(t, db)
		m          = mbox.NewMailbox(table)
	)

	require.NoError(t, err)

	var (
		noop = func(context.Context, mbox.Message) error { return nil }
		put  = func(id string) {
			tx, err := db.Begin()
			assert.NoError(t, err)
			assert.NoError(t, m.Put(ctx, tx, mbox.Message{ID: id}))
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
		failing = func(context.Context, mbox.Message) error { return anError }
	)

	t.Run("NewProcessor", func(t *testing.T) {
		t.Run("ShouldErrorOnNonExistingTable", func(t *testing.T) {
			_, err := mbox.NewProcessor(ctx, transactor, "notfound")
			assert.Error(t, err)
		})
		t.Run("ShouldErrorOnInvalidSchema", func(t *testing.T) {
			db.ExecContext(ctx, "CREATE TABLE bad (id VARCHAR PRIMARY KEY)")
			_, err := mbox.NewProcessor(ctx, transactor, "bad")
			assert.Error(t, err)
		})
		t.Run("Success", func(t *testing.T) {
			p, err := mbox.NewProcessor(ctx, transactor, table)
			assert.NotNil(t, p)
			assert.NoError(t, err)
		})

		truncate()
	})

	t.Run("Processor.Process", func(t *testing.T) {
		p, err := mbox.NewProcessor(ctx, transactor, table)
		require.NoError(t, err)

		t.Run("ReturnsErrNoMessageWhenEmpty", func(t *testing.T) {
			assert.ErrorIs(t, mbox.ErrNoMessage, p.Process(ctx, noop))
			put("a-message")
			assert.NoError(t, p.Process(ctx, noop))
			assert.ErrorIs(t, mbox.ErrNoMessage, p.Process(ctx, noop))
		})

		t.Run("EnsureMessageIsConsumed", func(t *testing.T) {
			put("a-message")
			assert.NoError(t, p.Process(ctx, noop))
			assert.ErrorIs(t, mbox.ErrNoMessage, p.Process(ctx, noop))
		})

		t.Run("WrapsConsumeError", func(t *testing.T) {
			put("a-message")
			assert.ErrorIs(t, p.Process(ctx, failing), anError)
		})

		truncate()
	})

	t.Run("Processor.Size", func(t *testing.T) {
		const max = 10
		p, err := mbox.NewProcessor(ctx, transactor, table)
		require.NoError(t, err)

		size, err := p.Size(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(0), size)

		for i := 0; i < max; i++ {
			put(fmt.Sprintf("%d", i))
		}

		size, err = p.Size(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(max), size)

		for i := 0; i < max; i++ {
			require.NoError(t, p.Process(ctx, noop))
			size, err = p.Size(ctx)
			require.NoError(t, err)
			assert.Equal(t, int64(max-(i+1)), size)
		}
	})

	truncate()
}
