package txtest

import (
	"context"
	"database/sql"
	"testing"

	ftx "github.com/fsaintjacques/fsjkit/tx"
	"github.com/stretchr/testify/assert"
)

func TestTxFromContext(t *testing.T) {
	t.Parallel()

	var (
		ctx    = context.Background()
		tx, ok = ftx.FromContext(ctx)
	)

	assert.Nil(t, tx)
	assert.False(t, ok)

	expected := new(sql.Tx)
	ctx = ftx.ContextWithTx(ctx, expected)
	tx, ok = ftx.FromContext(ctx)

	assert.Equal(t, expected, tx)
	assert.True(t, ok)

	newer := new(sql.Tx)
	ctx = ftx.ContextWithTx(ctx, expected)
	tx, ok = ftx.FromContext(ctx)

	assert.Equal(t, newer, tx)
	assert.True(t, ok)
}
