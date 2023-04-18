package tx

import (
	"context"
	"database/sql"
)

// TxFromContext returns the transaction from the context.
func TxFromContext(ctx context.Context) (*sql.Tx, bool) {
	tx, ok := ctx.Value(txCtxKey).(*sql.Tx)
	return tx, ok
}

// ContextWithTx returns a new context with the transaction.
func ContextWithTx(ctx context.Context, tx *sql.Tx) context.Context {
	return context.WithValue(ctx, txCtxKey, tx)
}

type txCtxKeyType int

var txCtxKey txCtxKeyType = 0
