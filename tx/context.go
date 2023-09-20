package tx

import (
	"context"
	"database/sql"
)

// FromContext returns the transaction from the context.
func FromContext(ctx context.Context) (*sql.Tx, bool) {
	sp, found := savepointTxFromContext(ctx)
	if found {
		return sp.Tx, true
	}
	return nil, false
}
