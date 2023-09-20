package tx

import (
	"context"
	"database/sql"
	"fmt"
)

type (
	savepointTx struct {
		*sql.Tx
		savepoints    []string
		savepoint     int
		useSavepoints bool
	}
)

func newSavepointTx(tx *sql.Tx, useSavepoints bool) *savepointTx {
	return &savepointTx{Tx: tx, useSavepoints: useSavepoints}
}

// BeginTx starts a new savepoint.
func (t *savepointTx) BeginTx(ctx context.Context, _ *sql.TxOptions) (*sql.Tx, error) {
	sp := t.push()
	if t.useSavepoints {
		if _, err := t.ExecContext(ctx, fmt.Sprintf("SAVEPOINT %s", sp)); err != nil {
			return nil, fmt.Errorf("tx.BeginTx: %w", err)
		}
	}
	return t.Tx, nil
}

// Commit commits the savepoint if any, or the transaction.
func (t *savepointTx) Commit() error {
	if len(t.savepoints) == 0 {
		return t.Tx.Commit()
	}
	sp := t.pop()
	if t.useSavepoints {
		if _, err := t.Exec(fmt.Sprintf("RELEASE SAVEPOINT %s", sp)); err != nil {
			return fmt.Errorf("releasing savepoint: t.Exec: %w", err)
		}
	}
	return nil
}

// Rollback rolls back the savepoint if any, or the transaction.
func (t *savepointTx) Rollback() error {
	if len(t.savepoints) == 0 {
		return t.Tx.Rollback()
	}
	sp := t.pop()
	if t.useSavepoints {
		if _, err := t.Exec(fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", sp)); err != nil {
			return fmt.Errorf("rollback to savepoint: t.Exec: %w", err)
		}
	}
	return nil
}

func (t *savepointTx) push() string {
	t.savepoint++
	var sp = fmt.Sprintf("sp%d", t.savepoint)
	t.savepoints = append(t.savepoints, sp)
	return sp
}

func (t *savepointTx) pop() string {
	var sp = t.savepoints[len(t.savepoints)-1]
	t.savepoints = t.savepoints[:len(t.savepoints)-1]
	return sp
}

func savepointTxFromContext(ctx context.Context) (*savepointTx, bool) {
	tx, ok := ctx.Value(txKey).(*savepointTx)
	return tx, ok
}

func contextWithSavepointTx(ctx context.Context, tx *savepointTx) context.Context {
	return context.WithValue(ctx, txKey, tx)
}

type txKeyType int

var txKey txKeyType
