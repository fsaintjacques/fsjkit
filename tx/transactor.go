package tx

import (
	"context"
	"database/sql"
	"fmt"
)

type (
	// Closure is a function that is executed within a transaction.
	// The function should not commit or rollback the transaction.
	Closure func(context.Context, *sql.Tx) error

	// Middleware is a function that is executed before and after the closure.
	Middleware func(Closure) Closure

	// Opener is an interface for database connections that support opening
	// transactions. This interface is implemented by *sql.DB and *sql.Conn.
	Opener interface {
		BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
	}

	// The transactor is responsible for creating and committing or rolling back
	// transactions.
	Transactor interface {
		// InTx executes the given closure within a transaction. The transaction is
		// committed if the closure returns nil, otherwise it is rolled back. It is
		// important that the closure does not commit or rollback the transaction as
		// this is handled by the transactor automatically. Recursively calling InTx
		// will re-use the same transaction.
		InTx(context.Context, Closure) error
	}

	TransactorOptions struct {
		// sql.TxOptions to pass the opener when it begins a transaction. This does
		// not apply to new savepoint transactions (only the root transaction).
		BeginOptions *sql.TxOptions
		// Middlewares to execute before and after the closure. The middlewares are
		// executed in the order they are passed. The middlewares are executed only
		// once per opened transaction, not for each recursive call to InTx.
		BeginMiddlewares []Middleware
		// If true, the transactor will use savepoints to create nested transactions.
		// Make sure the database supports savepoints before enabling this option.
		EnableSavepoints bool
	}
)

// NewTransactor creates a new Transactor that uses the given a Opener.
// This function panics if the Opener is nil. The transactor is responsible
// for creating and committing or rolling back transactions.
func NewTransactor(o Opener, opts TransactorOptions) Transactor {
	if o == nil {
		panic("NewTransactor: opener is nil")
	}
	t := &transactor{opener: o}

	if opts.BeginOptions != nil {
		t.txOptions = opts.BeginOptions
	}

	if opts.BeginMiddlewares != nil {
		t.middlewareChain = chainMiddlewares(opts.BeginMiddlewares...)
	}

	if opts.EnableSavepoints {
		t.enableSavepoints = true
	}

	return t
}

// InTx implements the Transactor interface.
func (t *transactor) InTx(ctx context.Context, fn Closure) (err error) {
	sp, opened, err := t.begin(ctx, t.txOptions)
	if err != nil {
		return err
	}

	if opened {
		ctx = contextWithSavepointTx(ctx, sp)
		if t.middlewareChain != nil {
			// Middleware is executed only once per opened transaction.
			fn = t.middlewareChain(fn)
		}
	}

	// Defer the commit or rollback of the transaction. This will be executed
	// after the closure returns.
	defer func() {
		// In case of a panic due to the closure, rollback the transaction and
		// then re-panic the original message.
		if r := recover(); r != nil {
			_ = sp.Rollback()
			panic(r)
		}
		if err != nil {
			_ = sp.Rollback()
		} else if err = sp.Commit(); err != nil {
			// If the closure did not return an error, but the commit failed,
			// return the commit error.
			err = fmt.Errorf("sp.Commit: %w", err)
		}
	}()

	return fn(ctx, sp.Tx)
}

func (t *transactor) begin(ctx context.Context, opts *sql.TxOptions) (*savepointTx, bool, error) {
	sp, found := savepointTxFromContext(ctx)
	if found {
		_, err := sp.BeginTx(ctx, nil)
		if err != nil {
			return nil, false, fmt.Errorf("sp.BeginTx: %w", err)
		}
		return sp, false, nil
	}

	tx, err := t.opener.BeginTx(ctx, opts)
	if err != nil {
		return nil, false, fmt.Errorf("t.opener.BeginTx: %w", err)
	}

	return newSavepointTx(tx, t.enableSavepoints), true, nil
}

func chainMiddlewares(middlewares ...Middleware) Middleware {
	return func(fn Closure) Closure {
		for i := len(middlewares) - 1; i >= 0; i-- {
			fn = middlewares[i](fn)
		}
		return fn
	}
}

type (
	transactor struct {
		opener Opener

		middlewareChain  Middleware
		txOptions        *sql.TxOptions
		enableSavepoints bool
	}
)
