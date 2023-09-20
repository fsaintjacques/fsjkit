package tx

import (
	"context"
	"database/sql"
	"fmt"
)

type (
	// Closure is a function that is executed within a transaction.
	// The function should not commit or rollback the transaction. The closure
	// must pass the context down to the next layer for some options to be used, like
	// WithRecursiveContext.
	Closure func(context.Context, *sql.Tx) error

	// TransactorOption affects the behavior of a Transactor.
	TransactorOption interface {
		apply(*transactor)
	}

	// The transactor is responsible for creating and committing or rolling back
	// transactions.
	Transactor interface {
		// InTx executes the given closure within a transaction. The transaction is
		// committed if the closure returns nil, otherwise it is rolled back. It is
		// important that the closure does not commit or rollback the transaction as
		// this is handled by the transactor automatically.
		//
		// If the Closure calls InTx and the Transactor was created with
		// WithRecursiveContext, the same transaction will be used.
		InTx(context.Context, Closure) error
	}

	// Middleware is a function that is executed before and after the closure.
	Middleware func(Closure) Closure

	// Opener is an interface for database connections that support opening
	// transactions. This interface is implemented by *sql.DB and *sql.Conn.
	Opener interface {
		BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
	}
)

// WithTxOptions sets the transaction options for the transactor when it
// creates a transaction.
func WithTxOptions(opts *sql.TxOptions) TransactorOption {
	return txOptFn(func(t *transactor) {
		t.txOptions = opts
	})
}

// WithSavepoints enables the use of savepoints for the transactor. This
// allows the transactor to create nested transactions. Don't use this option
// if the database does not support savepoints.
func WithSavepoints() TransactorOption {
	return txOptFn(func(t *transactor) {
		t.useSavepoints = true
	})
}

// WithMiddlewares adds middlewares to the transactor. Middlewares are
// functions that are executed before and after the closure. The middlewares
// can be used to perform logging, metrics, or other operations. The middle
// functions are executed in the order they are passed.
//
// When paired with WithRecursiveContext, the middlewares are executed only
// once per transaction and not for each call to InTx.
//
// If a middleware returns an error, the transaction is rolled back and the
// error is returned to the caller.
//
// Passing this option multiple times will overwrite the existing list of
// middlewares.
func WithMiddlewares(middlewares ...Middleware) TransactorOption {
	return txOptFn(func(t *transactor) {
		t.middlewareChain = chainMiddlewares(middlewares...)
	})
}

// NewTransactor creates a new Transactor that uses the given a Opener.
// This function panics if the Opener is nil. The transactor is responsible
// for creating and committing or rolling back transactions.
func NewTransactor(o Opener, opts ...TransactorOption) Transactor {
	if o == nil {
		panic("NewTransactor: opener is nil")
	}
	t := &transactor{opener: o}
	for _, opt := range opts {
		opt.apply(t)
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

	return newSavepointTx(tx, t.useSavepoints), true, nil
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

		middlewareChain Middleware
		txOptions       *sql.TxOptions
		useSavepoints   bool
	}

	txOptFn func(*transactor)
)

func (fn txOptFn) apply(t *transactor) {
	fn(t)
}
