package tx

import (
	"context"
	"database/sql"
	"fmt"
)

type (
	// TxClosure is a function that is executed within a transaction.
	// The function should not commit or rollback the transaction. The closure
	// must pass the context down to the next layer for some options to be used, like
	// WithRecursiveContext.
	TxClosure func(context.Context, *sql.Tx) error

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
		InTx(context.Context, TxClosure) error
	}

	// TxMiddleware is a function that is executed before and after the closure.
	TxMiddleware func(TxClosure) TxClosure

	// TxAble is an interface for database connections that support opening
	// transactions. This interface is implemented by *sql.DB and *sql.Conn.
	TxAble interface {
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

// WithRecursiveContext stashes transaction in context. The transaction is carried
// through the context and will be re-used by the transactor. In other words, multiple
// calls to InTx will use the same transaction if the context is passed through.
func WithRecursiveContext() TransactorOption {
	return txOptFn(func(t *transactor) {
		t.recursiveContext = true
	})
}

// WithAlwaysRollback forces the transactor to always rollback the transaction.
func WithAlwaysRollback() TransactorOption {
	return txOptFn(func(t *transactor) {
		t.alwaysRollback = true
	})
}

// WithTxMiddlewares adds middlewares to the transactor. Middlewares are
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
func WithTxMiddlewares(middlewares ...TxMiddleware) TransactorOption {
	return txOptFn(func(t *transactor) {
		t.middlewareChain = chainMiddlewares(middlewares...)
	})
}

// NewTransactor creates a new Transactor that uses the given a TxAble.
// This function panics if the TxAble is nil. The transactor is responsible
// for creating and committing or rolling back transactions.
func NewTransactor(db TxAble, opts ...TransactorOption) Transactor {
	if db == nil {
		panic("NewTransactor: db is nil")
	}
	t := &transactor{db: db}
	for _, opt := range opts {
		opt.apply(t)
	}
	return t
}

func (t *transactor) InTx(ctx context.Context, fn TxClosure) (err error) {
	if t.recursiveContext {
		tx, ok := TxFromContext(ctx)
		if ok {
			// Do not perform any commit or rollback operations since
			// the transaction was not created in this call. An ancestor
			// call will handle the commit or rollback.
			return fn(ctx, tx)
		}
	}

	var tx *sql.Tx
	tx, err = t.db.BeginTx(ctx, t.txOptions)
	if err != nil {
		return fmt.Errorf("db.BeginTx: %w", err)
	}

	// Defer the commit or rollback of the transaction. This will be executed
	// after the closure returns.
	defer func() {
		// In case of a panic due to the closure, rollback the transaction and
		// then re-panic the original message.
		if r := recover(); r != nil {
			_ = tx.Rollback()
			panic(r)
		}

		if err != nil || t.alwaysRollback {
			_ = tx.Rollback()
		} else if err = tx.Commit(); err != nil {
			// If the closure did not return an error, but the commit failed,
			// return the commit error.
			err = fmt.Errorf("tx.Commit: %w", err)
		}
	}()

	if t.recursiveContext {
		// Pass the transaction through the context.
		ctx = ContextWithTx(ctx, tx)
	}

	if t.middlewareChain != nil {
		fn = t.middlewareChain(fn)
	}

	return fn(ctx, tx)
}

func chainMiddlewares(middlewares ...TxMiddleware) TxMiddleware {
	return func(fn TxClosure) TxClosure {
		for i := len(middlewares) - 1; i >= 0; i-- {
			fn = middlewares[i](fn)
		}
		return fn
	}
}

type (
	transactor struct {
		db TxAble

		middlewareChain  TxMiddleware
		txOptions        *sql.TxOptions
		recursiveContext bool
		alwaysRollback   bool
	}

	txOptFn func(*transactor)
)

func (fn txOptFn) apply(t *transactor) {
	fn(t)
}
