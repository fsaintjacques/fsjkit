package txtest

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	ftx "github.com/fsaintjacques/fsjkit/tx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTransactorPanics(t *testing.T) {
	assert.Panics(t, func() { ftx.NewTransactor(nil) }, "should panic if db is nil")
}

func TestTransactorInterface(t *testing.T) {
	tests := []transactorMockTest{
		{
			"CommitsOnSuccess",
			func(m sqlmock.Sqlmock) {
				m.ExpectBegin()
				m.ExpectExec("SELECT 1").WillReturnResult(nil)
				m.ExpectCommit()
			},
			func(ctx context.Context, tx *sql.Tx) error {
				tx.ExecContext(ctx, "SELECT 1")
				return nil
			},
			noOptions,
			noError,
		},
		{
			"RollbacksOnErrorAndReturnsError",
			func(m sqlmock.Sqlmock) {
				m.ExpectBegin()
				m.ExpectRollback()
			},
			func(ctx context.Context, tx *sql.Tx) error { return anError },
			noOptions,
			anError,
		},
		{
			"BeginsErrorSkipClosure",
			func(m sqlmock.Sqlmock) {
				m.ExpectBegin().WillReturnError(anError)
			},
			func(ctx context.Context, tx *sql.Tx) error { panic("unreachable") },
			noOptions,
			anError,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) { test.run(t) })
	}
}

func TestTransactorOptions(t *testing.T) {
	tests := []transactorMockTest{
		/*
			// sqlmock does not support expecting the tx options.
			{
				"WithTxOptionsArePassedToBegin",
				func(m sqlmock.Sqlmock) {
					m.ExpectBegin()
					m.ExpectCommit()
				},
				func(ctx context.Context, tx *sql.Tx) error { return nil },
				[]ftx.TransactorOption{ftx.WithTxOptions(&sql.TxOptions{ReadOnly: true})},
				noError,
			},
		*/
		{
			"WithRecursionOptionUsesSameTx",
			func(m sqlmock.Sqlmock) {
				m.ExpectBegin()
				m.ExpectExec("SELECT parent").WillReturnResult(nil)
				m.ExpectExec("SELECT child").WillReturnResult(nil)
				m.ExpectCommit()
			},
			func(ctx context.Context, tx *sql.Tx) error {
				tx.ExecContext(ctx, "SELECT parent")
				txor := ctx.Value(txorKey).(ftx.Transactor)
				if err := txor.InTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
					tx.ExecContext(ctx, "SELECT child")
					return nil
				}); err != nil {
					return err
				}

				return nil
			},
			[]ftx.TransactorOption{ftx.WithRecursiveContext()},
			noError,
		},
		{
			"WithAlwaysRollbackOptionRollbacksOnSuccess",
			func(m sqlmock.Sqlmock) {
				m.ExpectBegin()
				m.ExpectRollback()
			},
			func(ctx context.Context, tx *sql.Tx) error { return nil },
			[]ftx.TransactorOption{ftx.WithAlwaysRollback()},
			// The explicit rollback is not an error.
			noError,
		},
		{
			"WithMiddlewareOptionCallsMiddlewares",
			func(m sqlmock.Sqlmock) {
				m.ExpectBegin()
				m.ExpectExec("SET LOCAL myvar = 1").WillReturnResult(nil)
				m.ExpectExec("SELECT closure").WillReturnResult(nil)
				m.ExpectExec("SELECT in_defer").WillReturnResult(nil)
				m.ExpectCommit()
			},
			func(ctx context.Context, tx *sql.Tx) error {
				tx.Exec("SELECT closure")
				return nil
			},
			[]ftx.TransactorOption{ftx.WithTxMiddlewares(
				func(fn ftx.TxClosure) ftx.TxClosure {
					return func(ctx context.Context, tx *sql.Tx) error {
						tx.ExecContext(ctx, "SET LOCAL myvar = 1")
						return fn(ctx, tx)
					}
				},
				func(fn ftx.TxClosure) ftx.TxClosure {
					return func(ctx context.Context, tx *sql.Tx) error {
						defer tx.ExecContext(ctx, "SELECT in_defer")
						return fn(ctx, tx)
					}
				},
			)},
			noError,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) { test.run(t) })
	}
}

type (
	transactorMockTest struct {
		// Name of the test
		name string
		// Object to set expectations on
		mockExpectation func(sqlmock.Sqlmock)
		// Closure passed to Transactor.InTx
		closure ftx.TxClosure
		// Options passed to NewTransactor
		options []ftx.TransactorOption
		// Controls how the error from Transactor.InTx is
		// asserted. It can be a bool, an error, or nil.
		//
		// If the type is bool:
		//  - true means an error expected and will be asserted
		// 	with assert.Error,
		//  - false means no error expected and will be asserted
		// 	with assert.NoError
		// If the type is error, an error is expected and will be
		// asserted with assert.ErrorIs
		expectedError any
	}
)

var (
	noOptions      = []ftx.TransactorOption{}
	anError        = errors.New("error")
	noError   bool = false
)

func (m *transactorMockTest) run(t *testing.T) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	m.mockExpectation(mock)

	// Allows closure to access the transactor.
	transactor := ftx.NewTransactor(db, m.options...)
	ctx := context.WithValue(context.Background(), txorKey, transactor)
	err = transactor.InTx(ctx, m.closure)

	switch v := m.expectedError.(type) {
	case bool:
		{
			switch v {
			case false:
				assert.NoError(t, err)
			case true:
				assert.Error(t, err)
			}
		}
	case error:
		assert.ErrorIs(t, err, v)
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}

type txorKeyType int

var txorKey txorKeyType = 0
