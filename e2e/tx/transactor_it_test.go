package txtest

import (
	"context"
	"database/sql"
	"testing"

	"github.com/fsaintjacques/fsjkit/tx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransactor(t *testing.T) {
	t.Parallel()

	mustExec := func(ctx context.Context, tx *sql.Tx, query string) {
		t.Helper()
		_, err := tx.ExecContext(ctx, query)
		assert.NoError(t, err)
	}
	mustFail := func(ctx context.Context, tx *sql.Tx, query string) {
		t.Helper()
		_, err := tx.ExecContext(ctx, query)
		assert.NotNil(t, err)
	}
	mustScan := func(ctx context.Context, tx *sql.Tx, query string, dest ...interface{}) {
		t.Helper()
		assert.NoError(t, tx.QueryRowContext(ctx, query).Scan(dest...))
	}

	db, err := pg.Open("pgx")
	require.NoError(t, err)

	var ctx = context.Background()

	t.Run("NewTransactor", func(t *testing.T) {
		t.Run("ShouldPanicOnNilOpener", func(t *testing.T) {
			assert.Panics(t, func() { tx.NewTransactor(nil) })
		})
		t.Run("WithTxOptions", func(t *testing.T) {
			txor := tx.NewTransactor(db, tx.WithTxOptions(&sql.TxOptions{ReadOnly: true}))
			assert.NotNil(t, txor.InTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
				// This should fail because the transaction is read-only.
				mustFail(ctx, tx, "CREATE TEMP TABLE foo (id int)")
				return nil
			}))
		})
	})

	t.Run("InTx", func(t *testing.T) {
		txor := tx.NewTransactor(db)
		t.Run("CommitsOnNoError", func(t *testing.T) {
			assert.NoError(t, txor.InTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
				mustExec(ctx, tx, "CREATE TABLE commits_on_nil (id int)")
				mustExec(ctx, tx, "INSERT INTO commits_on_nil VALUES (1)")
				return nil
			}))

			assert.NoError(t, txor.InTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
				var count int
				mustScan(ctx, tx, "SELECT COUNT(*) FROM commits_on_nil", &count)
				assert.Equal(t, 1, count)
				return nil
			}))
		})

		t.Run("RollbacksOnError", func(t *testing.T) {
			assert.ErrorIs(t, assert.AnError, txor.InTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
				mustExec(ctx, tx, "CREATE TABLE foo (id int)")
				mustExec(ctx, tx, "SELECT COUNT(*) FROM foo")
				// Return an error to rollback the transaction.
				return assert.AnError
			}))

			assert.NotNil(t, txor.InTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
				// This should fail because the table was rolled back.
				mustFail(ctx, tx, "SELECT COUNT(*) FROM foo")
				return nil
			}))
		})

		t.Run("SavesTxInContext", func(t *testing.T) {
			assert.NoError(t, txor.InTx(ctx, func(ctx context.Context, _ *sql.Tx) error {
				txn, found := tx.FromContext(ctx)
				assert.True(t, found)
				assert.NotNil(t, txn)
				return nil
			}))
		})

		t.Run("PartialStateWithtoutSavepoints", func(t *testing.T) {
			assert.NoError(t, txor.InTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
				mustExec(ctx, tx, "CREATE TABLE partial_state (id int)")

				assert.ErrorIs(t, assert.AnError, txor.InTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
					// Because parent tx is saved in the context, the table should be accessible.
					mustExec(ctx, tx, "INSERT INTO partial_state VALUES (1)")
					return assert.AnError
				}))

				var count int
				// This statement should success because only the parent transaction can be rolled back.
				mustScan(ctx, tx, "SELECT COUNT(*) FROM commits_on_nil", &count)
				// Even if the sub-transaction failed, the insert is not reverted because there is no savepoint.
				assert.Equal(t, 1, count)
				return nil
			}))
		})

		t.Run("PartialStateWithSavepoints", func(t *testing.T) {
			txor := tx.NewTransactor(db, tx.WithSavepoints())

			assert.NoError(t, txor.InTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
				mustExec(ctx, tx, "CREATE TABLE test_savepoints (id int)")

				assert.NoError(t, txor.InTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
					mustExec(ctx, tx, "INSERT INTO test_savepoints VALUES (1)")

					txor.InTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
						mustExec(ctx, tx, "INSERT INTO test_savepoints VALUES (2)")
						// Previous insert shall be reverted.
						return assert.AnError
					})

					mustExec(ctx, tx, "INSERT INTO test_savepoints VALUES (3)")
					return nil
				}))

				assert.ErrorIs(t, assert.AnError, txor.InTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
					mustExec(ctx, tx, "INSERT INTO test_savepoints VALUES (4)")
					return assert.AnError
				}))

				var count int
				mustScan(ctx, tx, "SELECT SUM(id) FROM test_savepoints", &count)
				// All odds have been committed.
				assert.Equal(t, 1+3, count)
				return nil
			}))
		})
	})
}
