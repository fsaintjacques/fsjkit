package mbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/fsaintjacques/fsjkit/tx"
)

type (
	// Processor consumes messages from the mailbox. Once a message is
	// processed, it is removed from the mailbox. The processing is controlled by
	// the caller via the Process method. The caller is responsible to call this method
	// in a loop. The processor does not have any background goroutines.
	Processor struct {
		table string
		txor  tx.Transactor

		claimStmt, deleteStmt, countStmt string
	}
)

// NewProcessor creates a new processor. The table must exist and have the same
// schema as required by Mailbox.
func NewProcessor(ctx context.Context, db *sql.DB, table string) (*Processor, error) {
	checkStmt := "SELECT id, payload FROM \"" + table + "\" ORDER BY create_time LIMIT 1"
	if _, err := db.ExecContext(ctx, checkStmt); err != nil {
		return nil, fmt.Errorf("invalid table: %w", err)
	}

	txor := tx.NewTransactor(db,
		tx.WithTxOptions(&sql.TxOptions{Isolation: sql.LevelSerializable}))

	return &Processor{
		txor: txor, table: table,
		claimStmt:  "SELECT id, payload FROM \"" + table + "\" FOR UPDATE SKIP LOCKED ORDER BY create_time LIMIT 1",
		deleteStmt: "DELETE FROM \"" + table + "\" WHERE id = $1",
		countStmt:  "SELECT COUNT(*) FROM \"" + table + "\"",
	}, nil
}

// Process processes messages from the mailbox. It is safe to call this method concurrently.
// This method is meant to be called in a loop, the caller is responsible to apply backpressure
// if needed.
//
// It is recommended that the function:
//   - is idempotent because it may be called multiple times for the same message
//   - returns as soon as possible to avoid holding the
//   - does not call other methods on the processor to avoid deadlocks
//
// The following return values are possible:
//   - found = false, err = nil: no message was processed, the mailbox is empty
//   - found = true, err = nil: a message was successfully processed
//   - found = false, err != nil: an error occurred, the caller should retry
//   - found = true, err != nil: a message was processed but an error occurred, the caller should retry
//
// The processor does not have any deadletter mechanism. If the function returns an error, the message
// is re-queued and will be processed again. It is the responsibility of the caller to handle a maximum number
// of retries and/or to move the message to a deadletter queue. It could be as simple as incrementing a counter
// keyed by the message ID and if a threshold is reached, move the message to a deadletter queue. The deadletter queue
// can be a Mailbox backed by a different table.
func (p *Processor) Process(ctx context.Context, fn func(context.Context, *Message) error) (found bool, err error) {
	err = p.txor.InTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var msg Message

		if err := tx.QueryRowContext(ctx, p.claimStmt).Scan(&msg.ID, &msg.Payload); errors.Is(err, sql.ErrNoRows) {
			return nil
		} else if err != nil {
			return fmt.Errorf("failed claiming message: %w", err)
		}

		// We have a message to process.
		found = true

		if err := fn(ctx, &msg); err != nil {
			return fmt.Errorf("failed processing message: %w", err)
		}

		if _, err := tx.ExecContext(ctx, p.deleteStmt, msg.ID); err != nil {
			return fmt.Errorf("failed to delete message: %w", err)
		}

		return nil
	})

	return
}

// Size returns the number of messages in the mailbox.
func (p *Processor) Size(ctx context.Context) (size int64, err error) {
	err = p.txor.InTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if err := tx.QueryRowContext(ctx, p.countStmt).Scan(&size); err != nil {
			return fmt.Errorf("failed to count messages: %w", err)
		}
		return nil
	})
	return
}
