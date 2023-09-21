package mailbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/fsaintjacques/fsjkit/tx"
)

// Error returned when the mailbox is empty.
var ErrNoMessage = errors.New("no message in mailbox")

type (
	// ConsumeFn is the function that is called by the processor to consume a
	// message. If the function returns an error, the message is re-queued and
	// will be processed again until success.
	ConsumeFn = func(context.Context, Message) error

	// Processor consumes messages from the mailbox. Once a message is processed,
	// it is removed from the mailbox. The processing is controlled by the caller
	// via the Process method. The caller is responsible to call this method in
	// a loop. The processor does not have any background goroutines.
	Processor interface {
		// Process processes messages from the mailbox. It is safe to call this
		// method concurrently. This method is meant to be called in a loop, the
		// caller is responsible to apply back-pressure if needed.
		//
		// It is recommended that the function:
		//   - is idempotent, it may be called multiple times for the same message
		//   - returns quickly to avoid holding the row lock for too long
		//   - does not call other methods on the processor to avoid deadlocks
		//
		// The following errors are returned:
		//   - nil: a message was successfully processed
		//   - ErrNoMessage: the mailbox is empty and no message was processed
		// 	 - Any other error: an error occurred while processing a message
		//
		// The processor does not have any dead-letter mechanism. If the function
		// returns an error, the message is re-queued and will be processed again.
		// It is the responsibility of the caller to handle a maximum number of
		// retries and/or to move the message to a dead-letter queue, see the
		// various middlewares for more details, e.g. WithTimeoutConsume,
		// WithRetryPolicyConsume.
		Process(context.Context) error

		// Size returns the number of messages in the mailbox.
		Size(context.Context) (size int64, err error)
	}
)

// NewProcessor creates a new processor. The table must exist and have the same
// schema as required by Mailbox. The processor does not have any background
// goroutines, the caller is responsible to drive the draining in an infinite loop.
//
// The context is not persisted and is only used to validate the database connection
// and schema validation.
func NewProcessor(ctx context.Context, transactor tx.Transactor, table string, consume ConsumeFn) (Processor, error) {
	if err := transactor.InTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		//nolint:gosec
		if _, err := tx.ExecContext(ctx, "SELECT id, payload FROM \""+table+"\" ORDER BY create_time LIMIT 1"); err != nil {
			return fmt.Errorf("invalid table: %w", err)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return &processor{
		transactor: transactor,
		consume:    consume,
		table:      table,
		claimStmt:  "SELECT id, payload FROM \"" + table + "\" ORDER BY create_time LIMIT 1 FOR UPDATE SKIP LOCKED",
		deleteStmt: "DELETE FROM \"" + table + "\" WHERE id = $1",
		countStmt:  "SELECT COUNT(*) FROM \"" + table + "\"",
	}, nil
}

// Process implements the Processor interface.
func (p *processor) Process(ctx context.Context) error {
	return p.transactor.InTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var msg Message

		if err := tx.QueryRowContext(ctx, p.claimStmt).Scan(&msg.ID, &msg.Payload); errors.Is(err, sql.ErrNoRows) {
			return ErrNoMessage
		} else if err != nil {
			return fmt.Errorf("failed claiming message: %w", err)
		}

		if err := p.consume(ctx, msg); err != nil {
			return fmt.Errorf("failed processing message: %w", err)
		}

		if _, err := tx.ExecContext(ctx, p.deleteStmt, msg.ID); err != nil {
			return fmt.Errorf("failed to delete message: %w", err)
		}

		return nil
	})
}

// Size implements the Processor interface.
func (p *processor) Size(ctx context.Context) (size int64, err error) {
	err = p.transactor.InTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if err := tx.QueryRowContext(ctx, p.countStmt).Scan(&size); err != nil {
			return fmt.Errorf("failed to count messages: %w", err)
		}
		return nil
	})
	return
}

type (
	processor struct {
		transactor                       tx.Transactor
		consume                          ConsumeFn
		table                            string
		claimStmt, deleteStmt, countStmt string
	}
)
