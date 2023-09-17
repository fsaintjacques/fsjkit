package mailbox

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/fsaintjacques/fsjkit/mailbox/internal"
)

type (
	// Message is a message in the mailbox.
	Message struct {
		// The message identifier.
		ID string
		// The message metadata.
		Metadata map[string]string
		// The message payload.
		Payload []byte
	}

	// Mailbox is a message queue backed by Postgres.
	Mailbox interface {
		// Put adds a message to the mailbox. If the method returns an error,
		// the message is not added to the mailbox and caller is responsible
		// for retrying and/or rolling back the transaction. The transaction is
		// explicitly passed allowing the caller to control the transaction boundaries.
		Put(context.Context, *sql.Tx, Message) error
	}

	Metadata = internal.Metadata
)

// NewMailbox creates a new mailbox. The table must exist
// and have the following schema:
//
// - id VARCHAR PRIMARY KEY
// - metadata JSONB
// - payload BYTEA
// - create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
//
// The table name must be fully qualified, for example: "public.mailbox".
func NewMailbox(table string) Mailbox {
	//nolint:gosec
	stmt := "INSERT INTO \"" + table + "\" (id, metadata, payload) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING"
	return mboxFn(func(ctx context.Context, tx *sql.Tx, msg Message) error {
		var m = internal.Metadata(msg.Metadata)
		if _, err := tx.ExecContext(ctx, stmt, msg.ID, m, msg.Payload); err != nil {
			return fmt.Errorf("tx.ExecContext: %w", err)
		}
		return nil
	})
}

type mboxFn func(context.Context, *sql.Tx, Message) error

func (fn mboxFn) Put(ctx context.Context, tx *sql.Tx, msg Message) error {
	return fn(ctx, tx, msg)
}
