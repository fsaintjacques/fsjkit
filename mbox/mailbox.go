package mbox

import (
	"context"
	"database/sql"
	"fmt"
)

type (
	// Message is a message in the mailbox.
	Message struct {
		// The message identifier.
		ID string
		// The message payload.
		Payload []byte
	}

	// Mailbox is a message queue backed by Postgres.
	Mailbox interface {
		// Put adds a message to the mailbox. If the method returns an error,
		// the message is not added to the mailbox and caller is responsible
		// for retrying and/or rolling back the transaction. The transaction is
		// explicitly passed to the method to allow the caller to control the
		// transaction boundaries.
		Put(context.Context, *sql.Tx, *Message) error
	}
)

// NewMailbox creates a new mailbox. The table must exist
// and have the following schema:
//
// - id VARCHAR(255) PRIMARY KEY
// - payload BYTEA
// - create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
//
// The table name must be fully qualified, for example: "public.mailbox".
func NewMailbox(table string) Mailbox {
	return &mailboxImpl{
		tableName:  table,
		insertStmt: "INSERT INTO \"" + table + "\" (id, payload) VALUES ($1, $2) ON CONFLICT DO NOTHING",
	}
}

func (i *mailboxImpl) Put(ctx context.Context, tx *sql.Tx, msg *Message) error {
	if _, err := tx.ExecContext(ctx, i.insertStmt, msg.ID, msg.Payload); err != nil {
		return fmt.Errorf("tx.ExecContext: %w", err)
	}
	return nil
}

type mailboxImpl struct {
	tableName  string
	insertStmt string
}
