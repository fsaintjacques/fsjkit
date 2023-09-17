package mailboxtest

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"testing"

	"github.com/fsaintjacques/fsjkit/mailbox"
	"github.com/stretchr/testify/require"
)

const createTpl = `
CREATE TABLE IF NOT EXISTS %s (
	id VARCHAR PRIMARY KEY, 
	metadata JSONB,
	payload VARCHAR, 
	create_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
)`

func createMailboxTable(t *testing.T, db *sql.DB) string {
	t.Helper()
	var (
		//nolint:gosec
		table      = fmt.Sprintf("mailbox_%d", rand.Int())
		createStmt = fmt.Sprintf(createTpl, table)
	)
	_, err := db.Exec(createStmt)
	require.NoError(t, err)
	return table
}

func metadata(kvs ...string) map[string]string {
	m := make(map[string]string)
	if len(kvs)%2 != 0 {
		panic("metadata: invalid number of arguments")
	}

	for i := 0; i < len(kvs); i += 2 {
		m[kvs[i]] = kvs[i+1]
	}

	return m
}

func TestMailbox(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, err := pg.Open("pgx")
	require.NoError(t, err)

	table := createMailboxTable(t, db)
	m := mailbox.NewMailbox(table)

	tx, err := db.Begin()
	require.NoError(t, err)
	m.Put(ctx, tx, mailbox.Message{ID: "1", Metadata: metadata("k1", "v1"), Payload: []byte("hello world")})
	m.Put(ctx, tx, mailbox.Message{ID: "2", Payload: []byte("hello world 2")})
	require.NoError(t, tx.Commit())
}
