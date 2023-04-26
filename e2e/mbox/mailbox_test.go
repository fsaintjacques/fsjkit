package mboxtest

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/fsaintjacques/fsjkit/mbox"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMailbox(t *testing.T) {
	var (
		tbl     = "mailbox"
		mailbox = mbox.NewMailbox(tbl)
	)

	tests := []struct {
		name    string
		tbl     string
		payload []byte
	}{
		{
			name:    "BasicPut",
			tbl:     tbl,
			payload: []byte("hello world"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()

			db, mock, err := sqlmock.New()
			require.NoError(t, err)

			//nolint:gosec
			id := fmt.Sprintf("%d", rand.Int())

			mock.ExpectBegin()
			mock.ExpectExec("INSERT INTO \""+test.tbl+"\" \\(id, payload\\) VALUES \\(\\$1, \\$2\\) ON CONFLICT DO NOTHING").
				WithArgs(id, test.payload).WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectCommit()

			tx, err := db.BeginTx(ctx, nil)
			require.NoError(t, err)
			assert.NoError(t, mailbox.Put(ctx, tx, &mbox.Message{ID: id, Payload: test.payload}))
			assert.NoError(t, tx.Commit())

			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
