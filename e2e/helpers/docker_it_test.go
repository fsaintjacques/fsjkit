package helpers

import (
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var pg *PostgresService

func TestPostgresService(t *testing.T) {
	db, err := pg.Open("pgx")
	require.NoError(t, err)

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS test (id SERIAL PRIMARY KEY, name VARCHAR(255))")
	assert.NoError(t, err)
}

func TestMain(m *testing.M) {
	MainWithPostgres(m, &pg, &PostgresServiceConfig{
		Repository: "postgres", Tag: "15", Database: "test",
	})
}
