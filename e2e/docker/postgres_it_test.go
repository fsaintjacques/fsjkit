package dockertest

import (
	"testing"

	"github.com/fsaintjacques/fsjkit/docker"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
)

var (
	pgCfg = docker.PostgresServiceConfig{Repository: "postgres", Tag: "15", Database: "test", Driver: "pgx"}
	pgSvc = docker.NewPostgresService(pgCfg)
)

func TestPostgresService(t *testing.T) {
	db, err := pgSvc.Open()
	require.NoError(t, err)
	require.NotNil(t, db)

	_, err = db.Exec("SELECT 1;")
	require.NoError(t, err)
}
