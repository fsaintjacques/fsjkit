package txtest

import (
	"testing"

	"github.com/fsaintjacques/fsjkit/docker"
	_ "github.com/jackc/pgx/v5/stdlib"
)

var (
	pgCfg = docker.PostgresServiceConfig{Repository: "postgres", Tag: "15", Database: "test", Driver: "pgx"}
	pg    = docker.NewPostgresService(pgCfg)
)

func TestMain(m *testing.M) {
	docker.MainWithServices(m, pg)
}
