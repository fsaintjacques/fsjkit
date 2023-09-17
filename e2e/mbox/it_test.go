package mboxtest

import (
	"e2e/helpers"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
)

var pg *helpers.PostgresService

func TestMain(m *testing.M) {
	helpers.MainWithPostgres(m, &pg, &helpers.PostgresServiceConfig{
		Repository: "postgres", Tag: "15", Database: "test",
	})
}
