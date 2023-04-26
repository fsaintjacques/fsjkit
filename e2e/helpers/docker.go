package helpers

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

type (
	// PostgresService is a helper struct for running tests with a Postgres database.
	// It will start a Postgres database in a Docker container and provide the connection
	// parameters for the database.
	PostgresService struct {
		// Docker resource
		r *dockertest.Resource
		// Connection parameters
		host, port, user, password, db string
	}

	PostgresServiceConfig struct {
		// Docker repository and tag to use for the Postgres image.
		Repository, Tag string
		// Database name to create.
		Database string
	}
)

// NewPostgresService creates a new PostgresService.
func NewPostgresService(p *dockertest.Pool, c *PostgresServiceConfig) (*PostgresService, error) {
	var (
		user     = "postgres"
		password = "postgres"
	)

	r, err := p.RunWithOptions(&dockertest.RunOptions{
		Repository: c.Repository, Tag: c.Tag,
		Env: []string{
			"POSTGRES_USER=" + user,
			"POSTGRES_PASSWORD=" + password,
			"POSTGRES_DB=" + c.Database,
		},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.NeverRestart()
	})
	if err != nil {
		return nil, err
	}

	const pgPort = "5432/tcp"
	svc := &PostgresService{
		r:    r,
		host: r.GetBoundIP(pgPort), port: r.GetPort(pgPort),
		user: user, password: password, db: c.Database,
	}

	if err := p.Retry(func() error {
		db, err := svc.Open("pgx")
		if err != nil {
			return err
		}
		return db.Ping()
	}); err != nil {
		r.Close()
		return nil, err
	}

	return svc, nil
}

// DSN returns the connection string for the Postgres database.
func (s *PostgresService) DSN() string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		s.host, s.port, s.user, s.password, s.db)
}

// Open opens an sql.DB to the Postgres database. The caller must provide the driver
// and close the database when done.
func (s *PostgresService) Open(driver string) (*sql.DB, error) {
	return sql.Open(driver, s.DSN())
}

// Close closes the PostgresService and removes the Docker container.
func (s *PostgresService) Close() error {
	return s.r.Close()
}

// MainWithPostgres is a helper function for running tests with a PostgresService.
// It will start the service, set the pointer to the service, and run the tests.
//
//	var pg *PostgresService
//
//	func TestPostgresService(t *testing.T) {
//		db, err := pg.Open("pgx")
//		require.NoError(t, err)
//	}
//
//	func TestMain(m *testing.M) {
//		RunWithMain(m, &pg, &PostgresServiceConfig{
//			Repository: "postgres", Tag: "15",
//			Database: "test",
//		})
//	}
func MainWithPostgres(m *testing.M, ptr **PostgresService, pgCfg *PostgresServiceConfig) {
	os.Exit(func() int {
		pool, err := dockertest.NewPool("")
		if err != nil {
			log.Fatalf("Could not connect to docker: %s", err)
		}

		pg, err := NewPostgresService(pool, pgCfg)
		if err != nil {
			log.Fatalf("Could not start resource: %s", err)
		}

		defer func() {
			// Ensure that the PostgresService is closed and the Docker container is removed.
			if r := recover(); r != nil {
				panic(r)
			}
			defer pg.Close()
		}()

		// Set the pointer to the PostgresService such that tests can access the service.
		*ptr = pg
		return m.Run()
	}())
}
