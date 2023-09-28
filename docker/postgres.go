package docker

import (
	"database/sql"
	"fmt"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

type (
	// PostgresService is a helper struct for running tests with a Postgres database.
	// It will start a Postgres database in a Docker container and provide the connection
	// parameters for the database.
	PostgresService struct {
		c PostgresServiceConfig
		// internal docker resource
		r *dockertest.Resource
		// Connection parameters
		host, port, user, password string
	}

	PostgresServiceConfig struct {
		// Docker repository and tag to use for the Postgres image.
		Repository, Tag string
		// Database name to create.
		Database string
		// Driver string
		Driver string
	}
)

// NewPostgresService creates a new PostgresService.
func NewPostgresService(c PostgresServiceConfig) *PostgresService {
	const (
		user     = "postgres"
		password = "postgres"
	)

	return &PostgresService{
		c:    c,
		user: user, password: password,
	}
}

func (s *PostgresService) Init(p *dockertest.Pool) error {
	r, err := p.RunWithOptions(&dockertest.RunOptions{
		Repository: s.c.Repository, Tag: s.c.Tag,
		Env: []string{
			"POSTGRES_USER=" + s.user,
			"POSTGRES_PASSWORD=" + s.password,
			"POSTGRES_DB=" + s.c.Database,
		},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.NeverRestart()
	})
	if err != nil {
		return fmt.Errorf("pool.RunWithOptions: %w", err)
	}

	const pgPort = "5432/tcp"
	s.host, s.port = r.GetBoundIP(pgPort), r.GetPort(pgPort)
	s.r = r

	if err := p.Retry(func() error {
		db, err := s.Open()
		if err != nil {
			return err
		}
		defer db.Close()
		return db.Ping()
	}); err != nil {
		return fmt.Errorf("pool.Retry: %w", err)
	}
	return nil
}

// DSN returns the connection string for the Postgres database.
func (s *PostgresService) DSN() string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		s.host, s.port, s.user, s.password, s.c.Database)
}

// Open opens an sql.DB to the Postgres database. The caller must provide the driver
// and close the database when done.
func (s *PostgresService) Open() (*sql.DB, error) {
	return sql.Open(s.c.Driver, s.DSN())
}

// Close closes the PostgresService and removes the Docker container.
func (s *PostgresService) Close() error {
	if s.r != nil {
		return s.r.Close()
	}
	return nil
}
