package docker

import (
	"log"
	"os"
	"testing"

	"github.com/ory/dockertest/v3"
)

type (
	// Service is a service that can be started in a Docker container.
	Service interface {
		// Init starts the service in a Docker container.
		Init(*dockertest.Pool) error
		// Close stops the service.
		Close() error
	}
)

// MainWithServices is a helper function for running tests with multiples docker
// services. It will start the services in Docker containers and then run the
// tests. The services will be closed after the tests are run. The main function
// is invoked once per test package.
//
//	var (
//		pgCfg = docker.PostgresServiceConfig{Repository: "postgres", Tag: "15", Database: "test"}
//		pgSvc = docker.NewPostgresService(pgCfg)
//	)
//
//	func TestPostgresService(t *testing.T) {
//		db, err := pgSvc.Open()
//		require.NoError(t, err)
//	}
//
//	func TestMain(m *testing.M) {
//		docker.MainWithServices(m, pgSvc)
//	}
func MainWithServices(m *testing.M, services ...Service) {
	os.Exit(func() int {
		pool, err := dockertest.NewPool("")
		if err != nil {
			log.Fatalf("Could not connect to docker: %s", err)
		}

		defer func() {
			for _, svc := range services {
				defer svc.Close()
			}
		}()

		for _, svc := range services {
			if err := svc.Init(pool); err != nil {
				log.Fatalf("Could not start resource: %s", err)
			}
		}

		return m.Run()
	}())
}
