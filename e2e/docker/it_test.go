package dockertest

import (
	"testing"

	"github.com/fsaintjacques/fsjkit/docker"
)

func TestMain(m *testing.M) {
	docker.MainWithServices(m, pgSvc, redisSvc)
}
