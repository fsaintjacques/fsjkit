package docker

import (
	"fmt"
	"net"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

type (
	// RedisService is a helper struct for running tests with a Redis database.
	RedisService struct {
		c          RedisServiceConfig
		r          *dockertest.Resource
		host, port string
	}

	RedisServiceConfig struct {
		// Docker repository and tag to use for the Redis image.
		Repository, Tag string
	}
)

// NewRedisService creates a RedisService.
func NewRedisService(c RedisServiceConfig) *RedisService {
	return &RedisService{c: c}
}

// Addr returns the address of the service, pass this to go-redis client.
func (s *RedisService) Addr() string {
	return s.host + ":" + s.port
}

// Init implements Service.Init.
func (s *RedisService) Init(p *dockertest.Pool) error {
	r, err := p.RunWithOptions(&dockertest.RunOptions{
		Repository: s.c.Repository, Tag: s.c.Tag,
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.NeverRestart()
	})
	if err != nil {
		return fmt.Errorf("pool.RunWithOptions: %w", err)
	}

	const redisPort = "6379/tcp"
	s.host, s.port = r.GetBoundIP(redisPort), r.GetPort(redisPort)
	s.r = r

	// Wait for the service to become available. Doesn't require the redis client as
	// it's a simple TCP connection.
	ping := func() (err error) {
		conn, err := net.Dial("tcp", s.host+":"+s.port)
		if err != nil {
			return err
		}
		defer conn.Close()

		const (
			pingReq = "PING\r\n"
			pongRep = "+PONG\r\n"
		)
		n, err := fmt.Fprint(conn, pingReq)
		if err != nil || n != len(pingReq) {
			return fmt.Errorf("fmt.Fprintf: %w", err)
		}

		buf := make([]byte, len(pongRep))
		if _, err := conn.Read(buf); err != nil {
			return fmt.Errorf("conn.Read: %w", err)
		}

		if string(buf) != pongRep {
			return fmt.Errorf("unexpected response: %s", buf)
		}

		return nil
	}

	if err := p.Retry(ping); err != nil {
		return fmt.Errorf("pool.Retry: %w", err)
	}
	return nil
}

// Close implements Service.Close.
func (s *RedisService) Close() error {
	if s.r != nil {
		return s.r.Close()
	}
	return nil
}
