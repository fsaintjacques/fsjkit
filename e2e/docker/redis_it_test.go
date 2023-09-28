package dockertest

import (
	"context"
	"testing"

	"github.com/fsaintjacques/fsjkit/docker"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

var (
	redisCfg = docker.RedisServiceConfig{Repository: "redis", Tag: "7.2"}
	redisSvc = docker.NewRedisService(redisCfg)
)

func TestRedis(t *testing.T) {
	var (
		ctx = context.Background()
		rdb = redis.NewClient(&redis.Options{
			Addr:     redisSvc.Addr(),
			Password: "", // no password set
			DB:       0,  // use default DB
		})
	)

	require.NoError(t, rdb.Set(ctx, "key", "my-value", 0).Err())
	val, err := rdb.Get(ctx, "key").Result()
	require.NoError(t, err)
	require.Equal(t, val, "my-value")
}
