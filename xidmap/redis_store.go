package xidmap

import (
	"context"
	"github.com/go-redis/redis/v8"
)

type RedisStore struct {
	client *redis.Client
}

var ctx = context.Background()

func NewRedisStore(db int) *RedisStore {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       db,
	})
	return &RedisStore{
		client: rdb,
	}
}

func (r *RedisStore) Get(key string) (uint64, error) {
	v, e := r.client.Get(ctx, key).Uint64()
	if e == redis.Nil {
		return 0, nil
	} else {
		return v, e
	}
}

func (r *RedisStore) Put(key string, val uint64) error {
	return r.client.Set(ctx, key, val, 0).Err()
}

func (r *RedisStore) Release() error {
	return r.client.FlushDB(ctx).Err()
}
