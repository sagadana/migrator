package states

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type RedisStore[V any] struct {
	client   *redis.Client
	hashName string
	isClosed bool
}

func NewRedisStateStore(addr string, password string, db int, hashName string) *RedisStore[State] {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		panic(fmt.Errorf("redis connection failed: %w", err))
	}

	return &RedisStore[State]{
		client:   client,
		hashName: hashName,
	}
}

func (s *RedisStore[V]) Store(ctx *context.Context, key string, value V) error {
	if s.isClosed {
		return ErrClosed
	}

	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("error marshaling state: %w", err)
	}

	if err := s.client.HSet(*ctx, s.hashName, key, data).Err(); err != nil {
		return fmt.Errorf("redis hset failed: %w", err)
	}

	return nil
}

func (s *RedisStore[V]) Load(ctx *context.Context, key string) (value V, ok bool) {
	if s.isClosed {
		return value, false
	}

	data, err := s.client.HGet(*ctx, s.hashName, key).Bytes()
	if err == redis.Nil {
		return value, false
	}
	if err != nil {
		return value, false
	}

	var result V
	if err := json.Unmarshal(data, &result); err != nil {
		return value, false
	}

	return result, true
}

func (s *RedisStore[V]) Delete(ctx *context.Context, key string) error {
	if s.isClosed {
		return ErrClosed
	}

	if err := s.client.HDel(*ctx, s.hashName, key).Err(); err != nil {
		return fmt.Errorf("redis hdel failed: %w", err)
	}

	return nil
}

func (s *RedisStore[V]) Clear(ctx *context.Context) error {
	if s.isClosed {
		return ErrClosed
	}

	if err := s.client.Del(*ctx, s.hashName).Err(); err != nil {
		return fmt.Errorf("redis del failed: %w", err)
	}

	return nil
}

func (s *RedisStore[V]) Close(ctx *context.Context) error {
	s.isClosed = true
	return s.client.Close()
}
