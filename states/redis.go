package states

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/redis/go-redis/v9"
)

type RedisStore[V any] struct {
	client *redis.Client
	mutex  *sync.Mutex
	prefix string

	isClosed bool
}

// Add this function to handle key management
func (s *RedisStore[V]) getFullKey(key string) string {
	return s.prefix + ":" + key
}

// Add this function to handle list key management
func (s *RedisStore[V]) getListKey() string {
	return s.getFullKey("list")
}

func (ds *RedisStore[V]) Client() *redis.Client {
	return ds.client
}

// Store method
func (s *RedisStore[V]) Store(ctx *context.Context, key string, value V) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.isClosed {
		return ErrStoreClosed
	}

	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("error marshaling state: %w", err)
	}

	pipe := s.client.TxPipeline()

	pipe.Set(*ctx, s.getFullKey(key), data, 0)
	pipe.SAdd(*ctx, s.getListKey(), key)

	_, err = pipe.Exec(*ctx)
	if err != nil {
		return fmt.Errorf("redis pipeline failed: %w", err)
	}

	return nil
}

// Load method
func (s *RedisStore[V]) Load(ctx *context.Context, key string) (value V, ok bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.isClosed {
		return value, false
	}

	data, err := s.client.Get(*ctx, s.getFullKey(key)).Bytes()
	if err == redis.Nil {
		return value, false
	}
	if err != nil {
		panic(err)
		// slog.Warn(fmt.Sprintf("Failed reading state: %s. Error: %v", key, err))
		// return value, false
	}

	var result V
	if err := json.Unmarshal(data, &result); err != nil {
		slog.Warn(fmt.Sprintf("Failed parsing state: %s. Error: %v", key, err))
		return value, false
	}

	return result, true
}

// Delete method
func (s *RedisStore[V]) Delete(ctx *context.Context, key string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.isClosed {
		return ErrStoreClosed
	}

	pipe := s.client.TxPipeline()

	pipe.Del(*ctx, s.getFullKey(key))
	pipe.SRem(*ctx, s.getListKey(), key)

	_, err := pipe.Exec(*ctx)
	if err != nil {
		return fmt.Errorf("redis transaction failed: %w", err)
	}

	return nil
}

// Clear method to use the keys list
func (s *RedisStore[V]) Clear(ctx *context.Context) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.isClosed {
		return ErrStoreClosed
	}

	listKey := s.getListKey()

	keys, err := s.client.SMembers(*ctx, listKey).Result()
	if err != nil {
		return fmt.Errorf("failed to get keys list: %w", err)
	}

	pipe := s.client.TxPipeline()

	// Delete all state keys
	for _, key := range keys {
		pipe.Del(*ctx, s.getFullKey(key))
	}

	// Delete the keys list itself
	pipe.Del(*ctx, listKey)

	// Execute all commands in the pipeline
	if _, err := pipe.Exec(*ctx); err != nil {
		return fmt.Errorf("redis pipeline failed: %w", err)
	}

	return nil
}

// Close store
func (s *RedisStore[V]) Close(ctx *context.Context) error {
	if err := s.client.Close(); err != nil {
		return err
	}
	s.isClosed = true
	return nil
}

// Creates and returns a new generic redis store.
func NewRedisStore[V any](ctx *context.Context, addr string, password string, db int, prefix string) *RedisStore[V] {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	if err := client.Ping(*ctx).Err(); err != nil {
		panic(fmt.Errorf("redis connection failed: %w", err))
	}

	return &RedisStore[V]{
		client: client,
		mutex:  new(sync.Mutex),
		prefix: prefix,
	}
}

// Creates and returns a new redis state store.
func NewRedisStateStore(ctx *context.Context, addr string, password string, db int, prefix string) *RedisStore[State] {
	return NewRedisStore[State](ctx, addr, password, db, prefix)
}
