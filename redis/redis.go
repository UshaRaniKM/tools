// (C) Copyright 2021-2024 Hewlett Packard Enterprise Development LP

package cache

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
)

// While there is no official namespace delimiter in Redis, the de facto delimiter is ":".
const (
	namespaceSeparator = ":"
)

// Non-allocating compile-time check to ensure the Store interface is implemented correctly.
var _ Store = (*RedisStore)(nil)

// redisClient is the interface used internally for cache operations.
type redisClient interface {
	Set(context.Context, string, any, time.Duration) *redis.StatusCmd
	Get(context.Context, string) *redis.StringCmd
}

// RedisOption implementations can be used to configure NewRedisStore.
type RedisOption interface {
	apply(*redisOptions)
}

// The configuration for a RedisStore.
type RedisConfig struct {
	// The address of the Redis server.
	Address string `xconfig:"address"`

	// The password to use for authenticationg to the Redis server.
	Password string `xconfig:"password"`

	// The namespace to use when getting and setting keys.
	Namespace string `xconfig:"namespace"`

	// Whether the client should operate in cluster mode or not. Defaults to false to ensure
	// production clients have it enabled. Local testing will often not use clusters and so should
	// set it to false.
	DisableClusterMode bool `xconfig:"disableclustermode=false"`

	// Whether the client should use TLS or not. TLS is only enabled for clients operating in
	// cluster mode and is not present otherwise. Defaults to false to to ensure production clients
	// have it enabled. Local testing will often not have TLS configured and so should set it to
	// true.
	DisableTLS bool `xconfig:"disabletls=false"`
}

// options converts the optional configuration to the equivalent RedisOption implementations.
func (c *RedisConfig) options() []RedisOption {
	options := []RedisOption{}

	if c.Namespace != "" {
		options = append(options, WithRedisNamespace(c.Namespace))
	}

	if !c.DisableClusterMode {
		options = append(options, WithRedisClusterMode())
	}

	if !c.DisableTLS {
		options = append(options, WithRedisTLS())
	}

	return options
}

// RedisStore is an implementation of the Store interface using Redis.
type RedisStore struct {
	client    redisClient
	namespace string
}

// NewRedisStoreFromConfig creates a new RedisStore instance from the given configuration. For more
// details, see NewRedisStore.
func NewRedisStoreFromConfig(config RedisConfig) *RedisStore {
	return NewRedisStore(config.Address, config.Password, config.options()...)
}

// NewRedisStore creates a new RedisStore instance. Namespacing of keys and cluster mode can be
// enabled via the provided options.
func NewRedisStore(address, password string, options ...RedisOption) *RedisStore {
	opts := &redisOptions{}

	for _, option := range options {
		option.apply(opts)
	}

	var client redis.UniversalClient

	if opts.clusterMode {
		var tlsConfig *tls.Config
		if opts.enableTLS {
			tlsConfig = &tls.Config{
				MinVersion: tls.VersionTLS12,
			}
		}

		client = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:     []string{address},
			Password:  password,
			TLSConfig: tlsConfig,
		})
	} else {
		client = redis.NewClient(&redis.Options{
			Addr:     address,
			Password: password,
			DB:       0,
		})
	}

	// fails if we give it an unknown implementation of UniversalClient
	// which we can catch in unit tests and avoid changing the function signature
	if err := redisotel.InstrumentTracing(client); err != nil {
		panic(err)
	}

	// fails if we give it an unknown implementation of UniversalClient
	// which we can catch in unit tests and avoid changing the function signature
	if err := redisotel.InstrumentMetrics(client); err != nil {
		panic(err)
	}

	return &RedisStore{
		client:    client,
		namespace: opts.namespace,
	}
}

// namespaceKey prefixes the given key with a namespace if defined. If the namespace is undefined,
// the unmodified key is returned instead.
func (s *RedisStore) namespaceKey(key string) string {
	if s.namespace != "" {
		return fmt.Sprintf("%s%s%s", s.namespace, namespaceSeparator, key)
	}

	return key
}

// Set stores the given key-value pair for the specified duration in Redis. The expiry must be
// greater than 0, otherwise an InvalidExpiry error shall be returned. If the set operation fails, a
// SetError error shall be returned.
func (s *RedisStore) Set(ctx context.Context, key, value string, expiry time.Duration) error {
	if expiry == 0 {
		return NewInvalidExpiry()
	}

	key = s.namespaceKey(key)

	if err := s.client.Set(ctx, key, value, expiry).Err(); err != nil {
		return NewSetError(key, err)
	}

	return nil
}

// Get retrieves the value of the given key from Redis. If the key was not found due to expiration
// or non-existence, a KeyNotFoundError shall be returned. If the get operation failed for any other
// other reason, a GetError shall be returned instead.
func (s *RedisStore) Get(ctx context.Context, key string) (string, error) {
	key = s.namespaceKey(key)

	value, err := s.client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return "", NewKeyNotFound(key)
		}

		return "", NewGetError(key, err)
	}

	return value, nil
}

// redisOptions are used to configure the behavior of RedisStore.
type redisOptions struct {
	namespace   string
	clusterMode bool
	enableTLS   bool
}

type redisNamespace string

func (r redisNamespace) apply(options *redisOptions) {
	options.namespace = string(r)
}

// WithRedisNamespace defines the namespace to use for keys passed to Get and Set. If used, the
// given namespace shall be prepended to the keys, separated with a ":" character.
func WithRedisNamespace(namespace string) RedisOption {
	return redisNamespace(namespace)
}

type redisClusterMode bool

func (r redisClusterMode) apply(options *redisOptions) {
	options.clusterMode = bool(r)
}

// WithRedisClusterMode enables cluster mode for the Redis client.
func WithRedisClusterMode() RedisOption {
	return redisClusterMode(true)
}

type redisTLS bool

func (r redisTLS) apply(options *redisOptions) {
	options.enableTLS = bool(r)
}

// WithRedisTLS enables TLS for the Redis client. This is ignored unless WithRedisClusterMode is
// also used.
func WithRedisTLS() RedisOption {
	return redisTLS(true)
}
