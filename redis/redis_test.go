// (C) Copyright 2021-2024 Hewlett Packard Enterprise Development LP

package cache_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/hpe-cds/go-gadgets/x/cache"
	mockcache "github.com/hpe-cds/go-gadgets/x/cache/mocks"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	mockAddress   = "127.0.0.1:6379"
	mockPassword  = "password"
	mockNamespace = "example"
	mockKey       = "key"
	mockValue     = "value"
)

func Test_NewRedisStoreFromConfig(t *testing.T) {
	testCases := []struct {
		name       string
		config     cache.RedisConfig
		clientType any
		namespace  string
		withTLS    bool
	}{
		{
			name: "no options",
			config: cache.RedisConfig{
				Address:  mockAddress,
				Password: mockPassword,
			},
			clientType: &redis.ClusterClient{},
			namespace:  "",
		},
		{
			name: "disable tls",
			config: cache.RedisConfig{
				Address:    mockAddress,
				Password:   mockPassword,
				DisableTLS: true,
			},
			clientType: &redis.ClusterClient{},
			namespace:  "",
		},
		{
			name: "disable cluster mode",
			config: cache.RedisConfig{
				Address:            mockAddress,
				Password:           mockPassword,
				DisableClusterMode: true,
			},
			clientType: &redis.Client{},
			namespace:  "",
		},
		{
			name: "with namespace",
			config: cache.RedisConfig{
				Address:   mockAddress,
				Password:  mockPassword,
				Namespace: mockNamespace,
			},
			clientType: &redis.ClusterClient{},
			namespace:  mockNamespace,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// no arrange necessary

			// act
			store := cache.NewRedisStoreFromConfig(tc.config)

			// assert
			require.NotNil(t, store)
			assert.IsType(t, tc.clientType, store.Client())
			assert.IsType(t, tc.namespace, store.Namespace())
		})
	}
}

func Test_NewRedisStore(t *testing.T) {
	testCases := []struct {
		name       string
		options    []cache.RedisOption
		clientType any
		namespace  string
	}{
		{
			name:       "no options",
			options:    []cache.RedisOption{},
			clientType: &redis.Client{},
			namespace:  "",
		},
		{
			name:       "with cluster mode",
			options:    []cache.RedisOption{cache.WithRedisClusterMode()},
			clientType: &redis.ClusterClient{},
			namespace:  "",
		},
		{
			name:       "with namespace",
			options:    []cache.RedisOption{cache.WithRedisNamespace(mockNamespace)},
			clientType: &redis.Client{},
			namespace:  mockNamespace,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// no arrange necessary

			// act
			store := cache.NewRedisStore(mockAddress, mockPassword, tc.options...)

			// assert
			require.IsType(t, &cache.RedisStore{}, store)
			assert.IsType(t, tc.clientType, store.Client())
			assert.Equal(t, tc.namespace, store.Namespace())
		})
	}
}

func Test_RedisStore_Set_Happy(t *testing.T) {
	testCases := []struct {
		name        string
		namespace   string
		expectedKey string
	}{
		{
			name:        "no namespace",
			namespace:   "",
			expectedKey: mockKey,
		},
		{
			name:        "with namespace",
			namespace:   mockNamespace,
			expectedKey: fmt.Sprintf("%s:%s", mockNamespace, mockKey),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// arrange
			ctx := context.Background()

			client := mockcache.NewRedisClient(t)
			client.
				EXPECT().
				Set(ctx, tc.expectedKey, mockValue, time.Hour).
				Return(redis.NewStatusResult("", nil)).
				Once()

			store := cache.NewTestRedisStore(client, tc.namespace)

			// act
			err := store.Set(ctx, mockKey, mockValue, time.Hour)

			// assert
			assert.NoError(t, err)
		})
	}
}

func Test_RedisStore_Set_InvalidExpiry(t *testing.T) {
	// arrange
	ctx := context.Background()
	client := mockcache.NewRedisClient(t)
	store := cache.NewTestRedisStore(client, "")

	// act
	err := store.Set(ctx, mockKey, mockValue, 0)

	// assert
	require.EqualError(t, err, "invalid parameter expiry: must be non-zero")
	assert.IsType(t, (*cache.InvalidExpiry)(nil), err)
}

func Test_RedisStore_Set_SetError(t *testing.T) {
	// arrange
	ctx := context.Background()
	mockError := errors.New("mock error")

	client := mockcache.NewRedisClient(t)
	client.
		EXPECT().
		Set(ctx, mockKey, mockValue, time.Hour).
		Return(redis.NewStatusResult("", mockError)).
		Once()

	store := cache.NewTestRedisStore(client, "")

	// act
	err := store.Set(ctx, mockKey, mockValue, time.Hour)

	// assert
	require.EqualError(t, err, `an internal error occurred: could not set value under key "key": mock error`)
	require.IsType(t, (*cache.SetError)(nil), err)
}

func Test_RedisStore_Get_Happy(t *testing.T) {
	testCases := []struct {
		name        string
		namespace   string
		expectedKey string
	}{
		{
			name:        "no namespace",
			namespace:   "",
			expectedKey: mockKey,
		},
		{
			name:        "with namespace",
			namespace:   mockNamespace,
			expectedKey: fmt.Sprintf("%s:%s", mockNamespace, mockKey),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// arrange
			ctx := context.Background()

			client := mockcache.NewRedisClient(t)
			client.
				EXPECT().
				Get(ctx, tc.expectedKey).
				Return(redis.NewStringResult(mockValue, nil)).
				Once()

			store := cache.NewTestRedisStore(client, tc.namespace)

			// act
			value, err := store.Get(ctx, mockKey)

			// assert
			assert.Equal(t, mockValue, value)
			assert.NoError(t, err)
		})
	}
}

func Test_RedisStore_Get_KeyNotFound(t *testing.T) {
	// arrange
	ctx := context.Background()

	client := mockcache.NewRedisClient(t)
	client.
		EXPECT().
		Get(ctx, mockKey).
		Return(redis.NewStringResult("", redis.Nil)).
		Once()

	store := cache.NewTestRedisStore(client, "")

	// act
	value, err := store.Get(ctx, mockKey)

	// assert
	assert.Empty(t, value)
	require.EqualError(t, err, "cache value was not found with key: key")
	assert.IsType(t, (*cache.KeyNotFound)(nil), err)
}

func Test_RedisStore_Get_GetError(t *testing.T) {
	// arrange
	ctx := context.Background()
	mockError := errors.New("mock error")

	client := mockcache.NewRedisClient(t)
	client.
		EXPECT().
		Get(ctx, mockKey).
		Return(redis.NewStringResult("", mockError)).
		Once()

	store := cache.NewTestRedisStore(client, "")

	// act
	value, err := store.Get(ctx, mockKey)

	// assert
	assert.Empty(t, value)
	require.EqualError(t, err, `an internal error occurred: could not get value under key "key": mock error`)
	require.IsType(t, (*cache.GetError)(nil), err)
}
