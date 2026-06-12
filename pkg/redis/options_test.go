package redis

import (
	"crypto/tls"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestNewAsynqOptions(t *testing.T) {
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}

	opt := &redis.Options{
		Network:      "tcp",
		Addr:         "localhost:6379",
		Username:     "user",
		Password:     "secret",
		DB:           3,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  6 * time.Second,
		WriteTimeout: 7 * time.Second,
		PoolSize:     11,
		TLSConfig:    tlsCfg,
	}

	got := NewAsynqOptions(opt)

	assert.Equal(t, "tcp", got.Network)
	assert.Equal(t, "localhost:6379", got.Addr)
	assert.Equal(t, "user", got.Username)
	assert.Equal(t, "secret", got.Password)
	assert.Equal(t, 3, got.DB)
	assert.Equal(t, 5*time.Second, got.DialTimeout)
	assert.Equal(t, 6*time.Second, got.ReadTimeout)
	assert.Equal(t, 7*time.Second, got.WriteTimeout)
	assert.Equal(t, 11, got.PoolSize)
	assert.Same(t, tlsCfg, got.TLSConfig)
}

func TestNewAsynqOptions_ZeroValues(t *testing.T) {
	got := NewAsynqOptions(&redis.Options{})

	assert.Empty(t, got.Addr)
	assert.Empty(t, got.Username)
	assert.Empty(t, got.Password)
	assert.Equal(t, 0, got.DB)
	assert.Nil(t, got.TLSConfig)
}
