// Package redis provides Redis client utilities
package redis

import (
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

// NewAsynqRedisOptions converts Redis options to Asynq Redis options
func NewAsynqRedisOptions(opt *redis.Options) *asynq.RedisClientOpt {
	return &asynq.RedisClientOpt{
		Network:      opt.Network,
		Addr:         opt.Addr,
		Username:     opt.Username,
		Password:     opt.Password,
		DB:           opt.DB,
		DialTimeout:  opt.DialTimeout,
		ReadTimeout:  opt.ReadTimeout,
		WriteTimeout: opt.WriteTimeout,
		PoolSize:     opt.PoolSize,
		TLSConfig:    opt.TLSConfig,
	}
}
