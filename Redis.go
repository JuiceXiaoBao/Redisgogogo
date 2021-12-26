package Redisgogogo

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

type Redis struct {
	*ctxRedis
}

var maps sync.Map

type ctxRedis struct {
	pool     *redis.Pool
	opts     *RedisConfig
	lastTime int64
	ctx      context.Context
}

// NewRedis 是redis客户端的初始化函数
func NewRedis(o *RedisConfig) (*Redis, error) {
	c := *o
	r, err := newCtxRedis(&c)
	if err != nil {
		return nil, err
	}
	maps.LoadOrStore(o.ServerName, r)
	return &Redis{r}, nil
}

// For 会返回一个具有context的redis客户端， 这个函数
// 是为了支持redis客户端接入trace系统而提供的，参数ctx
// 通常是从基础库中传递下来的。
func (r *Redis) For(c context.Context) *ctxRedis {
	return &ctxRedis{
		pool:     r.ctxRedis.pool,
		opts:     r.ctxRedis.opts,
		lastTime: r.ctxRedis.lastTime,
		ctx:      c,
	}
}

func newCtxRedis(o *RedisConfig) (*ctxRedis, error) {
	if err := o.init(); err != nil {
		return nil, err
	}
	var opts []redis.DialOption
	opts = append(opts, redis.DialConnectTimeout(time.Duration(o.ConnectTimeout)*time.Millisecond))
	opts = append(opts, redis.DialReadTimeout(time.Duration(o.ReadTimeout)*time.Millisecond))
	opts = append(opts, redis.DialReadTimeout(time.Duration(o.WriteTimeout)*time.Millisecond))
	if len(o.Password) != 0 {
		opts = append(opts, redis.DialPassword(o.Password))
	}
	opts = append(opts, redis.DialDatabase(o.Database))
	pool := redisinit(o.Addr, o.Password, o.MaxIdle, o.IdleTimeout, o.MaxActive, opts...)
	oo := *o
	return &ctxRedis{
		pool:     pool,
		opts:     &oo,
		lastTime: time.Now().UnixNano(),
		ctx:      context.TODO(),
	}, nil
}

func redisinit(server, password string, maxIdle, idleTimeout, maxActive int, options ...redis.DialOption) *redis.Pool {
	return &redis.Pool{
		Dial: func() (redis.Conn, error) {
			var c redis.Conn
			var err error
			protocol := "tcp"
			if strings.HasPrefix(server, "unix://") {
				server = strings.TrimLeft(server, "unix://")
				protocol = "unix"
			}
			c, err = redis.Dial(protocol, server, options...)
			if err != nil {
				return nil, err
			}
			if password != "" {
				if _, authErr := c.Do("AUTH", password); authErr != nil {
					c.Close()
					return nil, authErr
				}
			}
			return c, nil
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
		MaxIdle:     maxIdle,
		MaxActive:   maxActive,
		IdleTimeout: time.Duration(idleTimeout) * time.Second,
	}
}

