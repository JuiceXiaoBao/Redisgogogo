package Redisgogogo

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

var (
	serverLocalPid = os.Getpid()
	logFormat      = "2006/01/02 15:04:05"
)

var errNil = errors.New("redis nil")

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

func (r *ctxRedis) Lock(key string, expire time.Duration) (uuid string, err error) {
	buf := make([]byte, 16)
	_, _ = io.ReadFull(crand.Reader, buf)
	uuid = hex.EncodeToString(buf)
	ret, err := redis.String(r.Do("SET", key, uuid, "NX", "PX", expire.Nanoseconds()/1e6))
	if ret != "OK" {
		return "", errors.New("lock failed")
	}
	return
}

func (r *ctxRedis) Unlock(key string, uuid string) (err error) {
	script := `if redis.call('get',KEYS[1])==ARGV[1] then return redis.call('del',KEYS[1]) else return 0 end`
	ret, err := redis.Int(r.Do("EVAL", script, 1, key, uuid))
	if ret != 1 {
		return errors.New("unlock failed")
	}
	return
}

func (r *ctxRedis) RPop(key string) (res string, err error) {
	reply, err := r.do("RPOP", redisBytes, key)
	if err != nil {
		return "", err
	}
	return string(reply.([]byte)[:]), nil
}

func (r *ctxRedis) LPush(name string, fields ...interface{}) error {
	keys := []interface{}{name}
	keys = append(keys, fields...)
	_, err := r.do("LPUSH", nil, keys...)
	return err
}

func (r *ctxRedis) Send(name string, fields ...interface{}) error {
	keys := []interface{}{name}
	keys = append(keys, fields)
	_, err := r.do("RPUSH", nil, keys...)
	return err
}

// Set 返回两个参数,err不为空
func (r *ctxRedis) Set(key, value interface{}) (ret bool, err error) {
	var reply interface{}
	reply, err = r.do("SET", redisString, key, value)
	if err != nil {
		return
	}
	rsp := reply.(string)
	if rsp == "OK" {
		ret = true
	}
	return
}

func (r *ctxRedis) SetExSecond(key, value interface{}, dur int) (ret string, err error) {
	var reply interface{}
	reply, err = r.do("SET", redisString, key, value, "EX", dur)
	if err != nil {
		return
	}
	ret = reply.(string)
	return
}

func (r *ctxRedis) Get(key string) (ret []byte, err error) {
	var reply interface{}
	reply, err = r.do("GET", redisBytes, key)
	if err != nil {
		if err == redis.ErrNil {
			err = nil
			var tmp []byte
			ret = tmp
		}
		return
	}
	ret = reply.([]byte)
	return
}

func (r *ctxRedis) GetInt(key string) (ret int, err error) {
	var reply interface{}
	reply, err = r.do("GET", redisInt, key)
	if err != nil {
		return
	}
	ret = reply.(int)
	return
}

func (r *ctxRedis) MGet(keys ...interface{}) (ret [][]byte, err error) {
	var reply interface{}
	reply, err = r.do("MGET", redisByteSlices, keys...)
	if err != nil {
		return
	}
	ret = reply.([][]byte)
	return
}

func (r *ctxRedis) MSet(keys ...interface{}) (ret string, err error) {
	var reply interface{}
	reply, err = r.do("MSET", redisString, keys...)
	if err != nil {
		return
	}
	ret = reply.(string)
	return
}

func (r *ctxRedis) Del(args ...interface{}) (count int, err error) {
	var reply interface{}
	reply, err = r.do("Del", redisInt, args...)
	if err != nil {
		return
	}
	count = reply.(int)
	return
}

func (r *ctxRedis) Exists(key string) (res bool, err error) {
	var reply interface{}
	reply, err = r.do("Exists", redisBool, key)
	if err != nil {
		return
	}
	res = reply.(bool)
	return
}

func (r *ctxRedis) Expire(key string, expire time.Duration) error {
	_, err := r.do("EXPIRE", nil, key, int64(expire.Seconds()))
	if err != nil {
		return err
	}
	return nil
}

func (r *ctxRedis) HDel(key interface{}, fields ...interface{}) (res int, err error) {
	var reply interface{}
	keys := []interface{}{key}
	keys = append(keys, fields...)

	reply, err = r.do("HDEL", redisInt, keys...)
	if err != nil {
		return
	}
	res = reply.(int)
	return
}

func (r *ctxRedis) HSet(key, fieldk string, fieldv interface{}) (res int, err error) {
	var reply interface{}
	reply, err = r.do("HSET", redisInt, key, fieldk, fieldv)
	if err != nil {
		return
	}
	res = reply.(int)
	return
}

func (r *ctxRedis) HGet(key, field string) (res string, err error) {
	var reply interface{}
	reply, err = r.do("HGET", redisString, key, field)
	if err != nil {
		return
	}
	res = reply.(string)
	return
}

func (r *ctxRedis) HGetInt(key, field string) (res int, err error) {
	var reply interface{}
	reply, err = r.do("HGET", redisInt, key, field)
	if err != nil {
		return
	}
	res = reply.(int)
	return
}

func (r *ctxRedis) HMGet(key string, fields ...interface{}) (res []string, err error) {
	var reply interface{}
	keys := []interface{}{key}
	keys = append(keys, fields...)
	reply, err = r.do("HMGET", redisStrings, keys...)
	if err != nil {
		return
	}
	res = reply.([]string)
	return
}

func (r *ctxRedis) HMSet(key string, fields ...interface{}) (res string, err error) {
	var reply interface{}
	keys := []interface{}{key}
	keys = append(keys, fields...)
	reply, err = r.do("HMSET", redisString, keys...)
	if err != nil {
		return
	}
	res = reply.(string)
	return
}

func (r *ctxRedis) HGetAll(key string) (res map[string]string, err error) {
	var reply interface{}
	reply, err = r.do("HGETALL", redisStringMap, key)
	if err != nil {
		return
	}
	res = reply.(map[string]string)
	return
}

func (r *ctxRedis) HKeys(key string) (res []string, err error) {
	var reply interface{}
	reply, err = r.do("HKEYS", redisStrings, key)
	if err != nil {
		return
	}
	res = reply.([]string)
	return
}

func (r *ctxRedis) HIncrby(key, field string, incr int) (res int64, err error) {
	var reply interface{}
	reply, err = r.do("HINCRBY", redisInt64, key, field, incr)
	if err != nil {
		return
	}
	res = reply.(int64)
	return
}

func (r *ctxRedis) SAdd(key string, members ...interface{}) (res int, err error) {
	var reply interface{}
	keys := []interface{}{key}
	keys = append(keys, members...)
	reply, err = r.do("SADD", redisInt, keys...)
	if err != nil {
		return
	}
	res = reply.(int)
	return
}

func (r *ctxRedis) SRem(key string, members ...interface{}) (res int, err error) {
	var reply interface{}
	keys := []interface{}{key}
	keys = append(keys, members...)
	reply, err = r.do("SREM", redisInt, keys...)
	if err != nil {
		return
	}
	res = reply.(int)
	return
}

func (r *ctxRedis) SIsMember(key string, member string) (res bool, err error) {
	var reply interface{}
	reply, err = r.do("SISMEMBER", redisBool, key, member)
	if err != nil {
		return
	}
	res = reply.(bool)

	return
}

func (r *ctxRedis) SMembers(key string) (res []string, err error) {
	var reply interface{}
	reply, err = r.do("SMEMBERS", redisStrings, key)
	if err != nil {
		return
	}
	res = reply.([]string)
	return
}

func (r *ctxRedis) ZAdd(key string, args ...interface{}) (res int, err error) {
	var reply interface{}
	keys := []interface{}{key}
	keys = append(keys, args...)
	reply, err = r.do("ZADD", redisInt, keys...)
	if err != nil {
		return
	}
	res = reply.(int)
	return
}

func (r *ctxRedis) ZRange(key string, args ...interface{}) (res []string, err error) {
	var reply interface{}
	keys := []interface{}{key}
	keys = append(keys, args...)
	reply, err = r.do("ZRANGE", redisStrings, keys...)
	if err != nil {
		return
	}
	res = reply.([]string)
	return
}

func (r *ctxRedis) ZRangeInt(key string, start, stop int) (res []int, err error) {
	var reply interface{}
	reply, err = r.do("ZRANGE", redisInts, key, start, stop)
	if err != nil {
		return
	}
	res = reply.([]int)
	return
}

func (r *ctxRedis) ZRangeWithScore(key string, start, stop int) (res []string, err error) {
	var reply interface{}
	reply, err = r.do("ZRANGE", redisStrings, key, start, stop, "WITHSCORES")
	if err != nil {
		return
	}
	res = reply.([]string)
	return
}

func (r *ctxRedis) ZRevRangeWithScore(key string, start, stop int) (res []string, err error) {
	var reply interface{}
	reply, err = r.do("ZREVRANGE", redisStrings, key, start, stop, "WITHSCORES")
	if err != nil {
		return
	}
	res = reply.([]string)
	return
}

func (r *ctxRedis) ZCount(key string, min, max int) (res int, err error) {
	var reply interface{}
	reply, err = r.do("ZCOUNT", redisInt, key, min, max)
	if err != nil {
		return
	}
	res = reply.(int)
	return
}

func (r *ctxRedis) ZCard(key string) (res int, err error) {
	var reply interface{}
	reply, err = r.do("ZCARD", redisInt, key)
	if err != nil {
		return
	}
	res = reply.(int)
	return
}

func (r *ctxRedis) LLen(key string) (res int64, err error) {
	var reply interface{}
	reply, err = r.do("LLEN", redisInt64, key)
	if err != nil {
		return
	}
	res = reply.(int64)
	return
}

func (r *ctxRedis) Incrby(key string, incr int) (res int64, err error) {
	var reply interface{}
	reply, err = r.do("INCRBY", redisInt64, key, incr)
	if err != nil {
		return
	}
	res = reply.(int64)
	return
}

func (r *ctxRedis) ZIncrby(key string, incr int, member string) (res int, err error) {
	var reply interface{}
	reply, err = r.do("ZINCRBY", redisInt, key, incr, member)
	if err != nil {
		return
	}
	res = reply.(int)
	return
}

// Deprecated: ZRank function does not return errNil, please use ZRankIfExists instead.
// If the member not in the zset or key not exits, ZRank will returns 0, nil
func (r *ctxRedis) ZRank(key string, member string) (res int, err error) {
	var reply interface{}
	reply, err = r.do("ZRANK", redisInt, key, member)
	if err != nil {
		return
	}
	res = reply.(int)
	return
}

// ZRankIfExists equals ZRank but ZRankIfExists will returns errNil if key not exists or member not in sortedset
// you can use IsErrNil to catch error.
func (r *ctxRedis) ZRankIfExists(key string, member string) (int64, error) {
	var reply interface{}
	reply, err := r.do("ZRANK", nil, key, member)
	if err != nil {
		return 0, fmt.Errorf("ZRank2 %s: %w", key, err)
	}
	if reply == nil {
		return 0, errNil
	}
	reply, err = redisInt64(reply, err)
	if err != nil {
		return 0, err
	}
	return reply.(int64), nil
}

// If the members not in the zset or key not exits, ZRem will NOT return ErrNil
func (r *ctxRedis) ZRem(key string, members ...interface{}) (res int, err error) {
	var reply interface{}
	keys := []interface{}{key}
	keys = append(keys, members...)

	reply, err = r.do("ZREM", redisInt, keys...)
	if err != nil {
		return
	}
	res = reply.(int)
	return
}

func (r *ctxRedis) ZRemrangebyrank(key string, members ...interface{}) (res int, err error) {
	var reply interface{}
	keys := []interface{}{key}
	keys = append(keys, members...)

	reply, err = r.do("ZREMRANGEBYRANK", redisInt, keys...)
	if err != nil {
		return
	}
	res = reply.(int)
	return
}

func (r *ctxRedis) Subscribe(ctx context.Context, key string, maxSize int) (chan []byte, error) {
	ch := make(chan []byte, maxSize)

	if r.opts.ReadTimeout < 100 && r.opts.ReadTimeout > 0 {
		return nil, errors.New("Read timeout should be longer")
	}

	healthCheckPeriod := r.opts.ReadTimeout * (70 / 100) // 70%

	var offHealthCheck = (healthCheckPeriod == 0)
	done := make(chan error, 1)

	// While not a permanent error on the connection.
	go func() {
	start:
		client := r.pool.Get()
		psc := redis.PubSubConn{client}
		// Set up subscriptions
		err := psc.Subscribe(key)
		if err != nil {
			client.Close()
			close(ch)
			return
		}

		go func(c redis.PubSubConn) {
			if offHealthCheck {
				return
			}
			ticker := time.NewTicker(time.Duration(healthCheckPeriod * 10e5))
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					if err = c.Ping(""); err != nil {
						break
					}
				case <-ctx.Done():
					return
				case <-done:
					return
				}
			}
		}(psc)

		for client.Err() == nil {
			select {
			case <-ctx.Done():
				client.Close()
				return
			default:
				switch v := psc.ReceiveWithTimeout(time.Second * 0).(type) {
				case redis.Message:
					ch <- v.Data
				case redis.Subscription:
					LOGGER.Infof("Receive chan (%s) %s %d", v.Channel, v.Kind, v.Count)
				case error:
					LOGGER.Errorf("Receive error (%v), client will reconnect..", v)
					client.Close()
					if !offHealthCheck {
						done <- v
					}
					time.Sleep(time.Second / 10)
					goto start
				}
			}
		}
	}()

	return ch, nil
}

// Deprecated: ZScore function does not return errNil, please use ZScoreIfExists instead.
// If the member not in the zset or key not exits, ZScore will NOT return ErrNil
func (r *ctxRedis) ZScore(key, member string) (res float64, err error) {
	var reply interface{}
	reply, err = r.do("ZSCORE", redisFloat64, key, member)
	if err != nil {
		return
	}
	res = reply.(float64)
	return
}

// ZScoreIfExists will return errNil if the member not in the sortedset or key not exits,
// you can use IsErrNil to catch error.
func (r *ctxRedis) ZScoreIfExists(key, member string) (res float64, err error) {
	var reply interface{}
	reply, err = r.do("ZSCORE", nil, key, member)
	if err != nil {
		return
	}
	if reply == nil {
		return 0, errNil
	}
	reply, err = redisFloat64(reply, err)
	res = reply.(float64)
	return
}

func (r *ctxRedis) Zrevrange(key string, args ...interface{}) (res []string, err error) {
	var reply interface{}
	argss := []interface{}{key}
	argss = append(argss, args...)
	reply, err = r.do("ZREVRANGE", redisStrings, argss...)
	if err != nil {
		return
	}
	res = reply.([]string)
	return
}

func (r *ctxRedis) Zrevrangebyscore(key string, args ...interface{}) (res []string, err error) {
	var reply interface{}
	argss := []interface{}{key}
	argss = append(argss, args...)
	reply, err = r.do("ZREVRANGEBYSCORE", redisStrings, argss...)
	if err != nil {
		return
	}
	res = reply.([]string)
	return
}

func (r *ctxRedis) ZrevrangebyscoreInt(key string, args ...interface{}) (res []int, err error) {
	var reply interface{}
	argss := []interface{}{key}
	argss = append(argss, args...)
	reply, err = r.do("ZREVRANGEBYSCORE", redisInts, argss...)
	if err != nil {
		return
	}
	res = reply.([]int)
	return
}

func (r *ctxRedis) TryLock(key string, acquireTimeout, expireTimeout time.Duration) (uuid string, err error) {
	deadline := time.Now().Add(acquireTimeout)
	for {
		if time.Now().After(deadline) {
			return "", errors.New("lock timeout")
		}
		uuid, err = r.Lock(key, expireTimeout)
		if err != nil {
			time.Sleep(time.Millisecond)
		} else {
			return uuid, err
		}
	}
}