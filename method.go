package Redisgogogo

import (
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	opentracinglog "github.com/opentracing/opentracing-go/log"
)

func (r *Redis) do(cmd string, f func(interface{}, error) (interface{}, error), args ...interface{}) (reply interface{}, err error) {
	var (
		stCode   = redisSuccess
		count    = 0
		//now      = time.Now()
		address  = r.opts.Addr
		firstArg interface{}
	)

	//cmdCase := strings.ToUpper(cmd)
	if len(args) >= 1 {
		firstArg = args[0]
	}
	myCtx := r.ctx
	span, _ := opentracing.StartSpanFromContext(myCtx, fmt.Sprintf("REDIS Client Cmd %s", cmd))

	traceid := strings.SplitN(fmt.Sprintf("%s", span.Context()), ":", 2)[0]

	ext.SpanKindRPCClient.Set(span)
	ext.PeerAddress.Set(span, address)
	ext.PeerService.Set(span, r.opts.ServerName)
	ext.DBType.Set(span, "codis/redis")
	ext.Component.Set(span, "golang/redis-client")
	span.SetTag("slowtime", r.opts.SlowTime)

	defer func() {
		atomic.StoreInt64(&r.lastTime, time.Now().UnixNano())
		if err != nil {
			ext.Error.Set(span, true)
		}
		span.Finish()
	}()

retry1:
	client := r.pool.Get()
	defer client.Close()

	reply, err = client.Do(cmd, args...)
	if f != nil {
		reply, err = f(reply, err)
	}
	if err == redis.ErrNil {
		err = nil
	}
	if _, ok := err.(redis.Error); err != nil && !ok {
		var rterr error
		switch err {
		case redis.ErrPoolExhausted:
			stCode = redisConnExhausted
			rterr = ErrConnExhausted
		default:
			if strings.Contains(err.Error(), "timeout") {
				stCode = redisTimeout
				rterr = ErrTimeout
			} else {
				stCode = redisError
				rterr = err
			}
		}
		if r.opts.Retry > 0 && count < r.opts.Retry {
			count++
			LOGGER.Infof("%d|redisclient|%s|retry-%d|%s|%s|%v|%v|%s", serverLocalPid, r.opts.ServerName, count,
				traceid, cmd, firstArg, err, address)
			span.LogFields(
				opentracinglog.Int("retry", count),
				opentracinglog.String("cmd", cmd),
				opentracinglog.String("cause", err.Error()),
			)
			time.Sleep(time.Millisecond * r.randomDuration(10))
			goto retry1
		}
		return nil, rterr
	}

	LOGGER.Infof("%d|redisclient|%s|retry-%d|%s|%s|%v|%v|%s|%d", serverLocalPid, r.opts.ServerName, count,
		traceid, cmd, firstArg, err, address,stCode)
	return
}

func (r *ctxRedis) randomDuration(n int64) time.Duration {
	s := rand.NewSource(r.lastTime)
	return time.Duration(rand.New(s).Int63n(n) + 1)
}
