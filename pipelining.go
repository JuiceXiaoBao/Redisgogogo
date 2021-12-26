package Redisgogogo

import (
	"context"
	"errors"
	"sync"

	"github.com/garyburd/redigo/redis"
)

// Pipelining 提供了一些流水线的一些方法, 由NewPipelining函数创建
type Pipelining struct {
	conn    redis.Conn
	mu      sync.Mutex
	isClose bool
}

// NewPipelining 函数创建一个Pipelining， 参数ctx用于trace系统
func (r *Redis) NewPipelining(ctx context.Context) (*Pipelining, error) {
	p := &Pipelining{}
	client := r.pool.Get()
	err := client.Err()
	if err != nil {
		LOGGER.Errorf("NewPipelining error: %v", err)
		return nil, err
	}
	p.conn = client
	p.mu = sync.Mutex{}
	return p, nil
}

// Send 将命令发送到缓冲区中
func (p *Pipelining) Send(cmd string, args ...interface{}) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.isClose {
		return errors.New("Pipelining closed")
	}
	return p.conn.Send(cmd, args...)
}

// Flush 刷新缓冲区，将命令发送到redis服务器
func (p *Pipelining) Flush() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.isClose {
		return errors.New("Pipelining closed")
	}
	return p.conn.Flush()
}

// Receive 负责处理接收到的消息内容， 注意：receive一次只从结果中拿出一个send的命令进行处理
func (p *Pipelining) Receive() (reply interface{}, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.isClose {
		return nil, errors.New("Pipelining closed")
	}
	return p.conn.Receive()
}

// Close 关闭连接
func (p *Pipelining) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.isClose = true
	return p.conn.Close()
}
