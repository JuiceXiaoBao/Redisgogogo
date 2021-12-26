package Redisgogogo

import "fmt"

// NewRedis函数的参数，用于初始化Redis结构体
type RedisConfig struct {
	// 服务名,上报服务信息的标识
	ServerName string `json:"server_name"`

	// Redis服务器的host和port "localhost:6379"
	Addr string `json:"addr"`

	//连接到redis服务器的密码
	Password string `json:"password"`

	// 在连接池中可以存在的最大空闲连接数
	MaxIdle int `json:"max_idle"`

	// 在此期间保持空闲状态后关闭连接。如果该值为零，则不关闭空闲连接。应用程序应将超时设置为小于服务器超时的值
	IdleTimeout int `json:"idle_timeout"`

	// 池在给定时间分配的最大连接数。当为零时，池中的连接数没有限制。
	MaxActive int `json:"max_active"`

	// 连接超时
	ConnectTimeout int `json:"connect_timeout"`

	ReadTimeout int `json:"read_timeout"`

	WriteTimeout int `json:"write_timeout"`

	// 具体哪一个数据库
	Database int `json:"database"`

	// 慢日志打印
	SlowTime int `json:"slow_time"`

	// 内部重试次数
	Retry int `json:"retry"`
}

func (o *RedisConfig) init() error {
	if o.ServerName == "" {
		return fmt.Errorf("redis:ServerName not allowed empty string")
	}
	if o.Addr == "" {
		return fmt.Errorf("redis:Addr not allowed empty string")
	}
	if o.Database < 0 {
		return fmt.Errorf("redis:Database less than zero")
	}
	if o.MaxIdle < 0 {
		o.MaxIdle = 100
	}
	if o.MaxIdle < 0 {
		o.MaxIdle = 100
	}
	if o.IdleTimeout < 0 {
		o.IdleTimeout = 100
	}
	if o.ReadTimeout < 0 {
		o.ReadTimeout = 50
	}
	if o.WriteTimeout < 0 {
		o.WriteTimeout = 50
	}
	if o.ConnectTimeout < 0 {
		o.ConnectTimeout = 300
	}
	if o.SlowTime <= 0 {
		o.SlowTime = 100
	}
	if o.Retry < 0 {
		o.Retry = 0
	}
	return nil
}
