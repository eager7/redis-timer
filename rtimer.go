package redis_timer

import (
	"fmt"
	"github.com/go-redis/redis"
	"time"
)

type Handler func() error

type RTimer struct {
	red     *redis.Client  //redis client
	db      int            //redis分区
	opt     *redis.Options //设置，用于重置连接
	sub     *redis.PubSub  //订阅通道
	channel chan string    //用户层channel
	err     chan error     //用户层错误码channel
}

func Initialize(addr, pass string, db int, timeOut ...time.Duration) *RTimer {
	opt := &redis.Options{
		Addr:        addr,
		Password:    pass,
		DB:          db,
		PoolSize:    10,
		PoolTimeout: 30 * time.Second,
	}
	switch len(timeOut) {
	case 3:
		fmt.Println("init write timeout:", timeOut[2])
		opt.WriteTimeout = timeOut[2]
		fallthrough
	case 2:
		fmt.Println("init read timeout:", timeOut[1])
		opt.ReadTimeout = timeOut[1]
		fallthrough
	case 1:
		fmt.Println("init dial timeout:", timeOut[0])
		opt.DialTimeout = timeOut[0]
	}
	red := redis.NewClient(opt)
	sub := red.Subscribe(fmt.Sprintf("__keyevent@%d__:expired", db)) //订阅分区过期事件

	r := &RTimer{red: red, sub: sub, channel: make(chan string, 10), err: make(chan error)}
	go r.TimerEvent()
	return r
}

func (r *RTimer) Reset() {
	red := redis.NewClient(r.opt)
	sub := red.Subscribe(fmt.Sprintf("__keyevent@%d__:expired", r.db)) //订阅分区过期事件
	r.red, r.sub = red, sub
}

func (r *RTimer) AddTimer(interval time.Duration, handler Handler) {

}

func (r *RTimer) TimerEvent() {
	for {
		iFace, err := r.sub.Receive()
		if err != nil {
			fmt.Println("sub receiver err:", err)
			r.Reset() //重置连接
		}
		switch msg := iFace.(type) {
		case *redis.Subscription:
			fmt.Println("start sub:", msg.Channel)
		case *redis.Message:
			r.channel <- msg.Payload
		}
	}
}
