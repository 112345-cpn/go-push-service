package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	// 永远返回 true 等于任何网站都能连你的推送服务，上线就是漏洞。
	// 这里只放行本机页面；压测工具这类非浏览器客户端不带 Origin，直接放。
	CheckOrigin: func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		if origin == "" {
			return true
		}
		u, err := url.Parse(origin)
		if err != nil {
			return false
		}
		return u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1"
	},
}

const (
	// 拾味的业务服务往这个频道 PUBLISH。改这个名字要两边一起改
	redisChannel = "shiwei:push"
	pongWait     = 60 * time.Second
	pingPeriod   = 25 * time.Second
	maxMsgSize   = 1 << 20 // 单条消息上限 1MB，防止客户端猛发大包把内存吃光
)

// 拾味的 Redis 映射在宿主机 6380，不是默认的 6379，所以不能用默认地址
func redisAddr() string {
	if v := os.Getenv("REDIS_ADDR"); v != "" {
		return v
	}
	return "127.0.0.1:6380"
}

type Server struct {
	hub *Hub
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	uid := r.URL.Query().Get("uid") // 从 ws://...?uid=12345 里取
	if uid == "" {
		http.Error(w, "缺少 uid", http.StatusBadRequest)
		return
	}

	// err 必须马上判断：Upgrade 失败时 conn 是 nil，
	// 后面任何 conn.SetXxx 都会空指针 panic。
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	c := newClient(uid, conn)
	s.hub.Add(c)
	defer s.hub.Remove(uid, c)
	fmt.Printf("用户 %s 上线，当前在线 %d 人\n", uid, s.hub.Count())

	go c.pingLoop() // 服务端主动发心跳，别指望浏览器主动发

	conn.SetReadLimit(maxMsgSize)
	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(pongWait))
	})
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			fmt.Printf("用户 %s 下线\n", uid)
			return
		}
	}
}

func (s *Server) listenRedis(ctx context.Context) {
	for {
		if !s.subscribeOnce(ctx) {
			return // ctx 取消了，别再重连
		}
		log.Println("Redis 订阅断开，3 秒后重连")
		select {
		case <-ctx.Done():
			return
		case <-time.After(3 * time.Second):
		}
	}
}

// subscribeOnce 连一次、订阅一次，阻塞到连接断开为止。
// 返回值 false 表示 ctx 被取消，调用方该退出。
//
// 原来这里连不上就直接 return，Redis 一重启订阅就永久没了，
// 但服务还活着、还能接连接——属于最难查的假活。
func (s *Server) subscribeOnce(ctx context.Context) bool {
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr()})
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Printf("连不上 Redis %s: %v", redisAddr(), err)
		return true
	}

	sub := rdb.Subscribe(ctx, redisChannel)
	defer sub.Close()

	fmt.Println("已连上 Redis，等待推送消息...")
	for {
		select {
		case <-ctx.Done():
			return false
		case msg, ok := <-sub.Channel():
			if !ok {
				return true
			}
			uid, body, ok := parsePush(msg.Payload)
			if !ok {
				continue
			}
			if s.hub.Push(uid, body) {
				fmt.Printf("已推送给%s:%s\n", uid, body)
			} else {
				fmt.Printf("用户%s不在线，消息丢弃\n", uid)
			}
		}
	}
}

// parsePush 把 "uid:json" 切成接收者和消息体。
// 只切第一个冒号，JSON 内部的冒号原样保留。
// 拼接格式由 Java 侧 PushService 决定，两边改动要同步。
func parsePush(payload string) (uid string, msg string, ok bool) {
	parts := strings.SplitN(payload, ":", 2)
	if len(parts) != 2 || parts[0] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func main() {
	s := &Server{hub: NewHub()}
	go s.listenRedis(context.Background()) // 这行之前漏了，没它 Redis 永远收不到消息
	http.HandleFunc("/ws", s.handleWS)
	fmt.Println("服务已启动: ws://localhost:8080/ws")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
