package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
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
	redisChannel = "seckill:result"
	pongWait     = 60 * time.Second
	pingPeriod   = 25 * time.Second
	maxMsgSize   = 1 << 20 // 单条消息上限 1MB，防止客户端猛发大包把内存吃光
)

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
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Printf("连不上 Redis，订阅没启动: %v", err)
		return
	}
	fmt.Println("已连上 Redis，等待秒杀结果...")
	sub := rdb.Subscribe(ctx, redisChannel)
	defer sub.Close()
	for msg := range sub.Channel() {
		parts := strings.SplitN(msg.Payload, ":", 2)
		if len(parts) != 2 {
			continue
		}
		uid, result := parts[0], parts[1]
		if ok := s.hub.Push(uid, result); ok {
			fmt.Printf("已推送给%s:%s\n", uid, result)
		} else {
			fmt.Printf("用户%s不在线，消息丢弃\n", uid)
		}
	}
}

func main() {
	s := &Server{hub: NewHub()}
	go s.listenRedis(context.Background()) // 这行之前漏了，没它 Redis 永远收不到消息
	http.HandleFunc("/ws", s.handleWS)
	fmt.Println("服务已启动: ws://localhost:8080/ws")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
