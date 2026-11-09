package main

import (
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// client 包一层，是为了给每个连接配一把写锁。
// gorilla/websocket 规定：同一个连接同一时刻只能有一个 goroutine 在写，
// 否则会 panic "concurrent write to websocket connection"。
// 心跳 goroutine 和 Push 都会写，所以必须串起来。
type client struct {
	uid  string
	conn *websocket.Conn
	mu   sync.Mutex
}

func newClient(uid string, conn *websocket.Conn) *client {
	return &client{uid: uid, conn: conn}
}

func (c *client) write(mt int, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return c.conn.WriteMessage(mt, data)
}

// pingLoop 每 25 秒发一个 ping。浏览器收到 ping 会自动回 pong，
// 只要 pong 回来了，就说明这条连接还活着，把读超时往后推。
func (c *client) pingLoop() {
	t := time.NewTicker(pingPeriod)
	defer t.Stop()
	for range t.C {
		if err := c.write(websocket.PingMessage, nil); err != nil {
			return
		}
	}
}

type Hub struct {
	mu    sync.RWMutex
	conns map[string]*client
}

func NewHub() *Hub {
	return &Hub{conns: make(map[string]*client)}
}
func (h *Hub) Add(c *client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.conns[c.uid] = c
}

// Remove 只有"表里存的确实还是这个连接"时才删。
// 否则同一用户重连时（旧连接还没断、新连接已经 Add 覆盖），
// 旧连接收尾会把新连接误删掉，这个用户就再也收不到消息了。
func (h *Hub) Remove(uid string, c *client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if cur, ok := h.conns[uid]; ok && cur == c {
		delete(h.conns, uid)
	}
}
func (h *Hub) Push(uid string, msg string) bool {
	h.mu.RLock()
	c, ok := h.conns[uid]
	h.mu.RUnlock()
	if !ok {
		return false
	}
	if err := c.write(websocket.TextMessage, []byte(msg)); err != nil {
		// 写不出去说明这条连接已经坏了，直接踢出表，
		// 不然它会一直占着位置，之后每次 Push 都白试一遍。
		h.Remove(uid, c)
		return false
	}
	return true
}
func (h *Hub) Count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.conns)
}
