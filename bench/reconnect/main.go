// 验证「同一用户重连」不会把自己的新连接误删掉。
// 场景：用户断网重连，旧连接还没断干净，新连接已经注册。
// 旧连接收尾时如果无脑 delete(uid)，会把新连接删掉，这个用户之后永久收不到消息。
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

const uid = "re1"

func dial(tag string) *websocket.Conn {
	c, _, err := websocket.DefaultDialer.Dial("ws://127.0.0.1:8080/ws?uid="+uid, nil)
	if err != nil {
		fmt.Printf("[%s] 连接失败: %v\n", tag, err)
		os.Exit(1)
	}
	fmt.Printf("[%s] 已连上\n", tag)
	return c
}

func main() {
	// 1. 旧连接
	old := dial("旧连接")
	time.Sleep(300 * time.Millisecond)

	// 2. 新连接（同一个 uid，覆盖表里的指针）
	fresh := dial("新连接")
	time.Sleep(300 * time.Millisecond)

	// 3. 旧连接断开，触发它的 defer Remove
	old.Close()
	fmt.Println("[旧连接] 已断开")
	time.Sleep(600 * time.Millisecond)

	// 4. 这时候推一条，应该落到"新连接"上
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer rdb.Close()
	n, err := rdb.Publish(ctx, "seckill:result", uid+":重连后应该收到这条").Result()
	fmt.Printf("publish 时订阅者数 = %d, err=%v\n", n, err)

	// 5. 新连接能不能收到
	fresh.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, msg, err := fresh.ReadMessage()
	if err != nil {
		fmt.Println("结果: 失败 —— 新连接没收到消息，说明被旧连接的收尾误删了")
		os.Exit(1)
	}
	fmt.Printf("结果: 成功 —— 新连接收到「%s」\n", string(msg))
}
