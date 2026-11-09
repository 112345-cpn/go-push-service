package main

import (
	"fmt"
	"os"
	"time"

	"github.com/gorilla/websocket"
)

func main() {
	url := "ws://127.0.0.1:8080/ws?uid=1001"
	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		fmt.Println("连接失败:", err)
		os.Exit(1)
	}
	defer c.Close()
	fmt.Println("已连上", url)
	c.SetReadDeadline(time.Now().Add(6 * time.Second))
	_, msg, err := c.ReadMessage()
	if err != nil {
		fmt.Println("6 秒内没收到消息:", err)
		return
	}
	fmt.Println("收到:", string(msg))
}
