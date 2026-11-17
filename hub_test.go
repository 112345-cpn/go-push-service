package main

import "testing"

// 同一用户重连：新连接注册后，旧连接收尾不能把它删掉。
// 这是修复前必现的 bug —— 无脑 delete(uid) 会把新连接误删。
func TestRemoveOnlyDeletesItsOwnConnection(t *testing.T) {
	h := NewHub()
	oldConn := &client{uid: "u1"}
	newConn := &client{uid: "u1"}

	h.Add(oldConn)
	h.Add(newConn) // 重连，覆盖表里的指针

	h.Remove("u1", oldConn) // 旧连接收尾

	if h.Count() != 1 {
		t.Fatalf("新连接被误删了：Count=%d，期望 1", h.Count())
	}
	h.mu.RLock()
	got := h.conns["u1"]
	h.mu.RUnlock()
	if got != newConn {
		t.Fatal("表里存的不是新连接")
	}
}

// 正常断开：表里就是自己，应该删掉。
func TestRemoveNormalCase(t *testing.T) {
	h := NewHub()
	c := &client{uid: "u2"}
	h.Add(c)
	h.Remove("u2", c)
	if h.Count() != 0 {
		t.Fatalf("正常断开没删掉：Count=%d，期望 0", h.Count())
	}
}

// 重复建连不会让计数虚高。
func TestAddSameUIDKeepsCountOne(t *testing.T) {
	h := NewHub()
	for i := 0; i < 5; i++ {
		h.Add(&client{uid: "u3"})
	}
	if h.Count() != 1 {
		t.Fatalf("同一用户连 5 次，Count=%d，期望 1", h.Count())
	}
}

// 表里没有这个人时，Push 返回 false 而不是崩。
func TestPushUnknownUID(t *testing.T) {
	h := NewHub()
	if h.Push("nobody", "hi") {
		t.Fatal("推给不存在的用户却返回 true")
	}
}

// 拾味 Java 侧拼的是 userId + ":" + JSON，JSON 里自己带冒号。
// 只切第一个冒号才不会把 body 切碎。
func TestParsePushKeepsJSONIntact(t *testing.T) {
	payload := `7:{"type":"SECKILL_RESULT","data":{"status":"SUCCESS","orderId":"12"}}`
	uid, msg, ok := parsePush(payload)
	if !ok {
		t.Fatal("正常格式被判定为非法")
	}
	if uid != "7" {
		t.Fatalf("uid=%q，期望 7", uid)
	}
	want := `{"type":"SECKILL_RESULT","data":{"status":"SUCCESS","orderId":"12"}}`
	if msg != want {
		t.Fatalf("body 被切坏了：%q", msg)
	}
}

func TestParsePushInvalid(t *testing.T) {
	cases := []string{"", "no-colon", ":只有冒号没uid"}
	for _, c := range cases {
		if _, _, ok := parsePush(c); ok {
			t.Fatalf("%q 应该被判为非法", c)
		}
	}
}
