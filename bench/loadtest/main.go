// 压测工具：先建 N 条 WebSocket 连接，再通过 Redis 发 M 条消息，量端到端延迟。
// 用法示例：
//   go run ./bench/loadtest -conns 500 -msgs 1000
//   go run ./bench/loadtest -conns 1000 -msgs 2000 -hold 6s
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

// procMem 用 tasklist 读指定进程的物理内存，返回 KB。
// 只在 Windows 上可用，取不到就返回 -1，不阻断压测。
func procMem(exe string) int64 {
	out, err := exec.Command("tasklist", "/FI", "IMAGENAME eq "+exe, "/FO", "CSV", "/NH").Output()
	if err != nil {
		return -1
	}
	line := strings.TrimSpace(string(out))
	if !strings.HasPrefix(line, "\"") {
		return -1
	}
	f := strings.Split(line, "\",\"")
	if len(f) < 5 {
		return -1
	}
	s := strings.ReplaceAll(strings.Trim(f[4], "\" "), ",", "")
	s = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(s), "K"))
	kb, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return -1
	}
	return kb
}

func mb(kb int64) string {
	if kb < 0 {
		return "取不到"
	}
	return fmt.Sprintf("%.1f MB", float64(kb)/1024)
}

var (
	addr      = flag.String("addr", "127.0.0.1:8080", "服务地址")
	conns     = flag.Int("conns", 500, "并发连接数")
	batch     = flag.Int("batch", 50, "建连并发度")
	msgs      = flag.Int("msgs", 1000, "推送消息条数")
	pubConc   = flag.Int("pub", 20, "发布消息的并发度")
	redisAddr = flag.String("redis", "127.0.0.1:6379", "Redis 地址")
	channel   = flag.String("ch", "seckill:result", "订阅频道")
	hold      = flag.Duration("hold", 0, "建连完成后停顿多久（用来从外部量内存）")
	waitMsg   = flag.Duration("wait", 30*time.Second, "等待消息收齐的最长时间")
	procName  = flag.String("proc", "gopush.exe", "被测服务进程名（Windows，用来量内存）")
)

type link struct {
	uid  string
	conn *websocket.Conn
}

func main() {
	flag.Parse()
	n, m := *conns, *msgs

	// ---------- 阶段一：建连 ----------
	fmt.Printf("=== 阶段一：建立 %d 条连接（并发度 %d）===\n", n, *batch)
	memBefore := procMem(*procName)
	fmt.Printf("建连前服务内存：%s\n", mb(memBefore))
	links := make([]*link, n)
	dialCost := make([]time.Duration, n)
	var okN, failN int64
	var dialErr string
	var mu sync.Mutex

	start := time.Now()
	var wg sync.WaitGroup
	sem := make(chan struct{}, *batch)
	for i := 0; i < n; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			uid := "u" + strconv.Itoa(i)
			t := time.Now()
			c, _, err := websocket.DefaultDialer.Dial("ws://"+*addr+"/ws?uid="+uid, nil)
			dialCost[i] = time.Since(t)
			if err != nil {
				atomic.AddInt64(&failN, 1)
				mu.Lock()
				if dialErr == "" {
					dialErr = err.Error()
				}
				mu.Unlock()
				return
			}
			atomic.AddInt64(&okN, 1)
			links[i] = &link{uid: uid, conn: c}
		}(i)
	}
	wg.Wait()
	dialDur := time.Since(start)

	fmt.Printf("成功 %d / 失败 %d，总耗时 %v，建连速率 %.0f 条/秒\n",
		okN, failN, dialDur.Round(time.Millisecond), float64(okN)/dialDur.Seconds())
	if failN > 0 && dialErr != "" {
		fmt.Printf("首个错误：%s\n", dialErr)
	}
	okDial := make([]int64, 0, okN)
	for i := 0; i < n; i++ {
		if links[i] != nil {
			okDial = append(okDial, int64(dialCost[i]))
		}
	}
	fmt.Printf("建连耗时 P50=%v P95=%v P99=%v 最大=%v\n",
		ms(pct(okDial, 0.50)), ms(pct(okDial, 0.95)), ms(pct(okDial, 0.99)), ms(maxOf(okDial)))

	memAfter := procMem(*procName)
	fmt.Printf("连接全部在线后内存：%s\n", mb(memAfter))
	if memBefore > 0 && memAfter > 0 {
		delta := memAfter - memBefore
		fmt.Printf("每条连接平均占用：%.0f KB\n", float64(delta)/float64(okN))
	}

	if okN == 0 {
		fmt.Println("一条都没连上，压测结束")
		os.Exit(1)
	}

	if *hold > 0 {
		fmt.Printf(">>> 保持 %d 条连接不动，静置 %v（此刻可量内存）\n", okN, *hold)
		time.Sleep(*hold)
	}
	// ---------- 阶段二：起读协程收延迟 ----------
	lat := make([]int64, m) // 下标 = 消息序号，值 = 纳秒延迟
	var got, bad int64
	done := make(chan struct{})

	for i := 0; i < n; i++ {
		if links[i] == nil {
			continue
		}
		go func(c *websocket.Conn) {
			for {
				_, data, err := c.ReadMessage()
				if err != nil {
					return
				}
				// 服务端把 "uid:seq:纳秒时间戳" 拆开后，只把 "seq:纳秒时间戳" 推下来
				s := strings.SplitN(string(data), ":", 2)
				if len(s) != 2 {
					atomic.AddInt64(&bad, 1)
					continue
				}
				seq, e1 := strconv.Atoi(s[0])
				ts, e2 := strconv.ParseInt(s[1], 10, 64)
				if e1 != nil || e2 != nil || seq < 0 || seq >= m || ts == 0 {
					atomic.AddInt64(&bad, 1)
					continue
				}
				lat[seq] = time.Now().UnixNano() - ts
				atomic.AddInt64(&got, 1)
			}
		}(links[i].conn)
	}

	// ---------- 阶段三：并发发布 ----------
	fmt.Printf("=== 阶段二：发布 %d 条消息（并发 %d）===\n", m, *pubConc)
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	defer rdb.Close()

	var wg2 sync.WaitGroup
	psem := make(chan struct{}, *pubConc)
	pubStart := time.Now()
	for i := 0; i < m; i++ {
		wg2.Add(1)
		psem <- struct{}{}
		go func(i int) {
			defer wg2.Done()
			defer func() { <-psem }()
			uid := "u" + strconv.Itoa(i%n)
			payload := fmt.Sprintf("%s:%d:%d", uid, i, time.Now().UnixNano())
			rdb.Publish(ctx, *channel, payload)
		}(i)
	}
	wg2.Wait()
	pubDur := time.Since(pubStart)
	fmt.Printf("发布完成，耗时 %v，发布吞吐 %.0f 条/秒\n",
		pubDur.Round(time.Millisecond), float64(m)/pubDur.Seconds())

	// ---------- 阶段四：等收齐 ----------
	deadline := time.After(*waitMsg)
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
loop:
	for {
		select {
		case <-deadline:
			break loop
		case <-tick.C:
			if atomic.LoadInt64(&got) >= int64(m) {
				break loop
			}
		}
	}
	close(done)

	recv := atomic.LoadInt64(&got)
	cost := time.Since(pubStart)

	fmt.Println("\n========== 压测结果 ==========")
	fmt.Printf("连接数            : %d 条（失败 %d）\n", okN, failN)
	fmt.Printf("建连吞吐          : %.0f 条/秒，总耗时 %v\n", float64(okN)/dialDur.Seconds(), dialDur.Round(time.Millisecond))
	fmt.Printf("建连延迟          : P50=%v P95=%v P99=%v\n",
		ms(pct(okDial, 0.50)), ms(pct(okDial, 0.95)), ms(pct(okDial, 0.99)))
	fmt.Printf("服务内存          : 空载 %s -> %d 连接 %s\n", mb(memBefore), okN, mb(memAfter))
	if memBefore > 0 && memAfter > 0 && okN > 0 {
		fmt.Printf("单连接内存        : 约 %.0f KB\n", float64(memAfter-memBefore)/float64(okN))
	}
	fmt.Printf("消息收发          : 发出 %d 条，收到 %d 条，丢失 %d 条\n", m, recv, int64(m)-recv)
	if bad > 0 {
		fmt.Printf("格式异常          : %d 条（说明有客户端收到不该收的消息）\n", bad)
	}
	if recv > 0 {
		valid := make([]int64, 0, recv)
		for _, v := range lat {
			if v > 0 {
				valid = append(valid, v)
			}
		}
		fmt.Printf("端到端延迟        : P50=%v P95=%v P99=%v 最大=%v\n",
			ms(pct(valid, 0.50)), ms(pct(valid, 0.95)), ms(pct(valid, 0.99)), ms(maxOf(valid)))
		fmt.Printf("从发布到全部收齐  : %v，端到端吞吐 %.0f 条/秒\n", cost.Round(time.Millisecond), float64(recv)/cost.Seconds())
	}
	fmt.Println("==============================")
}

func pct(v []int64, p float64) int64 {
	if len(v) == 0 {
		return 0
	}
	s := make([]int64, len(v))
	copy(s, v)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	i := int(float64(len(s)-1) * p)
	return s[i]
}

func maxOf(v []int64) int64 {
	if len(v) == 0 {
		return 0
	}
	m := v[0]
	for _, x := range v {
		if x > m {
			m = x
		}
	}
	return m
}

func ms(ns int64) time.Duration { return time.Duration(ns) }
