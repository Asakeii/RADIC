package index_service

import (
	"math/rand/v2"
	"sync/atomic"
)

type LoadBalancer interface {
	Take([]string) string
}

// RoundRobin 负载均衡算法——轮询法
type RoundRobin struct {
	acc int64
}

func (b *RoundRobin) Take(endpoints []string) string {
	if len(endpoints) == 0 {
		return ""
	}
	n := atomic.AddInt64(&b.acc, 1)
	index := int(n % int64(len(endpoints)))
	return endpoints[index]
}

// 负载均衡算法——随机法
type RandomSelect struct {
}

func (b *RandomSelect) Take(endpoints []string) string {
	if len(endpoints) == 0 {
		return ""
	}
	index := rand.IntN(len(endpoints))
	return endpoints[index]
}
