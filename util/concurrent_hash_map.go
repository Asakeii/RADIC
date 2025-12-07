package util

// 实现支持并发独写的map，基于Segment分段和读写锁的map；
// 通过哈希%Segment将所有kv数据存入Segment个map切片中，每个小map都单独拥有一把锁，显著减少锁竞争
// 每个 segment 是一个独立的小 map：map 桶更小，热数据更可能留在 CPU L1/L2 cache，Cache 命中率显著提高，查询延迟更稳定
// 降低 map 扩容成本，如果只有一个大 map：扩容是一次性全部 bucket 重建，开销巨大，分段是每个 map 单独扩容
// 并发度更高，提高吞吐量，小map可同时进行的写操作越多
// 哈希分布让数据均匀分散，避免热点 key：比如2个高频词（“the”, “中国”）也不会集中在一个小 map 里

import "sync"
import "github.com/leemcloughlin/gofarmhash"

// ConcurrentHashMap 支持并发独写的map
type ConcurrentHashMap struct {
	mps   []map[string]any
	seg   int
	locks []sync.RWMutex
	seed  uint32
}

// NewConcurrentHashMap 初始化concurrenthashmap ，seg内部包含几个小map，cap是大map的预估容量
func NewConcurrentHashMap(seg, cap int) *ConcurrentHashMap {
	mps := make([]map[string]any, seg)
	locks := make([]sync.RWMutex, seg)

	for i := 0; i < seg; i++ {
		mps[i] = make(map[string]any, cap/seg)
	}

	return &ConcurrentHashMap{
		mps:   mps,
		seg:   seg,
		locks: locks,
		seed:  0,
	}
}

// getSegIndex 调用farmhash获取小map分区号
func (m *ConcurrentHashMap) getSegIndex(key string, seed uint32) int {
	index := int(farmhash.Hash32WithSeed([]byte(key), seed))
	return index % m.seg
}

// Set concurrentHaspMap的写操作
func (m *ConcurrentHashMap) Set(key string, value any) {
	index := m.getSegIndex(key, m.seed)

	m.locks[index].Lock()
	defer m.locks[index].Unlock()

	m.mps[index][key] = value
}

func (m *ConcurrentHashMap) Get(key string) (any, bool) {
	index := m.getSegIndex(key, m.seed)

	m.locks[index].RLock()
	defer m.locks[index].RUnlock()

	value, exists := m.mps[index][key]

	return value, exists

}
