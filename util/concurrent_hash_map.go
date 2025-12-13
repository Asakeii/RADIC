package util

// 实现支持并发独写的map，基于Segment分段和读写锁的map；
// 通过哈希%Segment将所有kv数据存入Segment个map切片中，每个小map都单独拥有一把锁，显著减少锁竞争
// 每个 segment 是一个独立的小 map：map 桶更小，热数据更可能留在 CPU L1/L2 cache，Cache 命中率显著提高，查询延迟更稳定
// 降低 map 扩容成本，如果只有一个大 map：扩容是一次性全部 bucket 重建，开销巨大，分段是每个 map 单独扩容
// 并发度更高，提高吞吐量，小map可同时进行的写操作越多
// 哈希分布让数据均匀分散，避免热点 key：比如2个高频词（“the”, “中国”）也不会集中在一个小 map 里

import (
	"github.com/leemcloughlin/gofarmhash"
	"sync"
)

// ConcurrentHashMap 支持并发独写的map
type ConcurrentHashMap struct {
	mps   []map[string]any // map切片，每个map都是一个分段的map
	seg   int              // 要分的段数
	locks []sync.RWMutex   // 读写锁，为每一段都配锁，降低锁竞争
	seed  uint32           // 哈希种子
}

// MapEntry kv存储结构体
type MapEntry struct {
	key   string
	value any
}

// MapIterator 提供MapEntry的Next迭代接口		迭代器模式
type MapIterator interface {
	Next() *MapEntry
}

// ConcurrentHashMapIterator
type ConcurrentHashMapIterator struct {
	cm       *ConcurrentHashMap // 底层用于存储的[]map的指针
	keys     [][]string         // 将map里的key都存下来，固定顺序
	rowIndex int                // 行索引
	colIndex int                // 列索引
}

// CreateIterator 迭代器的初始化
func (m *ConcurrentHashMap) CreateIterator() *ConcurrentHashMapIterator {
	keys := make([][]string, 0, m.seg)

	for k, mp := range m.mps {
		var row []string

		// 确保并发安全的遍历
		m.locks[k].RLock()

		row = make([]string, 0, len(mp))
		for key := range mp {
			row = append(row, key)
		}

		m.locks[k].RUnlock()

		keys = append(keys, row)
	}
	return &ConcurrentHashMapIterator{
		cm:       m,
		keys:     keys,
		rowIndex: 0,
		colIndex: 0,
	}
}

// Next 用于自定义map的迭代
func (iter *ConcurrentHashMapIterator) Next() *MapEntry {
	if iter.rowIndex >= len(iter.keys) {
		return nil
	}
	row := iter.keys[iter.rowIndex]

	// 注意数组越界的情况
	if len(row) == 0 {
		iter.rowIndex++
		return iter.Next() // 通过递归实现每一行的检查,且第一个if可以控制递归退出
	}

	key := row[iter.colIndex] // 第一个if已经检查过是否为空列了，后面的if检查了是否要换行
	value, _ := iter.cm.Get(key)

	if iter.colIndex >= len(row)-1 {
		// 换行
		iter.rowIndex++
		iter.colIndex = 0
	} else {
		iter.colIndex++
	}
	return &MapEntry{key, value}

}

// NewConcurrentHashMap 初始化concurrentHashMap ，seg内部包含几个小map，cap是大map的预估容量
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

// Get concurrentHaspMap的读操作 返回key对应的跳表
func (m *ConcurrentHashMap) Get(key string) (any, bool) {
	index := m.getSegIndex(key, m.seed)

	m.locks[index].RLock()
	defer m.locks[index].RUnlock()

	value, exists := m.mps[index][key]

	return value, exists

}
