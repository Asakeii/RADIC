package reverse_index

import (
	"RADIC/types"
	"RADIC/util"
	"github.com/huandu/skiplist"
	farmhash "github.com/leemcloughlin/gofarmhash"
	"runtime"
	"sync"
)

// SkipListReverseIndex 倒排索引整体上是map， map的value是一个List
type SkipListReverseIndex struct {
	table *util.ConcurrentHashMap // 分段map，并发安全
	locks []sync.RWMutex          // 修改倒排索引时，相同的key需要去竞争同一把锁
}

// SkipListValue 将Id和BitsFeature封装到一起，因为在跳表中key对应的是document的IntId，value是业务侧的Id和BitsFeature
type SkipListValue struct {
	Id          string
	BitsFeature uint64
}

// NewSkipListReverseIndex 初始化倒排索引，DocNumEstimate是预估的doc数量
func NewSkipListReverseIndex(DocNumEstimate int) *SkipListReverseIndex {
	indexer := new(SkipListReverseIndex)
	indexer.table = util.NewConcurrentHashMap(runtime.NumCPU(), DocNumEstimate)
	indexer.locks = make([]sync.RWMutex, 1000)
	return indexer
}

// Add 将文档增加到倒排索引中
func (indexer *SkipListReverseIndex) Add(doc types.Document) {
	for _, keyword := range doc.Keywords {
		key := keyword.ToString()
		lock := indexer.getLock(key)
		lock.Lock()
		sklValue := SkipListValue{doc.Id, doc.BitsFeature}
		if value, exists := indexer.table.Get(key); exists {
			list := value.(*skiplist.SkipList)
			list.Set(doc.IntId, sklValue)
		} else {
			list := skiplist.New(skiplist.Uint64)
			list.Set(doc.IntId, sklValue)
			indexer.table.Set(key, list)
		}
		lock.Unlock()
	}
}

// Delete 根据IntId删除key上的对应的doc
func (indexer *SkipListReverseIndex) Delete(IntId uint64, keyword *types.Keyword) {
	key := keyword.ToString()
	lock := indexer.getLock(key)
	lock.Lock()
	if value, exists := indexer.table.Get(key); exists {
		list := value.(*skiplist.SkipList)
		list.Remove(IntId)
	}
	lock.Unlock()
}

// getLock 通过哈希方式，将key分成多组，每组key争夺一个lock去写，相当于每个key都有一把锁，但是没办法开辟那么多锁，因为不知道会有多少个key
func (indexer *SkipListReverseIndex) getLock(key string) *sync.RWMutex {
	n := int(farmhash.Hash32WithSeed([]byte(key), 0))
	return &indexer.locks[n%len(indexer.locks)]
}

// IntersectionOfSkipList 求多个跳表的交集 利用了跳表有序的特性，使用多路归并（Multi-way Merge）的思想。
func IntersectionOfSkipList(lists ...*skiplist.SkipList) *skiplist.SkipList {

	// 初始化检查
	if len(lists) == 0 {
		return nil
	}
	if len(lists) == 1 {
		return lists[0]
	}

	result := skiplist.New(skiplist.Uint64)        // 新建一个结果跳表
	iters := make([]*skiplist.Element, len(lists)) // 指向每一个跳表的单个元素的指针 切片

	// 遍历每一个跳表，如果有空的，则直接return，同时给iters初始化到每个跳表的首元素地址
	for i, list := range lists {
		if list == nil || list.Len() == 0 {
			// 只要有一个跳表是空的，则结果交集为空
			return nil
		}
		iters[i] = list.Front()
	}

	for {
		maxList := make(map[int]struct{}, len(iters)) // 存放每个跳表的当前最大值的key
		var maxValue uint64 = 0                       // 当前最大值
		// 以上的变量：每一轮都会重新初始化

		// 遍历每个跳表的当前访问元素
		for i, node := range iters {
			if node.Key().(uint64) > maxValue {
				// 如果当前跳表的key值 > 当前最大值，更新最大值并用maxList存储元素key
				maxValue = node.Key().(uint64)
				maxList = map[int]struct{}{i: {}}
			} else if node.Key().(uint64) == maxValue {
				// 如果当前跳表的key值 == 当前最大值，填充当前跳表对应的maxList
				maxList[i] = struct{}{}
			}
		}
		// 循环结束后，如果当前指向的都是相等的最大值，则maxList满，说明找到了相交元素
		// 如果maxList不满，说明当前iters切片中指向的各个跳表中的元素还有比maxValue小的

		if len(maxList) == len(iters) {
			result.Set(iters[0].Key(), iters[0].Value) // 找到相交元素，将kv存入result

			// 让各指针都next
			for i, node := range iters {
				iters[i] = node.Next()
				if iters[i] == nil {
					return result // 只要有一个跳表走完，说明后面就不会再有交集了
				}
			}
		} else {
			// 如果不满足相交条件，应让小于maxValue的跳表指针向后next
			for i := range iters {
				for iters[i] != nil && iters[i].Key().(uint64) < maxValue {
					// 让不等于max的往后next，直到相等或大于
					iters[i] = iters[i].Next()
				}
				if iters[i] == nil {
					return result // 只要有一个跳表走完，说明后面就不会再有交集了
				}
			}
		}

	}
}

// UnionOfSkipList 求多个跳表的并集
// 逻辑：多路归并。每一轮找出所有指针中 Key 最小的那个值，加入结果集，然后让拥有该最小值的指针后移。
func UnionOfSkipList(lists ...*skiplist.SkipList) *skiplist.SkipList {

	// 初始化检查
	if len(lists) == 0 {
		return nil
	}
	if len(lists) == 1 {
		return lists[0]
	}

	result := skiplist.New(skiplist.Uint64)        // 新建一个结果跳表
	iters := make([]*skiplist.Element, len(lists)) // 指向每一个跳表的单个元素的指针

	// 初始化指针
	for i, list := range lists {
		if list != nil && list.Len() > 0 {
			iters[i] = list.Front()
		} else {
			iters[i] = nil // 空跳表直接置为 nil，不影响其他跳表的合并
		}
	}

	for {
		var minValue uint64 = 0
		minFound := false // 标记本轮是否找到了有效的最小值

		// 1. 第一轮遍历：找出当前所有 iters 指向的元素中，最小的 Key 是多少
		for _, node := range iters {
			if node != nil {
				val := node.Key().(uint64)
				if !minFound {
					// 如果是本轮看到的第一个非空节点，直接初始化 minValue
					minValue = val
					minFound = true
				} else {
					// 否则与当前最小值比较，取更小者
					if val < minValue {
						minValue = val
					}
				}
			}
		}

		// 如果遍历完一圈，minFound 依然是 false，说明所有 iters 都是 nil 了
		// 意味着所有跳表都处理完毕，合并结束
		if !minFound {
			return result
		}

		// 2. 将找到的最小值存入结果集
		// 注意：这里需要暂时保存 Value，如果有多个跳表包含同一个 Key，通常保留任意一个即可（这里保留最后一个遍历到的）
		var targetValue interface{}

		// 3. 第二轮遍历：找到所有等于 minValue 的节点，让它们向前移动 (Next)
		for i, node := range iters {
			if node != nil && node.Key().(uint64) == minValue {
				targetValue = node.Value // 获取值
				iters[i] = node.Next()   // 关键点：命中最小值的指针都要往前走，没命中的原地不动
			}
		}

		// 写入结果
		result.Set(minValue, targetValue)
	}
}

// FilterByBits 倒排索引的特征过滤
func (indexer SkipListReverseIndex) FilterByBits(bit uint64, onFlag uint64, offFlag uint64, orFlags []uint64) bool {
	// bit: 文档自身的属性	需要对应的条件写入xxFlag中，不同的Flag对应不同的要求

	// onFlag:所有bit必须全部命中
	if bit&onFlag != onFlag {
		return false
	}
	// offFlag:所有bit必须全部不命中
	if bit&offFlag != 0 {
		return false
	}
	// 多个orFlags必须全部命中
	for _, orFlag := range orFlags {
		if orFlag > 0 && bit&orFlag == 0 {
			// 单个orFlag只有一个bit命中即可
			return false
		}
	}
	return true
}

func (indexer SkipListReverseIndex) search(q *types.TermQuery, onFlag uint64, offFlag uint64, orFlags []uint64) *skiplist.SkipList {
	if q.Keyword != "" {
		keyword := q.Keyword
		if value, exists := indexer.table.Get(keyword); exists {
			result := skiplist.New(skiplist.Uint64)
			list := value.(*skiplist.SkipList)
			node := list.Front()
			for node != nil {
				intId := node.Key().(uint64)
				skv, _ := node.Value.(SkipListValue)
				flag := skv.BitsFeature
				if intId > 0 && indexer.FilterByBits(flag, onFlag, offFlag, orFlags) {
					result.Set(intId, skv)
				}
				node = node.Next()
			}
			return result
		}
	} else if len(q.Must) > 0 {
		results := make([]*skiplist.SkipList, 0, len(q.Must))
		for _, q := range q.Must {
			results = append(results, indexer.search(q, onFlag, offFlag, orFlags))
		}
		return IntersectionOfSkipList(results...)
	} else if len(q.Should) > 0 {
		results := make([]*skiplist.SkipList, 0, len(q.Should))
		for _, q := range q.Should {
			results = append(results, indexer.search(q, onFlag, offFlag, orFlags))
		}
		return UnionOfSkipList(results...)
	}

	return nil
}

// Search 搜索，返回docId
func (indexer SkipListReverseIndex) Search(query *types.TermQuery, onFlag uint64, offFlag uint64, orFlags []uint64) []string {
	result := indexer.search(query, onFlag, offFlag, orFlags)
	if result == nil {
		return nil
	}

	arr := make([]string, 0, result.Len())
	node := result.Front()
	for node != nil {
		skv, _ := node.Value.(SkipListValue)
		arr = append(arr, skv.Id)
		node = node.Next()
	}
	return arr
}
