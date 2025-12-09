package reverse_index

import (
	"RADIC/types"
)

// 统一接口，方便倒排索引的数据结构重构

type IReverseIndexer interface {
	Add(doc types.Document)
	Delete(IntId uint64, keyword *types.Keyword)
	Search(query *types.TermQuery, onFlag uint64, offFlag uint64, orFlags []uint64) []string
}
