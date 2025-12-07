package course

// Doc 文档的信息，类似：文档1，对应的关键词是【数据结构】，【go语言】
type Doc struct {
	ID       int
	Keywords []string
}

// BuildInvertIndex 对doc文档实现倒排索引
func BuildInvertIndex(docs []*Doc) map[string][]int {
	index := make(map[string][]int, 100) // 预估放100个关键词
	for _, value := range docs {
		for _, docKeyword := range value.Keywords {
			index[docKeyword] = append(index[docKeyword], value.ID)
		}
	}
	return index
}
