package types

// AST（抽象语法树）实现搜索表达式
// builder模式

type TermQuery struct {
	Must    []*TermQuery
	Should  []*TermQuery
	Keyword string
}

func (q *TermQuery) Empty() bool {
	return q.Keyword == "" && len(q.Must) == 0 && len(q.Should) == 0
}

// And 实现 AND 逻辑
func (q *TermQuery) And(queries ...*TermQuery) *TermQuery {
	if len(queries) == 0 {
		return q
	}

	// 预估容量：1 (q本身) + 参数长度
	mergedMust := make([]*TermQuery, 0, 1+len(queries))

	// 逻辑优化：扁平化 (Flatten)
	// 如果 q 本身就是一个纯粹的 "Must" 容器（没有 Keyword 也没有 Should），
	// 我们可以把它的子节点直接提取出来，而不是把它作为一层嵌套。
	if q.Keyword == "" && len(q.Should) == 0 && len(q.Must) > 0 {
		mergedMust = append(mergedMust, q.Must...)
	} else if !q.Empty() {
		mergedMust = append(mergedMust, q)
	}

	// 处理传入的 queries
	for _, ele := range queries {
		if ele.Empty() {
			continue
		}
		// 同样的逻辑，如果传入的也是纯 Must 容器，也可以打平
		if ele.Keyword == "" && len(ele.Should) == 0 && len(ele.Must) > 0 {
			mergedMust = append(mergedMust, ele.Must...)
		} else {
			mergedMust = append(mergedMust, ele)
		}
	}

	return &TermQuery{Must: mergedMust}
}

// Or 实现 OR 逻辑 (对应 Should 字段)
func (q *TermQuery) Or(queries ...*TermQuery) *TermQuery {
	if len(queries) == 0 {
		return q
	}

	// 预估容量：1 (q本身) + 参数长度
	mergedShould := make([]*TermQuery, 0, 1+len(queries))

	// 1. 处理接收者 q
	// 逻辑优化：如果 q 本身就是一个纯粹的 "Should" 容器（没有 Keyword 也没有 Must），
	// 我们可以把它的子节点直接提取出来合并，实现扁平化。
	if q.Keyword == "" && len(q.Must) == 0 && len(q.Should) > 0 {
		mergedShould = append(mergedShould, q.Should...) // 存元素 Must: [ A, B, C ]
	} else if !q.Empty() {
		mergedShould = append(mergedShould, q) // 存切片 Must: [ {Must: [A, B]}, C ]
	}

	// 2. 处理传入的参数 queries
	for _, ele := range queries {
		if ele.Empty() {
			continue
		}
		// 对参数也做同样的扁平化处理
		if ele.Keyword == "" && len(ele.Must) == 0 && len(ele.Should) > 0 {
			mergedShould = append(mergedShould, ele.Should...)
		} else {
			mergedShould = append(mergedShould, ele)
		}
	}

	return &TermQuery{Should: mergedShould}
}
