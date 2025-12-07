package test

import (
	"RADIC/course"
	"fmt"
	"testing"
)

func TestBuildInvertIndex(t *testing.T) {
	docs := []*course.Doc{&course.Doc{
		ID:       0,
		Keywords: []string{"go", "java", "python"},
	}, &course.Doc{
		ID:       1,
		Keywords: []string{"php", "java", "rust"},
	}, &course.Doc{
		ID:       2,
		Keywords: []string{"cpp", "python", "go"},
	}}

	index := course.BuildInvertIndex(docs)
	for k, v := range index {
		fmt.Printf("关键词%s所在的文档ID：%v\n", k, v)
	}
}
