package index_service

import (
	"RADIC/internal/kvdb"
	"RADIC/internal/reverse_index"
	"RADIC/types"
	"bytes"
	"encoding/gob"
	"log/slog"
	"strings"
	"sync/atomic"
)

// 外观模式：把正排和倒排2个子系统封装在一起，对外提供更简单的接口

// Indexer 正排索引+倒排索引
type Indexer struct {
	forwardIndex kvdb.IKeyVakyeDB
	reverseIndex reverse_index.IReverseIndexer
	maxIntId     uint64
}

// Init 初始化索引
func (indexer *Indexer) Init(DocNumEstimate, dbtype int, path string) error {
	db, err := kvdb.GetKvDb(dbtype, path)
	if err != nil {
		return err
	}
	indexer.forwardIndex = db
	indexer.reverseIndex = reverse_index.NewSkipListReverseIndex(DocNumEstimate)

	return nil
}

func (indexer *Indexer) Close() error {
	return indexer.forwardIndex.Close()
}

// DeleteDoc 删除索引中指定Id的文档
func (indexer *Indexer) DeleteDoc(docId string) int {
	n := 0
	forwardKey := []byte(docId)

	// 先读正排索引，得到InitId和keywords
	docBs, err := indexer.forwardIndex.Get(forwardKey)
	if err == nil {
		reader := bytes.NewReader([]byte{})
		if len(docBs) > 0 {
			n = 1
			reader.Reset(docBs)
			decoder := gob.NewDecoder(reader)
			var doc types.Document
			err := decoder.Decode(&doc)
			if err == nil {
				for _, kw := range doc.Keywords {
					indexer.reverseIndex.Delete(doc.IntId, kw)
				}
			}
		}
	}
	// 从正排上删除
	indexer.forwardIndex.Delete(forwardKey)
	return n
}

// AddDoc 新增新的文档到索引上，如果之前有相同docID的文档，则删掉
func (indexer *Indexer) AddDoc(doc types.Document) (int, error) {
	docId := strings.TrimSpace(doc.Id)
	if len(docId) == 0 {
		return 0, nil
	}

	// 删除存在的doc
	indexer.DeleteDoc(docId)

	doc.IntId = atomic.AddUint64(&doc.IntId, 1) // 原子性+1，支持并发

	// 写入正排索引
	var value bytes.Buffer
	encoder := gob.NewEncoder(&value)
	if err := encoder.Encode(doc); err != nil {
		return 0, nil
	} else {
		indexer.forwardIndex.Set([]byte(docId), value.Bytes())
	}

	// 写入倒排索引
	indexer.reverseIndex.Add(doc)
	return 1, nil

}

// LoadFromIndexFile 系统重启时，直接从索引文件里加载数据
func (indexer *Indexer) LoadFromIndexFile() int {
	reader := bytes.NewReader([]byte{})
	n := indexer.forwardIndex.IterDB(func(k, v []byte) error {
		reader.Reset(v)
		decoder := gob.NewDecoder(reader)
		var doc types.Document
		err := decoder.Decode(&doc)
		if err != nil {
			slog.Warn("gob decode document failed",
				slog.Any("err", err),
			)
			return err
		}
		indexer.reverseIndex.Add(doc)
		return err
	})
	slog.Info("load data from forward index",
		slog.Any("dataNum", n))
	return int(n)
}

// Search 检索，返回文档列表
func (indexer *Indexer) Search(query *types.TermQuery, onFlag uint64, offFlag uint64, orFlag []uint64) []*types.Document {
	docIds := indexer.reverseIndex.Search(query, onFlag, offFlag, orFlag)
	if len(docIds) == 0 {
		return nil
	}
	keys := make([][]byte, 0, len(docIds))
	for _, docId := range docIds {
		keys = append(keys, []byte(docId))
	}
	data, err := indexer.forwardIndex.BatchGet(keys)
	if err != nil {
		slog.Warn("read kvdb failed", slog.Any("err", err))
		return nil
	}
	result := make([]*types.Document, 0, len(data))
	reader := bytes.NewReader([]byte{})

	for _, docBs := range data {
		if len(docBs) > 0 {
			reader.Reset(docBs)
			decoder := gob.NewDecoder(reader)
			var doc types.Document
			err := decoder.Decode(&doc)
			if err == nil {
				result = append(result, &doc)
			}
		}
	}
	return result
}
