package index_service

import "RADIC/types"

type IIndexer interface {
	AddDoc(doc types.Document) (int, error)
	DeleteDoc(docId string)
	intSearch(query *types.TermQuery, onFlag uint64, offFlag uint64, orFlag []uint64)
}
