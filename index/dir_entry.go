package index

import "github.com/adieumonks/simple-db/query"

type DirEntry struct {
	dataVal  *query.Constant
	blockNum int32
}

func NewDirEntry(dataVal *query.Constant, blockNum int32) *DirEntry {
	return &DirEntry{dataVal: dataVal, blockNum: blockNum}
}

func (de *DirEntry) DataVal() *query.Constant {
	return de.dataVal
}

func (de *DirEntry) BlockNumber() int32 {
	return de.blockNum
}
