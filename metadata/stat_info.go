package metadata

type StatInfo struct {
	numBlocks  int32
	numRecords int32
}

func NewStatInfo(numBlocks int32, numRecords int32) *StatInfo {
	return &StatInfo{
		numBlocks:  numBlocks,
		numRecords: numRecords,
	}
}

func (si *StatInfo) blocksAccessed() int32 {
	return si.numBlocks
}

func (si *StatInfo) recordsOutput() int32 {
	return si.numRecords
}

func (si *StatInfo) DistinctValues(fieldName string) int32 {
	return 1 + (si.numRecords / 3)
}
