package multibuffer

import (
	"github.com/adieumonks/simple-db/file"
	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/record"
	"github.com/adieumonks/simple-db/tx"
)

var _ query.Scan = (*ChunkScan)(nil)

type ChunkScan struct {
	buffs           []*record.RecordPage
	tx              *tx.Transaction
	fileName        string
	layout          *record.Layout
	startBlockNum   int32
	endBlockNum     int32
	currentBlockNum int32
	rp              *record.RecordPage
	currentSlot     int32
}

func NewChunkScan(
	tx *tx.Transaction,
	fileName string,
	layout *record.Layout,
	startBlockNum int32,
	endBlockNum int32,
) (*ChunkScan, error) {
	s := &ChunkScan{
		buffs:         make([]*record.RecordPage, 0),
		tx:            tx,
		fileName:      fileName,
		layout:        layout,
		startBlockNum: startBlockNum,
		endBlockNum:   endBlockNum,
	}
	for i := startBlockNum; i <= endBlockNum; i++ {
		block := file.NewBlockID(fileName, i)
		rp, err := record.NewRecordPage(tx, block, layout)
		if err != nil {
			return nil, err
		}
		s.buffs = append(s.buffs, rp)
	}
	return s, nil
}

func (s *ChunkScan) Close() {
	for i := 0; i < len(s.buffs); i++ {
		block := file.NewBlockID(s.fileName, s.startBlockNum+int32(i))
		s.tx.Unpin(block)
	}
}

func (s *ChunkScan) BeforeFirst() error {
	s.moveToBlock(s.startBlockNum)
	return nil
}

func (s *ChunkScan) Next() (bool, error) {
	var err error
	s.currentSlot, err = s.rp.NextAfter(s.currentSlot)
	if err != nil {
		return false, err
	}

	for s.currentSlot < 0 {
		if s.currentBlockNum == s.endBlockNum {
			return false, nil
		}
		s.moveToBlock(s.rp.Block().Number() + 1)
		s.currentSlot, err = s.rp.NextAfter(s.currentSlot)
	}
	return true, nil
}

func (s *ChunkScan) GetInt(fieldName string) (int32, error) {
	return s.rp.GetInt(s.currentSlot, fieldName)
}

func (s *ChunkScan) GetString(fieldName string) (string, error) {
	return s.rp.GetString(s.currentSlot, fieldName)
}

func (s *ChunkScan) GetVal(fieldName string) (*query.Constant, error) {
	if s.layout.Schema().Type(fieldName) == record.INTEGER {
		ival, err := s.GetInt(fieldName)
		if err != nil {
			return nil, err
		}
		return query.NewConstantWithInt(ival), nil
	} else {
		sval, err := s.GetString(fieldName)
		if err != nil {
			return nil, err
		}
		return query.NewConstantWithString(sval), nil
	}
}

func (s *ChunkScan) HasField(fieldName string) bool {
	return s.layout.Schema().HasField(fieldName)
}

func (s *ChunkScan) moveToBlock(blockNum int32) {
	s.currentBlockNum = blockNum
	s.rp = s.buffs[s.currentBlockNum-s.startBlockNum]
	s.currentSlot = -1
}
