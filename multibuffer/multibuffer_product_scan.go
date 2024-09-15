package multibuffer

import (
	"fmt"

	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/record"
	"github.com/adieumonks/simple-db/tx"
)

var _ query.Scan = (*MultibufferProductScan)(nil)

type MultibufferProductScan struct {
	tx           *tx.Transaction
	lhsScan      query.Scan
	rhsScan      query.Scan
	prodScan     query.Scan
	fileName     string
	layout       *record.Layout
	chunkSize    int32
	nextBlockNum int32
	fileSize     int32
}

func NewMultibufferProductScan(tx *tx.Transaction, lhsScan query.Scan, tableName string, layout *record.Layout) (*MultibufferProductScan, error) {
	s := &MultibufferProductScan{
		tx:       tx,
		lhsScan:  lhsScan,
		fileName: fmt.Sprintf("%s.tbl", tableName),
		layout:   layout,
	}
	fileSize, err := s.tx.Size(s.fileName)
	if err != nil {
		return nil, err
	}
	s.fileSize = fileSize
	available := tx.AvailableBuffers()
	s.chunkSize = BestFactor(available, s.fileSize)
	if err := s.BeforeFirst(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *MultibufferProductScan) BeforeFirst() error {
	s.nextBlockNum = 0
	if _, err := s.useNextChunk(); err != nil {
		return err
	}
	return nil
}

func (s *MultibufferProductScan) Next() (bool, error) {
	for {
		hasNextRecord, err := s.prodScan.Next()
		if err != nil {
			return false, err
		}
		if hasNextRecord {
			break
		}

		hasNextChunk, err := s.useNextChunk()
		if err != nil {
			return false, err
		}
		if !hasNextChunk {
			return false, nil
		}
	}
	return true, nil
}

func (s *MultibufferProductScan) Close() {
	s.prodScan.Close()
}

func (s *MultibufferProductScan) GetVal(fieldName string) (*query.Constant, error) {
	return s.prodScan.GetVal(fieldName)
}

func (s *MultibufferProductScan) GetInt(fieldName string) (int32, error) {
	return s.prodScan.GetInt(fieldName)
}

func (s *MultibufferProductScan) GetString(fieldName string) (string, error) {
	return s.prodScan.GetString(fieldName)
}

func (s *MultibufferProductScan) HasField(fieldName string) bool {
	return s.prodScan.HasField(fieldName)
}

func (s *MultibufferProductScan) useNextChunk() (bool, error) {
	if s.nextBlockNum >= s.fileSize {
		return false, nil
	}

	if s.rhsScan != nil {
		s.rhsScan.Close()
	}
	end := s.nextBlockNum + s.chunkSize - 1
	cs, err := NewChunkScan(s.tx, s.fileName, s.layout, s.nextBlockNum, end)
	if err != nil {
		return false, err
	}
	s.rhsScan = cs

	if err := s.lhsScan.BeforeFirst(); err != nil {
		return false, err
	}
	ps, err := query.NewProductScan(s.lhsScan, s.rhsScan)
	if err != nil {
		return false, err
	}
	s.prodScan = ps

	s.nextBlockNum = end + 1

	return true, nil
}
