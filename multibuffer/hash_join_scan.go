package multibuffer

import (
	"github.com/adieumonks/simple-db/materialize"
	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/tx"
)

var _ query.Scan = (*HashJoinScan)(nil)

type HashJoinScan struct {
	tx                 *tx.Transaction
	buckets1, buckets2 []*materialize.TempTable
	currentBucket      int
	currentScan        *MultibufferProductScan
}

func NewHashJoinScan(tx *tx.Transaction, buckets1, buckets2 []*materialize.TempTable) *HashJoinScan {
	return &HashJoinScan{
		tx:       tx,
		buckets1: buckets1,
		buckets2: buckets2,
	}
}

func (s *HashJoinScan) BeforeFirst() error {
	s.currentBucket = 0

	leftScan, err := s.buckets1[s.currentBucket].Open()
	if err != nil {
		return err
	}

	rightTableName := s.buckets2[s.currentBucket].TableName()
	rightLayout := s.buckets2[s.currentBucket].GetLayout()
	s.currentScan, err = NewMultibufferProductScan(s.tx, leftScan, rightTableName, rightLayout)
	if err != nil {
		return err
	}

	if err := s.currentScan.BeforeFirst(); err != nil {
		return err
	}

	return nil
}

func (s *HashJoinScan) Next() (bool, error) {
	for {
		hasNextRecord, err := s.currentScan.Next()
		if err != nil {
			return false, err
		}

		if hasNextRecord {
			return true, nil
		}

		s.currentBucket++
		if s.currentBucket >= len(s.buckets1) {
			return false, nil
		}

		s.currentScan.Close()
		leftScan, err := s.buckets1[s.currentBucket].Open()
		if err != nil {
			return false, err
		}

		rightTableName := s.buckets2[s.currentBucket].TableName()
		rightLayout := s.buckets2[s.currentBucket].GetLayout()
		s.currentScan, err = NewMultibufferProductScan(s.tx, leftScan, rightTableName, rightLayout)
		if err != nil {
			return false, err
		}

		if err := s.currentScan.BeforeFirst(); err != nil {
			return false, err
		}
	}
}

func (s *HashJoinScan) GetInt(fieldName string) (int32, error) {
	return s.currentScan.GetInt(fieldName)
}

func (s *HashJoinScan) GetString(fieldName string) (string, error) {
	return s.currentScan.GetString(fieldName)
}

func (s *HashJoinScan) GetVal(fieldName string) (*query.Constant, error) {
	return s.currentScan.GetVal(fieldName)
}

func (s *HashJoinScan) HasField(fieldName string) bool {
	return s.currentScan.HasField(fieldName)
}

func (s *HashJoinScan) Close() {
	s.currentScan.Close()
}
