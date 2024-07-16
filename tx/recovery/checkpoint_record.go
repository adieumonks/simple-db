package recovery

import (
	"github.com/adieumonks/simple-db/file"
	"github.com/adieumonks/simple-db/log"
)

type CheckPointRecord struct{}

func NewCheckpointRecord() *CheckPointRecord {
	return &CheckPointRecord{}
}

func (r *CheckPointRecord) Op() LogRecordType {
	return CHECKPOINT
}

func (r *CheckPointRecord) TxNumber() int32 {
	return -1
}

func (r *CheckPointRecord) Undo(tx Transaction) {}

func (r *CheckPointRecord) String() string {
	return "<CHECKPOINT>"
}

func (r *CheckPointRecord) WriteToLog(lm *log.LogManager) (int32, error) {
	rec := make([]byte, file.Int32Bytes)
	p := file.NewPageFromBytes(rec)
	p.SetInt(0, int32(CHECKPOINT))
	return lm.Append(rec)
}
