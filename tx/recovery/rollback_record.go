package recovery

import (
	"fmt"

	"github.com/adieumonks/simple-db/file"
	"github.com/adieumonks/simple-db/log"
)

type RollbackRecord struct {
	txnum int32
}

func NewRollbackRecord(txnum int32) *RollbackRecord {
	return &RollbackRecord{
		txnum: txnum,
	}
}

func NewRollbackRecordFrom(p *file.Page) *RollbackRecord {
	tpos := file.Int32Bytes
	return &RollbackRecord{
		txnum: p.GetInt(tpos),
	}
}

func (r *RollbackRecord) Op() LogRecordType {
	return ROLLBACK
}

func (r *RollbackRecord) TxNumber() int32 {
	return r.txnum
}

func (r *RollbackRecord) Undo(tx Transaction) {}

func (r *RollbackRecord) String() string {
	return fmt.Sprintf("<ROLLBACK %d>", r.txnum)
}

func (r *RollbackRecord) WriteToLog(lm *log.LogManager) (int32, error) {
	rec := make([]byte, 2*file.Int32Bytes)
	p := file.NewPageFromBytes(rec)
	p.SetInt(0, int32(ROLLBACK))
	p.SetInt(file.Int32Bytes, r.txnum)
	return lm.Append(rec)
}
