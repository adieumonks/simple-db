package recovery

import (
	"fmt"

	"github.com/adieumonks/simple-db/file"
	"github.com/adieumonks/simple-db/log"
)

type CommitRecord struct {
	txnum int32
}

func NewCommitRecord(txnum int32) *CommitRecord {
	return &CommitRecord{
		txnum: txnum,
	}
}

func NewCommitRecordFrom(p *file.Page) *CommitRecord {
	tpos := file.Int32Bytes
	return &CommitRecord{
		txnum: p.GetInt(tpos),
	}
}

func (r *CommitRecord) Op() LogRecordType {
	return COMMIT
}

func (r *CommitRecord) TxNumber() int32 {
	return r.txnum
}

func (r *CommitRecord) Undo(tx Transaction) error {
	return nil
}

func (r *CommitRecord) String() string {
	return fmt.Sprintf("<COMMIT %d>", r.txnum)
}

func (r *CommitRecord) WriteToLog(lm *log.LogManager) (int32, error) {
	rec := make([]byte, 2*file.Int32Bytes)
	p := file.NewPageFromBytes(rec)
	p.SetInt(0, int32(COMMIT))
	p.SetInt(file.Int32Bytes, r.txnum)
	return lm.Append(rec)
}
