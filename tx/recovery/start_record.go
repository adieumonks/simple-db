package recovery

import (
	"fmt"

	"github.com/adieumonks/simple-db/file"
	"github.com/adieumonks/simple-db/log"
)

type StartRecord struct {
	txnum int32
}

func NewStartRecord(txnum int32) *StartRecord {
	return &StartRecord{
		txnum: txnum,
	}
}

func NewStartRecordFrom(p *file.Page) *StartRecord {
	tpos := file.Int32Bytes
	return &StartRecord{
		txnum: p.GetInt(tpos),
	}
}

func (r *StartRecord) Op() LogRecordType {
	return START
}

func (r *StartRecord) TxNumber() int32 {
	return r.txnum
}

func (r *StartRecord) Undo(tx Transaction) {}

func (r *StartRecord) String() string {
	return fmt.Sprintf("<START %d>", r.txnum)
}

func (r *StartRecord) WriteToLog(lm *log.LogManager) (int32, error) {
	rec := make([]byte, 2*file.Int32Bytes)
	p := file.NewPageFromBytes(rec)
	p.SetInt(0, int32(START))
	p.SetInt(file.Int32Bytes, r.txnum)
	return lm.Append(rec)
}
