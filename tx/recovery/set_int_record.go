package recovery

import (
	"fmt"

	"github.com/adieumonks/simple-db/file"
	"github.com/adieumonks/simple-db/log"
)

type SetIntRecord struct {
	txnum  int32
	offset int32
	val    int32
	block  file.BlockID
}

func NewSetIntRecord(txnum int32, block file.BlockID, offset int32, val int32) *SetIntRecord {
	return &SetIntRecord{
		txnum:  txnum,
		offset: offset,
		val:    val,
		block:  block,
	}
}

func NewSetIntRecordFrom(p *file.Page) *SetIntRecord {
	tpos := file.Int32Bytes
	txnum := p.GetInt(tpos)
	fpos := tpos + file.Int32Bytes
	filename := p.GetString(fpos)
	bpos := fpos + file.MaxLength(int32(len(filename)))
	blockNum := p.GetInt(bpos)
	block := file.NewBlockID(filename, blockNum)
	opos := bpos + file.Int32Bytes
	offset := p.GetInt(opos)
	vpos := opos + file.Int32Bytes
	val := p.GetInt(vpos)

	return &SetIntRecord{
		txnum:  txnum,
		offset: offset,
		val:    val,
		block:  block,
	}
}

func (r *SetIntRecord) Op() LogRecordType {
	return SETINT
}

func (r *SetIntRecord) TxNumber() int32 {
	return r.txnum
}

func (r *SetIntRecord) Undo(tx Transaction) {
	tx.Pin(r.block)
	tx.SetInt(r.block, r.offset, r.val, false)
	tx.Unpin(r.block)
}

func (r *SetIntRecord) String() string {
	return fmt.Sprintf("<SETINT %d %v %d %d>", r.txnum, r.block, r.offset, r.val)
}

func (r *SetIntRecord) WriteToLog(lm *log.LogManager) (int32, error) {
	tpos := file.Int32Bytes
	fpos := tpos + file.Int32Bytes
	bpos := fpos + file.MaxLength(int32(len(r.block.Filename())))
	opos := bpos + file.Int32Bytes
	vpos := opos + file.Int32Bytes

	rec := make([]byte, vpos+file.Int32Bytes)
	p := file.NewPageFromBytes(rec)
	p.SetInt(0, int32(SETINT))
	p.SetInt(tpos, r.txnum)
	p.SetString(fpos, r.block.Filename())
	p.SetInt(bpos, r.block.Number())
	p.SetInt(opos, r.offset)
	p.SetInt(vpos, r.val)
	return lm.Append(rec)
}
