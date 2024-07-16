package recovery

import (
	"fmt"

	"github.com/adieumonks/simple-db/file"
	"github.com/adieumonks/simple-db/log"
)

type SetStringRecord struct {
	txnum  int32
	offset int32
	val    string
	block  file.BlockID
}

func NewSetStringRecord(txnum int32, block file.BlockID, offset int32, val string) *SetStringRecord {
	return &SetStringRecord{
		txnum:  txnum,
		offset: offset,
		val:    val,
		block:  block,
	}
}

func NewSetStringRecordFrom(p *file.Page) *SetStringRecord {
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
	val := p.GetString(vpos)

	return &SetStringRecord{
		txnum:  txnum,
		offset: offset,
		val:    val,
		block:  block,
	}
}

func (r *SetStringRecord) Op() LogRecordType {
	return SETSTRING
}

func (r *SetStringRecord) TxNumber() int32 {
	return r.txnum
}

func (r *SetStringRecord) Undo(tx Transaction) {
	tx.Pin(r.block)
	tx.SetString(r.block, r.offset, r.val, false)
	tx.Unpin(r.block)
}

func (r *SetStringRecord) String() string {
	return fmt.Sprintf("<SETSTRING %d %v %d %s>", r.txnum, r.block, r.offset, r.val)
}

func (r *SetStringRecord) WriteToLog(lm *log.LogManager) (int32, error) {
	tpos := file.Int32Bytes
	fpos := tpos + file.Int32Bytes
	bpos := fpos + file.MaxLength(int32(len(r.block.Filename())))
	opos := bpos + file.Int32Bytes
	vpos := opos + file.Int32Bytes
	recLength := vpos + file.MaxLength(int32(len(r.val)))

	rec := make([]byte, recLength)
	p := file.NewPageFromBytes(rec)
	p.SetInt(0, int32(SETSTRING))
	p.SetInt(tpos, r.txnum)
	p.SetString(fpos, r.block.Filename())
	p.SetInt(bpos, r.block.Number())
	p.SetInt(opos, r.offset)
	p.SetString(vpos, r.val)
	return lm.Append(rec)
}
