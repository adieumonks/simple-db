package buffer

import (
	"github.com/adieumonks/simple-db/file"
	"github.com/adieumonks/simple-db/log"
)

type Buffer struct {
	fm       *file.FileManager
	lm       *log.LogManager
	contents *file.Page
	block    file.BlockID
	pins     int32
	txnum    int32
	lsn      int32
}

func NewBuffer(fm *file.FileManager, lm *log.LogManager) *Buffer {
	return &Buffer{
		fm:       fm,
		lm:       lm,
		contents: file.NewPage(fm.BlockSize()),
		txnum:    -1,
		lsn:      -1,
	}
}

func (b *Buffer) Contents() *file.Page {
	return b.contents
}

func (b *Buffer) Block() file.BlockID {
	return b.block
}

func (b *Buffer) SetModified(txnum, lsn int32) {
	b.txnum = txnum
	if lsn >= 0 {
		b.lsn = lsn
	}
}

func (b *Buffer) IsPinned() bool {
	return b.pins > 0
}

func (b *Buffer) ModifyingTx() int32 {
	return b.txnum
}

func (b *Buffer) AssignToBlock(block file.BlockID) {
	b.Flush()
	b.block = block
	b.fm.Read(block, b.contents)
	b.pins = 0
}

func (b *Buffer) Flush() {
	if b.txnum >= 0 {
		b.lm.Flush(b.lsn)
		b.fm.Write(b.block, b.contents)
		b.txnum = -1
	}
}

func (b *Buffer) Pin() {
	b.pins++
}

func (b *Buffer) Unpin() {
	b.pins--
}