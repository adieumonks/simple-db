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

func (b *Buffer) AssignToBlock(block file.BlockID) error {
	b.Flush()
	b.block = block
	if err := b.fm.Read(block, b.contents); err != nil {
		return err
	}
	b.pins = 0
	return nil
}

func (b *Buffer) Flush() error {
	if b.txnum >= 0 {
		b.lm.Flush(b.lsn)
		if err := b.fm.Write(b.block, b.contents); err != nil {
			return err
		}
		b.txnum = -1
	}
	return nil
}

func (b *Buffer) Pin() {
	b.pins++
}

func (b *Buffer) Unpin() {
	b.pins--
}
