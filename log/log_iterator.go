package log

import (
	"fmt"

	"github.com/adieumonks/simple-db/file"
)

type LogIterator struct {
	fm        *file.FileManager
	block     file.BlockID
	page      *file.Page
	curentPos int32
	boundary  int32
}

func NewLogIterator(fm *file.FileManager, block file.BlockID) (*LogIterator, error) {
	b := make([]byte, fm.BlockSize())
	page := file.NewPageFromBytes(b)

	it := &LogIterator{
		fm:    fm,
		block: block,
		page:  page,
	}

	err := it.moveToBlock(block)
	if err != nil {
		return nil, fmt.Errorf("failed to move to block %v: %w", block, err)
	}
	return it, nil
}

func (it *LogIterator) HasNext() bool {
	return it.curentPos < it.fm.BlockSize() || it.block.Number() > 0
}

func (it *LogIterator) Next() []byte {
	if it.curentPos == it.fm.BlockSize() {
		it.block = file.NewBlockID(it.block.Filename(), it.block.Number()-1)
		it.moveToBlock(it.block)
	}

	rec := it.page.GetBytes(it.curentPos)
	it.curentPos += file.Int32Bytes + int32(len(rec))
	return rec
}

func (it *LogIterator) moveToBlock(block file.BlockID) error {
	err := it.fm.Read(block, it.page)
	if err != nil {
		return fmt.Errorf("failed to read block %v: %w", block, err)
	}
	it.boundary = it.page.GetInt(0)
	it.curentPos = it.boundary
	return nil
}
