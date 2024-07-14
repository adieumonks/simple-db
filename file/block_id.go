package file

import "fmt"

type BlockID struct {
	filename string
	blockNum int32
}

func NewBlockID(filename string, blockNum int32) *BlockID {
	return &BlockID{
		filename: filename,
		blockNum: blockNum,
	}
}

func (b *BlockID) Filename() string {
	return b.filename
}

func (b *BlockID) Number() int32 {
	return b.blockNum
}

func (b *BlockID) Equals(other *BlockID) bool {
	return b.filename == other.filename && b.blockNum == other.blockNum
}

func (b *BlockID) String() string {
	return fmt.Sprintf("[file %s, block %d]", b.filename, b.blockNum)
}
