package tx

import (
	"fmt"

	"github.com/adieumonks/simple-db/buffer"
	"github.com/adieumonks/simple-db/file"
)

type BufferList struct {
	buffers map[file.BlockID]*buffer.Buffer
	pins    []file.BlockID
	bm      *buffer.BufferManager
}

func NewBufferList(bm *buffer.BufferManager) *BufferList {
	return &BufferList{
		buffers: make(map[file.BlockID]*buffer.Buffer),
		pins:    make([]file.BlockID, 0),
		bm:      bm,
	}
}

func (bl *BufferList) GetBuffer(block file.BlockID) *buffer.Buffer {
	return bl.buffers[block]
}

func (bl *BufferList) Pin(block file.BlockID) error {
	buffer, err := bl.bm.Pin(block)
	if err != nil {
		return fmt.Errorf("failed to pin block: %w", err)
	}
	bl.buffers[block] = buffer
	bl.pins = append(bl.pins, block)
	return nil
}

func (bl *BufferList) Unpin(block file.BlockID) {
	buff := bl.buffers[block]
	bl.bm.Unpin(buff)
	bl.removeBlockFromPins(block)
	if !bl.containsBlockInPins(block) {
		delete(bl.buffers, block)
	}
}

func (bl *BufferList) UnpinAll() {
	for _, block := range bl.pins {
		buffer := bl.buffers[block]
		bl.bm.Unpin(buffer)
	}
	bl.buffers = make(map[file.BlockID]*buffer.Buffer)
	bl.pins = make([]file.BlockID, 0)
}

func (bl *BufferList) containsBlockInPins(block file.BlockID) bool {
	for _, b := range bl.pins {
		if b == block {
			return true
		}
	}
	return false
}

func (bl *BufferList) removeBlockFromPins(block file.BlockID) {
	for i, b := range bl.pins {
		if b == block {
			bl.pins = append(bl.pins[:i], bl.pins[i+1:]...)
			break
		}
	}
}
