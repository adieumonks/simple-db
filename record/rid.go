package record

import "fmt"

type RID struct {
	blockNum int32
	slot     int32
}

func NewRID(blockNum int32, slot int32) *RID {
	return &RID{blockNum, slot}
}

func (rid *RID) BlockNumber() int32 {
	return rid.blockNum
}

func (rid *RID) Slot() int32 {
	return rid.slot
}

func (rid *RID) Equals(other *RID) bool {
	return rid.blockNum == other.blockNum && rid.slot == other.slot
}

func (rid *RID) String() string {
	return fmt.Sprintf("[%d, %d]", rid.blockNum, rid.slot)
}
