package multibuffer

import (
	"fmt"

	"github.com/adieumonks/simple-db/materialize"
	"github.com/adieumonks/simple-db/plan"
	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/record"
	"github.com/adieumonks/simple-db/tx"
)

var _ plan.Plan = (*MultiBufferSortPlan)(nil)

type MultiBufferSortPlan struct {
	tx   *tx.Transaction
	p    plan.Plan
	sch  *record.Schema
	comp *materialize.RecordComparator
}

func NewMultiBufferSortPlan(tx *tx.Transaction, p plan.Plan, sortFields []string) *MultiBufferSortPlan {
	return &MultiBufferSortPlan{
		tx:   tx,
		p:    p,
		sch:  p.Schema(),
		comp: materialize.NewRecordComparator(sortFields),
	}
}

func (sp *MultiBufferSortPlan) Open() (query.Scan, error) {
	src, err := sp.p.Open()
	if err != nil {
		return nil, err
	}

	size := sp.p.BlocksAccessed()
	available := sp.tx.AvailableBuffers()
	numBuffers := BestRoot(available, size)

	fmt.Printf("MultiBufferSortPlan: %d blocks, %d buffers\n", size, numBuffers)

	runs, err := sp.splitIntoRuns(src, numBuffers)
	if err != nil {
		return nil, err
	}
	src.Close()
	for len(runs) > int(numBuffers) {
		runs = sp.doAMergeIteration(runs, numBuffers)
	}
	return NewMultiBufferSortScan(runs, sp.comp)
}

func (sp *MultiBufferSortPlan) BlocksAccessed() int32 {
	mp := materialize.NewMaterializePlan(sp.tx, sp.p)
	return mp.BlocksAccessed()
}

func (sp *MultiBufferSortPlan) RecordsOutput() int32 {
	return sp.p.RecordsOutput()
}

func (sp *MultiBufferSortPlan) DistinctValues(fieldName string) int32 {
	return sp.p.DistinctValues(fieldName)
}

func (sp *MultiBufferSortPlan) Schema() *record.Schema {
	return sp.sch
}

func (sp *MultiBufferSortPlan) splitIntoRuns(src query.Scan, k int32) ([]*materialize.TempTable, error) {
	// 出力先のテンポラリテーブルを作成
	temps := []*materialize.TempTable{}
	currentTemp := materialize.NewTempTable(sp.tx, sp.sch)
	temps = append(temps, currentTemp)
	currentScan, err := currentTemp.Open()
	if err != nil {
		return nil, err
	}

	// in-memoryでソートを行うためにrecord pageを作成
	t := materialize.NewTempTable(sp.tx, sp.sch)
	buffers := make([]*record.RecordPage, k)
	for i := int32(0); i < k; i++ {
		block, err := sp.tx.Append(t.TableName() + ".tbl")
		if err != nil {
			return nil, err
		}
		rp, err := record.NewRecordPage(sp.tx, block, t.GetLayout())
		if err != nil {
			return nil, err
		}
		if err := rp.Format(); err != nil {
			return nil, err
		}
		buffers[i] = rp
	}

	// 入力が空になるまで、kブロック分のデータを読みこみ -> ソート -> 書き込みを繰り返す
	if err := src.BeforeFirst(); err != nil {
		return nil, err
	}
	currentBufferIndex := int32(0)
	rp := buffers[currentBufferIndex]
	totalSlot := int32(0)
	slotCounts := make([]int32, k)
	slot := int32(-1)
	for {
		next, err := src.Next()
		if err != nil {
			return nil, err
		}
		if !next {
			break
		}

		nextSlot, err := rp.InsertAfter(slot)
		if err != nil {
			return nil, err
		}
		if nextSlot < 0 {
			slotCounts[currentBufferIndex] = slot + 1

			// bufferが余っていれば次のbufferを用意
			if currentBufferIndex < k-1 {
				currentBufferIndex++
				rp = buffers[currentBufferIndex]
				nextSlot, err = rp.InsertAfter(-1)
				if err != nil {
					return nil, err
				}

			} else {
				// bufferがいっぱいになったらソートしてテンポラリテーブルに書き込む
				if err := sp.sortInMemory(buffers, slotCounts, totalSlot); err != nil {
					return nil, err
				}

				// rpからcurrentScanにコピー
				if err := sp.copyFromRecordPageToRun(buffers, currentScan); err != nil {
					return nil, err
				}

				// buffersをリセット
				for i := int32(0); i < k; i++ {
					rp = buffers[i]
					if err := rp.Format(); err != nil {
						return nil, err
					}
				}
				currentBufferIndex = 0
				rp = buffers[currentBufferIndex]
				totalSlot = 0
				slotCounts = make([]int32, k)
				nextSlot, err = rp.InsertAfter(-1)
				if err != nil {
					return nil, err
				}

				// 新しいテンポラリテーブルを作成
				currentScan.Close()
				currentTemp = materialize.NewTempTable(sp.tx, sp.sch)
				temps = append(temps, currentTemp)
				currentScan, err = currentTemp.Open()
				if err != nil {
					return nil, err
				}
			}
		}

		for _, fieldName := range sp.sch.Fields() {
			val, err := src.GetVal(fieldName)
			if err != nil {
				return nil, err
			}
			if sp.sch.Type(fieldName) == record.INTEGER {
				if err := rp.SetInt(nextSlot, fieldName, val.AsInt()); err != nil {
					return nil, err
				}
			} else {
				if err := rp.SetString(nextSlot, fieldName, val.AsString()); err != nil {
					return nil, err
				}
			}
		}

		slot = nextSlot
		totalSlot++
	}

	slotCounts[currentBufferIndex] = slot + 1

	// 最後のページをソートしてテンポラリテーブルに書き込む
	if err := sp.sortInMemory(buffers, slotCounts, totalSlot); err != nil {
		return nil, err
	}
	if err := sp.copyFromRecordPageToRun(buffers, currentScan); err != nil {
		return nil, err
	}
	currentScan.Close()
	for i := int32(0); i < k; i++ {
		sp.tx.Unpin(buffers[i].Block())
	}
	return temps, nil
}

// クイックソート
func (sp *MultiBufferSortPlan) sortInMemory(buffers []*record.RecordPage, slotCounts []int32, slots int32) error {
	return sp.quickSort(buffers, slotCounts, 0, slots-1)
}

func (sp *MultiBufferSortPlan) quickSort(buffers []*record.RecordPage, slotCounts []int32, low, high int32) error {
	if low < high {
		pivot, err := sp.partition(buffers, slotCounts, low, high)
		if err != nil {
			return err
		}
		if err := sp.quickSort(buffers, slotCounts, low, pivot-1); err != nil {
			return err
		}
		if err := sp.quickSort(buffers, slotCounts, pivot+1, high); err != nil {
			return err
		}
	}
	return nil
}

func (sp *MultiBufferSortPlan) partition(buffers []*record.RecordPage, slotCounts []int32, low, high int32) (int32, error) {
	pivotSlot := high
	i := low - 1
	for j := low; j < high; j++ {
		cmp, err := sp.compareAcrossBuffers(buffers, slotCounts, j, pivotSlot, sp.comp.Fields())
		if err != nil {
			return 0, err
		}
		if cmp < 0 {
			i++
			if err := sp.swapAcrossBuffers(buffers, slotCounts, i, j); err != nil {
				return 0, err
			}
		}
	}
	if err := sp.swapAcrossBuffers(buffers, slotCounts, i+1, pivotSlot); err != nil {
		return 0, err
	}
	return i + 1, nil
}

func (sp *MultiBufferSortPlan) compareAcrossBuffers(buffers []*record.RecordPage, slotCounts []int32, slot1 int32, slot2 int32, fields []string) (int32, error) {
	rp1, offset1, err := sp.getBlockAndOffset(buffers, slotCounts, slot1)
	if err != nil {
		return 0, err
	}
	rp2, offset2, err := sp.getBlockAndOffset(buffers, slotCounts, slot2)
	if err != nil {
		return 0, err
	}
	for _, fieldName := range fields {
		if sp.sch.Type(fieldName) == record.INTEGER {
			val1, err := rp1.GetInt(offset1, fieldName)
			if err != nil {
				return 0, err
			}
			val2, err := rp2.GetInt(offset2, fieldName)
			if err != nil {
				return 0, err
			}
			if val1 < val2 {
				return -1, nil
			} else if val1 > val2 {
				return 1, nil
			}
		} else {
			val1, err := rp1.GetString(offset1, fieldName)
			if err != nil {
				return 0, err
			}
			val2, err := rp2.GetString(offset2, fieldName)
			if err != nil {
				return 0, err
			}
			if val1 < val2 {
				return -1, nil
			} else if val1 > val2 {
				return 1, nil
			}
		}
	}
	return 0, nil
}

func (sp *MultiBufferSortPlan) swapAcrossBuffers(buffers []*record.RecordPage, slotCounts []int32, slot1 int32, slot2 int32) error {
	rp1, offset1, err := sp.getBlockAndOffset(buffers, slotCounts, slot1)
	if err != nil {
		return err
	}
	rp2, offset2, err := sp.getBlockAndOffset(buffers, slotCounts, slot2)
	if err != nil {
		return err
	}
	for _, fieldName := range sp.sch.Fields() {
		if sp.sch.Type(fieldName) == record.INTEGER {
			val1, err := rp1.GetInt(offset1, fieldName)
			if err != nil {
				return err
			}
			val2, err := rp2.GetInt(offset2, fieldName)
			if err != nil {
				return err
			}
			if err := rp1.SetInt(offset1, fieldName, val2); err != nil {
				return err
			}
			if err := rp2.SetInt(offset2, fieldName, val1); err != nil {
				return err
			}
		} else {
			val1, err := rp1.GetString(offset1, fieldName)
			if err != nil {
				return err
			}
			val2, err := rp2.GetString(offset2, fieldName)
			if err != nil {
				return err
			}
			if err := rp1.SetString(offset1, fieldName, val2); err != nil {
				return err
			}
			if err := rp2.SetString(offset2, fieldName, val1); err != nil {
				return err
			}
		}
	}
	return nil
}

func (sp *MultiBufferSortPlan) getBlockAndOffset(buffers []*record.RecordPage, slotCounts []int32, slot int32) (*record.RecordPage, int32, error) {
	total := int32(0)
	for i := 0; i < len(slotCounts); i++ {
		if total+slotCounts[i] > slot {
			return buffers[i], slot - total, nil
		}
		total += slotCounts[i]
	}
	return nil, 0, fmt.Errorf("slot %d not found", slot)
}

func (sp *MultiBufferSortPlan) copyFromRecordPageToRun(buffers []*record.RecordPage, dest query.UpdateScan) error {
	if err := dest.BeforeFirst(); err != nil {
		return err
	}
	for i := int32(0); i < int32(len(buffers)); i++ {
		rp := buffers[i]
		slot := int32(-1)
		for {
			var err error
			slot, err = rp.NextAfter(slot)
			if err != nil {
				return err
			}
			if slot < 0 {
				break
			}
			if err := dest.Insert(); err != nil {
				return err
			}
			for _, fieldName := range sp.sch.Fields() {
				if sp.sch.Type(fieldName) == record.INTEGER {
					val, err := rp.GetInt(slot, fieldName)
					if err != nil {
						return err
					}
					if err := dest.SetInt(fieldName, val); err != nil {
						return err
					}
				} else {
					val, err := rp.GetString(slot, fieldName)
					if err != nil {
						return err
					}
					if err := dest.SetString(fieldName, val); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func (sp *MultiBufferSortPlan) doAMergeIteration(runs []*materialize.TempTable, k int32) []*materialize.TempTable {
	result := []*materialize.TempTable{}
	for len(runs) > int(k) {
		kruns := runs[:k]
		runs = runs[k:]
		merged, err := sp.mergeSeveralRuns(kruns)
		if err != nil {
			return nil
		}
		result = append(result, merged)
	}
	if len(runs) > 0 {
		result = append(result, runs...)
	}
	return result
}

func (sp *MultiBufferSortPlan) mergeSeveralRuns(runs []*materialize.TempTable) (*materialize.TempTable, error) {
	srcs := []query.Scan{}
	for _, run := range runs {
		src, err := run.Open()
		if err != nil {
			return nil, err
		}
		srcs = append(srcs, src)
	}

	result := materialize.NewTempTable(sp.tx, sp.sch)
	dest, err := result.Open()
	if err != nil {
		return nil, err
	}
	hasMores := make([]bool, len(srcs))
	for i := 0; i < len(srcs); i++ {
		hasMores[i], err = srcs[i].Next()
		if err != nil {
			return nil, err
		}
	}
	for {
		min := -1
		for i := 0; i < len(srcs); i++ {
			if hasMores[i] {
				if min == -1 {
					min = i
				} else {
					cmp, err := sp.comp.Compare(srcs[i], srcs[min])
					if err != nil {
						return nil, err
					}
					if cmp < 0 {
						min = i
					}
				}
			}
		}
		if min == -1 {
			break
		}
		hasMores[min], err = sp.copy(srcs[min], dest)
		if err != nil {
			return nil, err
		}
	}
	for _, src := range srcs {
		src.Close()
	}
	dest.Close()
	return result, nil
}

func (sp *MultiBufferSortPlan) copy(src query.Scan, dest query.UpdateScan) (bool, error) {
	if err := dest.Insert(); err != nil {
		return false, err
	}
	for _, fieldName := range sp.sch.Fields() {
		val, err := src.GetVal(fieldName)
		if err != nil {
			return false, err
		}
		if err := dest.SetVal(fieldName, val); err != nil {
			return false, err
		}
	}
	return src.Next()
}
