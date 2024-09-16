package materialize

import (
	"github.com/adieumonks/simple-db/plan"
	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/record"
	"github.com/adieumonks/simple-db/tx"
)

var _ plan.Plan = (*OneBufferSortPlan)(nil)

type OneBufferSortPlan struct {
	tx   *tx.Transaction
	p    plan.Plan
	sch  *record.Schema
	comp *RecordComparator
}

func NewOneBufferSortPlan(tx *tx.Transaction, p plan.Plan, sortFields []string) *OneBufferSortPlan {
	return &OneBufferSortPlan{
		tx:   tx,
		p:    p,
		sch:  p.Schema(),
		comp: NewRecordComparator(sortFields),
	}
}

func (sp *OneBufferSortPlan) Open() (query.Scan, error) {
	src, err := sp.p.Open()
	if err != nil {
		return nil, err
	}
	runs, err := sp.splitIntoRuns(src)
	if err != nil {
		return nil, err
	}
	src.Close()
	for len(runs) > 2 {
		runs = sp.doAMergeIteration(runs)
	}
	return NewSortScan(runs, sp.comp)
}

func (sp *OneBufferSortPlan) BlocksAccessed() int32 {
	mp := NewMaterializePlan(sp.tx, sp.p)
	return mp.BlocksAccessed()
}

func (sp *OneBufferSortPlan) RecordsOutput() int32 {
	return sp.p.RecordsOutput()
}

func (sp *OneBufferSortPlan) DistinctValues(fieldName string) int32 {
	return sp.p.DistinctValues(fieldName)
}

func (sp *OneBufferSortPlan) Schema() *record.Schema {
	return sp.sch
}

func (sp *OneBufferSortPlan) splitIntoRuns(src query.Scan) ([]*TempTable, error) {
	// 出力先のテンポラリテーブルを作成
	temps := []*TempTable{}
	currentTemp := NewTempTable(sp.tx, sp.sch)
	temps = append(temps, currentTemp)
	currentScan, err := currentTemp.Open()
	if err != nil {
		return nil, err
	}

	// in-memoryでソートを行うためにrecord pageを作成
	t := NewTempTable(sp.tx, sp.sch)
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

	// 入力が空になるまで、1ブロック分のデータを読みこみ -> ソート -> 書き込みを繰り返す
	if err := src.BeforeFirst(); err != nil {
		return nil, err
	}
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
			// ページがいっぱいになったらソートしてテンポラリテーブルに書き込む
			if err := sp.sortInMemory(rp, slot+1); err != nil {
				return nil, err
			}

			// rpからcurrentScanにコピー
			if err := sp.copyFromRecordPageToRun(rp, currentScan); err != nil {
				return nil, err
			}

			// rpをリセット
			if err := rp.Format(); err != nil {
				return nil, err
			}
			nextSlot, err = rp.InsertAfter(-1)
			if err != nil {
				return nil, err
			}

			// 新しいテンポラリテーブルを作成
			currentScan.Close()
			currentTemp = NewTempTable(sp.tx, sp.sch)
			temps = append(temps, currentTemp)
			currentScan, err = currentTemp.Open()
			if err != nil {
				return nil, err
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
	}

	// 最後のページをソートしてテンポラリテーブルに書き込む
	if err := sp.sortInMemory(rp, slot+1); err != nil {
		return nil, err
	}
	if err := sp.copyFromRecordPageToRun(rp, currentScan); err != nil {
		return nil, err
	}
	currentScan.Close()
	sp.tx.Unpin(block)
	return temps, nil
}

// // 挿入ソート
// func (sp *OneBufferSortPlan) sortInMemory(rp *record.RecordPage, slots int32) error {
// 	for i := int32(1); i < slots; i++ {
// 		j := i
// 		for j > 0 {
// 			cmp, err := rp.Compare(j, j-1, sp.comp.Fields())
// 			if err != nil {
// 				return err
// 			}
// 			if cmp >= 0 {
// 				break
// 			}
// 			if err := rp.Swap(j, j-1); err != nil {
// 				return err
// 			}
// 			j--
// 		}
// 	}
// 	return nil
// }

// クイックソート
func (sp *OneBufferSortPlan) sortInMemory(rp *record.RecordPage, slots int32) error {
	return sp.quickSort(rp, 0, slots-1)
}

func (sp *OneBufferSortPlan) quickSort(rp *record.RecordPage, low, high int32) error {
	if low < high {
		pivot, err := sp.partition(rp, low, high)
		if err != nil {
			return err
		}
		if err := sp.quickSort(rp, low, pivot-1); err != nil {
			return err
		}
		if err := sp.quickSort(rp, pivot+1, high); err != nil {
			return err
		}
	}
	return nil
}

func (sp *OneBufferSortPlan) partition(rp *record.RecordPage, low, high int32) (int32, error) {
	pivotSlot := high
	i := low - 1
	for j := low; j < high; j++ {
		cmp, err := rp.Compare(j, pivotSlot, sp.comp.Fields())
		if err != nil {
			return 0, err
		}
		if cmp < 0 {
			i++
			if err := rp.Swap(i, j); err != nil {
				return 0, err
			}
		}
	}
	if err := rp.Swap(i+1, pivotSlot); err != nil {
		return 0, err
	}
	return i + 1, nil
}

func (sp *OneBufferSortPlan) copyFromRecordPageToRun(rp *record.RecordPage, dest query.UpdateScan) error {
	slot := int32(-1)
	if err := dest.BeforeFirst(); err != nil {
		return err
	}
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
	return nil
}

func (sp *OneBufferSortPlan) doAMergeIteration(runs []*TempTable) []*TempTable {
	result := []*TempTable{}
	for len(runs) > 1 {
		p1 := runs[0]
		p2 := runs[1]
		runs = runs[2:]
		merged, err := sp.mergeTwoRuns(p1, p2)
		if err != nil {
			return nil
		}
		result = append(result, merged)
	}
	if len(runs) == 1 {
		result = append(result, runs[0])
	}
	return result
}

func (sp *OneBufferSortPlan) mergeTwoRuns(p1, p2 *TempTable) (*TempTable, error) {
	src1, err := p1.Open()
	if err != nil {
		return nil, err
	}
	src2, err := p2.Open()
	if err != nil {
		return nil, err
	}

	result := NewTempTable(sp.tx, sp.sch)
	dest, err := result.Open()
	if err != nil {
		return nil, err
	}

	hasMore1, err := src1.Next()
	if err != nil {
		return nil, err
	}
	hasMore2, err := src2.Next()
	if err != nil {
		return nil, err
	}
	for hasMore1 && hasMore2 {
		cmp, err := sp.comp.Compare(src1, src2)
		if err != nil {
			return nil, err
		}
		if cmp < 0 {
			hasMore1, err = sp.copy(src1, dest)
			if err != nil {
				return nil, err
			}
		} else {
			hasMore2, err = sp.copy(src2, dest)
			if err != nil {
				return nil, err
			}
		}
	}

	if hasMore1 {
		for hasMore1 {
			hasMore1, err = sp.copy(src1, dest)
			if err != nil {
				return nil, err
			}
		}
	} else {
		for hasMore2 {
			hasMore2, err = sp.copy(src2, dest)
			if err != nil {
				return nil, err
			}
		}
	}

	src1.Close()
	src2.Close()
	dest.Close()
	return result, nil
}

func (sp *OneBufferSortPlan) copy(src query.Scan, dest query.UpdateScan) (bool, error) {
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
