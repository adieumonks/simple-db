package record_test

import (
	"math"
	"math/rand"
	"path"
	"testing"

	"github.com/adieumonks/simple-db/record"
	"github.com/adieumonks/simple-db/server"
)

func TestRecord(t *testing.T) {
	db, _ := server.NewSimpleDB(path.Join(t.TempDir(), "recordtest"), 400, 8)
	tx := db.NewTransaction()

	sch := record.NewSchema()
	sch.AddIntField("A")
	sch.AddStringField("B", 9)
	layout := record.NewLayoutFromSchema(sch)
	for _, fieldName := range layout.Schema().Fields() {
		offset := layout.Offset(fieldName)
		t.Logf("%s has offset %d\n", fieldName, offset)
	}

	block, _ := tx.Append("testfile")
	tx.Pin(block)

	rp := record.NewRecordPage(tx, block, layout)
	rp.Format()

	t.Log("filling the page with random records.")
	slot, err := rp.InsertAfter(-1)
	if err != nil {
		t.Fatalf("failed to insert after -1: %v", err)
	}
	for slot >= 0 {
		n := int32(math.Round(rand.Float64() * 50))
		rp.SetInt(slot, "A", n)
		rp.SetString(slot, "B", "rec"+string(n))
		t.Logf("inserting into slot %d: {%d, rec%d}\n", slot, n, n)
		slot, err = rp.InsertAfter(slot)
		if err != nil {
			t.Fatalf("failed to insert after %d: %v", slot, err)
		}
	}

	t.Log("deleteing these records, whose A-values are less than 25.")
	count := 0
	slot, err = rp.NextAfter(-1)
	if err != nil {
		t.Fatalf("failed to get next after -1: %v", err)
	}
	for slot >= 0 {
		a, err := rp.GetInt(slot, "A")
		if err != nil {
			t.Fatalf("failed to get int: %v", err)
		}
		b, err := rp.GetString(slot, "B")
		if err != nil {
			t.Fatalf("failed to get string: %v", err)
		}
		if a < 25 {
			count++
			t.Logf("slot %d: {%d, %s}", slot, a, b)
			err = rp.Delete(slot)
			if err != nil {
				t.Fatalf("failed to delete slot %d: %v", slot, err)
			}
		}
		slot, err = rp.NextAfter(slot)
		if err != nil {
			t.Fatalf("failed to get next after %d: %v", slot, err)
		}
	}
	t.Logf("%d values under 25 were deleted.", count)

	t.Log("Here are the remaining records.")
	slot, err = rp.NextAfter(-1)
	if err != nil {
		t.Fatalf("failed to get next after -1: %v", err)
	}
	for slot >= 0 {
		a, err := rp.GetInt(slot, "A")
		if err != nil {
			t.Fatalf("failed to get int: %v", err)
		}
		b, err := rp.GetString(slot, "B")
		if err != nil {
			t.Fatalf("failed to get string: %v", err)
		}
		t.Logf("slot %d: {%d, %s}", slot, a, b)
		slot, err = rp.NextAfter(slot)
		if err != nil {
			t.Fatalf("failed to get next after %d: %v", slot, err)
		}
	}

	tx.Unpin(block)
	tx.Commit()
}
