package record_test

import (
	"math"
	"math/rand"
	"path"
	"testing"

	"github.com/adieumonks/simple-db/record"
	"github.com/adieumonks/simple-db/server"
)

func TestTableScan(t *testing.T) {
	db, _ := server.NewSimpleDB(path.Join(t.TempDir(), "tabletest"), 400, 8)
	tx, err := db.NewTransaction()
	if err != nil {
		t.Fatalf("failed to create new transaction: %v", err)
	}

	sch := record.NewSchema()
	sch.AddIntField("A")
	sch.AddStringField("B", 9)
	layout := record.NewLayoutFromSchema(sch)
	for _, fieldName := range layout.Schema().Fields() {
		offset := layout.Offset(fieldName)
		t.Logf("%s has offset %d\n", fieldName, offset)
	}

	t.Log("filling the table with 50 random records.")
	ts, err := record.NewTableScan(tx, "T", layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}

	for i := 0; i < 50; i++ {
		err := ts.Insert()
		if err != nil {
			t.Fatalf("failed to insert record: %v", err)
		}

		n := int32(math.Round(rand.Float64() * 50))

		err = ts.SetInt("A", n)
		if err != nil {
			t.Fatalf("failed to set int: %v", err)
		}

		err = ts.SetString("B", "rec"+string(n))
		if err != nil {
			t.Fatalf("failed to set string: %v", err)
		}

		t.Logf("inserting into slot %v: {%d, rec%d}\n", ts.GetRID(), n, n)
	}

	t.Logf("deleting these records, whose A-values are less than 25.")
	count := int32(0)
	if err := ts.BeforeFirst(); err != nil {
		t.Fatalf("failed to move to before first: %v", err)
	}
	for {
		hasNext, err := ts.Next()
		if err != nil {
			t.Fatalf("failed to move to next record: %v", err)
		}
		if !hasNext {
			break
		}

		a, err := ts.GetInt("A")
		if err != nil {
			t.Fatalf("failed to get int: %v", err)
		}
		b, err := ts.GetString("B")
		if err != nil {
			t.Fatalf("failed to get string: %v", err)
		}
		if a < 25 {
			count++
			t.Logf("slot %v: {%d, %s} will be deleted\n", ts.GetRID(), a, b)
			err := ts.Delete()
			if err != nil {
				t.Fatalf("failed to delete record: %v", err)
			}
		}
	}
	t.Logf("%d values under 25 were deleted\n", count)

	t.Log("Here are the remianing records.")
	if err := ts.BeforeFirst(); err != nil {
		t.Fatalf("failed to move to before first: %v", err)
	}
	for {
		hasNext, err := ts.Next()
		if err != nil {
			t.Fatalf("failed to move to next record: %v", err)
		}
		if !hasNext {
			break
		}

		a, err := ts.GetInt("A")
		if err != nil {
			t.Fatalf("failed to get int: %v", err)
		}
		b, err := ts.GetString("B")
		if err != nil {
			t.Fatalf("failed to get string: %v", err)
		}
		t.Logf("slot %v: {%d, %s}\n", ts.GetRID(), a, b)
	}

	ts.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}
