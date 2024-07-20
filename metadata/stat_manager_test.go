package metadata_test

import (
	"fmt"
	"math"
	"math/rand/v2"
	"path"
	"testing"

	"github.com/adieumonks/simple-db/metadata"
	"github.com/adieumonks/simple-db/record"
	"github.com/adieumonks/simple-db/server"
)

func TestStatManager(t *testing.T) {
	db, _ := server.NewSimpleDB(path.Join(t.TempDir(), "statmanagertest"), 400, 8)
	tx := db.NewTransaction()
	tm, err := metadata.NewTableManager(true, tx)
	if err != nil {
		t.Fatalf("failed to create table manager: %v", err)
	}

	schema := record.NewSchema()
	schema.AddIntField("A")
	schema.AddStringField("B", 9)

	err = tm.CreateTable("MyTable", schema, tx)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	layout, err := tm.GetLayout("MyTable", tx)
	if err != nil {
		t.Fatalf("failed to get layout: %v", err)
	}

	ts, err := record.NewTableScan(tx, "MyTable", layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}

	for i := 0; i < 50; i++ {
		ts.Insert()
		n := int32(math.Round(rand.Float64() * 50))
		ts.SetInt("A", n)
		ts.SetString("B", fmt.Sprintf("rec%d", n))
	}

	sm, err := metadata.NewStatManager(tm, tx)
	if err != nil {
		t.Fatalf("failed to create stat manager: %v", err)
	}

	si, err := sm.GetStatInfo("MyTable", layout, tx)
	if err != nil {
		t.Fatalf("failed to get stat info: %v", err)
	}

	t.Logf("B(MyTable) = %d", si.BlocksAccessed())
	t.Logf("R(MyTable) = %d", si.RecordsOutput())
	t.Logf("V(MyTable, A) = %d", si.DistinctValues("A"))
	t.Logf("V(MyTable, B) = %d", si.DistinctValues("B"))

	tx.Commit()
}
