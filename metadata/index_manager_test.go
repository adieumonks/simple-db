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

func TestIndexManager(t *testing.T) {
	db, _ := server.NewSimpleDB(path.Join(t.TempDir(), "indexmanagertest"), 400, 8)
	tx, err := db.NewTransaction()
	if err != nil {
		t.Fatalf("failed to create new transaction: %v", err)
	}
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
		if err := ts.Insert(); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
		n := int32(math.Round(rand.Float64() * 50))
		if err := ts.SetInt("A", n); err != nil {
			t.Fatalf("failed to set int: %v", err)
		}
		if err := ts.SetString("B", fmt.Sprintf("rec%d", n)); err != nil {
			t.Fatalf("failed to set string: %v", err)
		}
	}

	sm, err := metadata.NewStatManager(tm, tx)
	if err != nil {
		t.Fatalf("failed to create stat manager: %v", err)
	}

	im, err := metadata.NewIndexManager(true, tm, sm, tx)
	if err != nil {
		t.Fatalf("failed to create index manager: %v", err)
	}

	err = im.CreateIndex("indexA", "MyTable", "A", tx)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}
	err = im.CreateIndex("indexB", "MyTable", "B", tx)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	indexMap, err := im.GetIndexInfo("MyTable", tx)
	if err != nil {
		t.Fatalf("failed to get index info: %v", err)
	}

	ii, ok := indexMap["indexA"]
	if !ok {
		t.Fatalf("index not found")
	}
	t.Logf("B(indexA) = %v", ii.BlocksAccessed())
	t.Logf("R(indexA) = %v", ii.RecordsOutput())
	t.Logf("V(indexA,A) = %v", ii.DistinctValues("A"))
	t.Logf("V(indexA,B) = %v", ii.DistinctValues("B"))

	ii, ok = indexMap["indexB"]
	if !ok {
		t.Fatalf("index not found")
	}
	t.Logf("B(indexB) = %v", ii.BlocksAccessed())
	t.Logf("R(indexB) = %v", ii.RecordsOutput())
	t.Logf("V(indexB,A) = %v", ii.DistinctValues("A"))
	t.Logf("V(indexB,B) = %v", ii.DistinctValues("B"))
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}
