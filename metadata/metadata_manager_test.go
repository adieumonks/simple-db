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

func TestMetadataManager(t *testing.T) {
	db, _ := server.NewSimpleDB(path.Join(t.TempDir(), "metadatamanagertest"), 400, 8)
	tx, err := db.NewTransaction()
	if err != nil {
		t.Fatalf("failed to create new transaction: %v", err)
	}
	mm, err := metadata.NewMetadataManager(true, tx)
	if err != nil {
		t.Fatalf("failed to create table manager: %v", err)
	}

	schema := record.NewSchema()
	schema.AddIntField("A")
	schema.AddStringField("B", 9)

	// Part 1: Table Metadata
	err = mm.CreateTable("MyTable", schema, tx)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	layout, err := mm.GetLayout("MyTable", tx)
	if err != nil {
		t.Fatalf("failed to get layout: %v", err)
	}
	slotSize := layout.SlotSize()
	schema2 := layout.Schema()
	t.Logf("MyTable has slot size %d", slotSize)
	t.Log("Its fiedls are:")
	for _, fieldName := range schema2.Fields() {
		var fieldType string
		if schema2.Type(fieldName) == record.INTEGER {
			fieldType = "int"
		} else {
			fieldType = fmt.Sprintf("varchar(%d)", schema2.Length(fieldName))
		}
		t.Logf("%s: %s", fieldName, fieldType)
	}

	// Part 2: Statistics Metadata
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
	si, err := mm.GetStatInfo("MyTable", layout, tx)
	if err != nil {
		t.Fatalf("failed to get stat info: %v", err)
	}
	t.Logf("B(MyTable) = %d", si.BlocksAccessed())
	t.Logf("R(MyTable) = %d", si.RecordsOutput())
	t.Logf("V(MyTable, A) = %d", si.DistinctValues("A"))
	t.Logf("V(MyTable, B) = %d", si.DistinctValues("B"))

	// Part 3: View Metadata
	viewDef := "SELECT A, B FROM MyTable WHERE A = 1"
	err = mm.CreateView("viewA", viewDef, tx)
	if err != nil {
		t.Fatalf("failed to create view: %v", err)
	}
	viewDef2, err := mm.GetViewDef("viewA", tx)
	if err != nil {
		t.Fatalf("failed to get view def: %v", err)
	}
	t.Logf("view def = %s", viewDef2)

	// Part 4: Index Metadata
	err = mm.CreateIndex("indexA", "MyTable", "A", tx)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}
	err = mm.CreateIndex("indexB", "MyTable", "B", tx)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}
	indexMap, err := mm.GetIndexInfo("MyTable", tx)
	if err != nil {
		t.Fatalf("failed to get index info: %v", err)
	}
	ii, ok := indexMap["indexA"]
	if !ok {
		t.Fatalf("indexA not found")
	}
	t.Logf("B(indexA) = %d", ii.BlocksAccessed())
	t.Logf("R(indexA) = %d", ii.RecordsOutput())
	t.Logf("V(indexA, A) = %d", ii.DistinctValues("A"))
	t.Logf("V(indexA, B) = %d", ii.DistinctValues("B"))

	ii, ok = indexMap["indexB"]
	if !ok {
		t.Fatalf("indexB not found")
	}
	t.Logf("B(indexB) = %d", ii.BlocksAccessed())
	t.Logf("R(indexB) = %d", ii.RecordsOutput())
	t.Logf("V(indexB, A) = %d", ii.DistinctValues("A"))
	t.Logf("V(indexB, B) = %d", ii.DistinctValues("B"))

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}
