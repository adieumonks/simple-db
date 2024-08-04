package metadata_test

import (
	"path"
	"testing"

	"github.com/adieumonks/simple-db/metadata"
	"github.com/adieumonks/simple-db/record"
	"github.com/adieumonks/simple-db/server"
)

func TestTableManager(t *testing.T) {
	db, _ := server.NewSimpleDB(path.Join(t.TempDir(), "tablemanagertest"), 400, 8)
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
	if err := tm.CreateTable("MyTable", schema, tx); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	layout, err := tm.GetLayout("MyTable", tx)
	if err != nil {
		t.Fatalf("failed to get layout: %v", err)
	}
	slotSize := layout.SlotSize()
	schema2 := layout.Schema()
	t.Logf("MyTable has slot size %d\n", slotSize)
	t.Logf("Its fields are:")
	for _, fieldName := range schema2.Fields() {
		var fieldType string
		if schema2.Type(fieldName) == record.INTEGER {
			fieldType = "INT"
		} else {
			fieldType = "STRING"
		}
		t.Logf("  %s: %s", fieldName, fieldType)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}
