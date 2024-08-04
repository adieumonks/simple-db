package metadata_test

import (
	"path"
	"testing"

	"github.com/adieumonks/simple-db/metadata"
	"github.com/adieumonks/simple-db/record"
	"github.com/adieumonks/simple-db/server"
)

func TestCatalog(t *testing.T) {
	db, _ := server.NewSimpleDB(path.Join(t.TempDir(), "catalogtest"), 400, 8)
	tx, err := db.NewTransaction()
	if err != nil {
		t.Fatalf("failed to create new transaction: %v", err)
	}
	tm, err := metadata.NewTableManager(true, tx)
	if err != nil {
		t.Fatalf("failed to create table manager: %v", err)
	}

	tcatLayout, err := tm.GetLayout("tblcat", tx)
	if err != nil {
		t.Fatalf("failed to get table layout: %v", err)
	}

	t.Log("Here are all the tables and their lengths.")
	ts, err := record.NewTableScan(tx, "tblcat", tcatLayout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}
	next, err := ts.Next()
	if err != nil {
		t.Fatalf("failed to get next record: %v", err)
	}
	for next {
		tableName, err := ts.GetString("tblname")
		if err != nil {
			t.Fatalf("failed to get string: %v", err)
		}
		slotSize, err := ts.GetInt("slotsize")
		if err != nil {
			t.Fatalf("failed to get int: %v", err)
		}
		t.Logf("%s %d", tableName, slotSize)
		next, err = ts.Next()
		if err != nil {
			t.Fatalf("failed to get next record: %v", err)
		}
	}
	ts.Close()

	t.Log("Here are the fields for each table and their offsets")
	fcatLayout, err := tm.GetLayout("fldcat", tx)
	if err != nil {
		t.Fatalf("failed to get table layout: %v", err)
	}
	ts, err = record.NewTableScan(tx, "fldcat", fcatLayout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}
	next, err = ts.Next()
	if err != nil {
		t.Fatalf("failed to get next record: %v", err)
	}
	for next {
		tableName, err := ts.GetString("tblname")
		if err != nil {
			t.Fatalf("failed to get string: %v", err)
		}
		fieldName, err := ts.GetString("fldname")
		if err != nil {
			t.Fatalf("failed to get string: %v", err)
		}
		offset, err := ts.GetInt("offset")
		if err != nil {
			t.Fatalf("failed to get int: %v", err)
		}
		t.Logf("%s %s %d", tableName, fieldName, offset)
		next, err = ts.Next()
		if err != nil {
			t.Fatalf("failed to get next record: %v", err)
		}
	}
	ts.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}
