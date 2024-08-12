package metadata_test

import (
	"path"
	"testing"

	"github.com/adieumonks/simple-db/metadata"
	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/server"
)

func TestViewManager(t *testing.T) {
	db, _ := server.NewSimpleDB(path.Join(t.TempDir(), "viewmanagertest"), 400, 8)
	tx, err := db.NewTransaction()
	if err != nil {
		t.Fatalf("failed to create new transaction: %v", err)
	}
	tm, err := metadata.NewTableManager(true, tx)
	if err != nil {
		t.Fatalf("failed to create table manager: %v", err)
	}

	vm, err := metadata.NewViewManager(true, tm, tx)
	if err != nil {
		t.Fatalf("failed to create view manager: %v", err)
	}

	if err := vm.CreateView("MyView", "SELECT A, B FROM MyTable", tx); err != nil {
		t.Fatalf("failed to create view: %v", err)
	}

	layout, err := tm.GetLayout("viewcat", tx)
	if err != nil {
		t.Fatalf("failed to get layout: %v", err)
	}

	ts, err := query.NewTableScan(tx, "viewcat", layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}

	next, err := ts.Next()
	if err != nil {
		t.Fatalf("failed to get next: %v", err)
	}
	if !next {
		t.Fatalf("no records")
	}

	viewName, err := ts.GetString("viewname")
	if err != nil {
		t.Fatalf("failed to get string: %v", err)
	}
	if viewName != "MyView" {
		t.Fatalf("unexpected view name: %s", viewName)
	}
	t.Logf("viewName: %s", viewName)
	viewDef, err := ts.GetString("viewdef")
	if err != nil {
		t.Fatalf("failed to get string: %v", err)
	}
	if viewDef != "SELECT A, B FROM MyTable" {
		t.Fatalf("unexpected view def: %s", viewDef)
	}
	t.Logf("viewDef: %s", viewDef)

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}
