package plan_test

import (
	"fmt"
	"math"
	"math/rand/v2"
	"path"
	"testing"

	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/server"
	"github.com/adieumonks/simple-db/tx"
)

func TestIndexRetrieval(t *testing.T) {
	db, err := server.NewSimpleDBWithMetadata(path.Join(t.TempDir(), "indexretrievaltest"))
	if err != nil {
		t.Fatalf("failed to create new database: %v", err)
	}

	tx, err := db.NewTransaction()
	if err != nil {
		t.Fatalf("failed to create new transaction: %v", err)
	}

	initialize(db, tx, t)

	mdm := db.MetadataManager()
	indexes, err := mdm.GetIndexInfo("t1", tx)
	if err != nil {
		t.Fatalf("failed to get index info: %v", err)
	}
	ii, ok := indexes["b"]
	if !ok {
		t.Fatalf("index idxb not found")
	}
	idx := ii.Open()

	layout, err := mdm.GetLayout("t1", tx)
	if err != nil {
		t.Fatalf("failed to get layout: %v", err)
	}
	ts, err := query.NewTableScan(tx, "t1", layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}

	// query with index
	idx.BeforeFirst(query.NewConstantWithInt(3))
	for {
		next, err := idx.Next()
		if err != nil {
			t.Fatalf("failed to get next index: %v", err)
		}
		if !next {
			break
		}

		rid, err := idx.GetDataRID()
		if err != nil {
			t.Fatalf("failed to get data rid: %v", err)
		}

		if err := ts.MoveToRID(rid); err != nil {
			t.Fatalf("failed to move to rid: %v", err)
		}

		b, err := ts.GetInt("b")
		if err != nil {
			t.Fatalf("failed to get int: %v", err)
		}

		if b != 3 {
			t.Fatalf("unexpected value for B: %d", b)
		}
	}

	idx.Close()
	ts.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit transaction: %v", err)
	}
}

func TestIndexDelete(t *testing.T) {
	db, err := server.NewSimpleDBWithMetadata(path.Join(t.TempDir(), "indexdeletetest"))
	if err != nil {
		t.Fatalf("failed to create new database: %v", err)
	}

	tx, err := db.NewTransaction()
	if err != nil {
		t.Fatalf("failed to create new transaction: %v", err)
	}

	initialize(db, tx, t)

	mdm := db.MetadataManager()
	indexes, err := mdm.GetIndexInfo("t1", tx)
	if err != nil {
		t.Fatalf("failed to get index info: %v", err)
	}
	ii, ok := indexes["b"]
	if !ok {
		t.Fatalf("index idxb not found")
	}
	idx := ii.Open()

	layout, err := mdm.GetLayout("t1", tx)
	if err != nil {
		t.Fatalf("failed to get layout: %v", err)
	}
	ts, err := query.NewTableScan(tx, "t1", layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}

	// delete record where b=3
	if err := ts.BeforeFirst(); err != nil {
		t.Fatalf("failed to move to first record: %v", err)
	}
	for {
		next, err := ts.Next()
		if err != nil {
			t.Fatalf("failed to get next index: %v", err)
		}
		if !next {
			break
		}

		b, err := ts.GetInt("b")
		if err != nil {
			t.Fatalf("failed to get int: %v", err)
		}
		if b != 3 {
			continue
		}

		rid := ts.GetRID()
		if err := idx.Delete(query.NewConstantWithInt(b), rid); err != nil {
			t.Fatalf("failed to delete index: %v", err)
		}
		if err := ts.Delete(); err != nil {
			t.Fatalf("failed to delete record: %v", err)
		}
	}

	// confirm that the record where b=3 has been deleted
	if err := ts.BeforeFirst(); err != nil {
		t.Fatalf("failed to move to first record: %v", err)
	}
	for {
		next, err := ts.Next()
		if err != nil {
			t.Fatalf("failed to get next index: %v", err)
		}
		if !next {
			break
		}

		b, err := ts.GetInt("b")
		if err != nil {
			t.Fatalf("failed to get int: %v", err)
		}
		if b == 3 {
			t.Fatalf("record with b=3 not deleted")
		}
	}

	// confirm that the index has been updated
	idx.BeforeFirst(query.NewConstantWithInt(3))
	next, err := idx.Next()
	if err != nil {
		t.Fatalf("failed to get next index: %v", err)
	}
	fmt.Println(next)
	if next {
		t.Fatalf("record with b=3 not deleted from index")
	}

	idx.Close()
	ts.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit transaction: %v", err)
	}
}

func initialize(db *server.SimpleDB, tx *tx.Transaction, t *testing.T) {
	planner := db.Planner()

	// create table
	command := "create table t1(a int, b int)"
	_, err := planner.ExecuteUpdate(command, tx)
	if err != nil {
		t.Fatalf("failed to execute update: %v", err)
	}

	// create index
	command = "create index idxb on t1(b)"
	_, err = planner.ExecuteUpdate(command, tx)
	if err != nil {
		t.Fatalf("failed to execute update: %v", err)
	}

	mdm := db.MetadataManager()
	indexes, err := mdm.GetIndexInfo("t1", tx)
	if err != nil {
		t.Fatalf("failed to get index info: %v", err)
	}

	ii, ok := indexes["b"]
	if !ok {
		t.Fatalf("index idxa not found")
	}

	idx := ii.Open()

	layout, err := mdm.GetLayout("t1", tx)
	if err != nil {
		t.Fatalf("failed to get layout: %v", err)
	}

	ts, err := query.NewTableScan(tx, "t1", layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}

	n := 100
	t.Logf("inserting %d records into T1.", n)
	for i := 0; i < n; i++ {
		a := int32(math.Round(rand.Float64() * 50))
		b := a % 10

		if err := ts.Insert(); err != nil {
			t.Fatalf("failed to insert record: %v", err)
		}
		if err := ts.SetInt("a", a); err != nil {
			t.Fatalf("failed to set int: %v", err)
		}
		if err := ts.SetInt("b", b); err != nil {
			t.Fatalf("failed to set string: %v", err)
		}

		rid := ts.GetRID()

		if err := idx.Insert(query.NewConstantWithInt(b), rid); err != nil {
			t.Fatalf("failed to insert index: %v", err)
		}
	}
}
