package index_test

import (
	"fmt"
	"path"
	"testing"

	"github.com/adieumonks/simple-db/plan"
	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/server"
)

func TestIndexQuery(t *testing.T) {
	db, err := server.NewSimpleDBWithMetadata(path.Join(t.TempDir(), "indexquerytest"))
	if err != nil {
		t.Fatalf("failed to create new database: %v", err)
	}

	tx, err := db.NewTransaction()
	if err != nil {
		t.Fatalf("failed to create new transaction: %v", err)
	}

	mdm := db.MetadataManager()

	// create table
	planner := db.Planner()

	command := "create table t1(a int, b varchar(9), c int)"
	_, err = planner.ExecuteUpdate(command, tx)
	if err != nil {
		t.Fatalf("failed to execute update: %v", err)
	}

	// create index
	command = "create index idxc on t1(c)"
	_, err = planner.ExecuteUpdate(command, tx)
	if err != nil {
		t.Fatalf("failed to execute update: %v", err)
	}

	indexes, err := mdm.GetIndexInfo("t1", tx)
	if err != nil {
		t.Fatalf("failed to get index info: %v", err)
	}
	ii, ok := indexes["idxc"]
	if !ok {
		t.Fatalf("index idxc not found")
	}
	idx := ii.Open()

	p, err := plan.NewTablePlan(tx, "t1", mdm)
	if err != nil {
		t.Fatalf("failed to create table plan: %v", err)
	}
	s, err := p.Open()
	if err != nil {
		t.Fatalf("failed to open scan: %v", err)
	}
	us, ok := s.(query.UpdateScan)
	if !ok {
		t.Fatalf("expected update scan, got %T", s)
	}

	n := 100
	t.Logf("inserting %d records into t1.", n)
	for i := 0; i < n; i++ {
		a := int32(i)
		b := fmt.Sprintf("rec%d", a)
		c := a % 10

		t.Logf("inserting record: a: %d, b: %s, c: %d", a, b, c)

		if err := us.Insert(); err != nil {
			t.Fatalf("failed to insert record: %v", err)
		}
		if err := us.SetInt("a", a); err != nil {
			t.Fatalf("failed to set int: %v", err)
		}
		if err := us.SetString("b", b); err != nil {
			t.Fatalf("failed to set string: %v", err)
		}
		if err := us.SetInt("c", c); err != nil {
			t.Fatalf("failed to set int: %v", err)
		}

		dataRID := us.GetRID()
		dataVal, err := us.GetVal("c")
		if err != nil {
			t.Fatalf("failed to get data value: %v", err)
		}
		if err := idx.Insert(dataVal, dataRID); err != nil {
			t.Fatalf("failed to insert index: %v", err)
		}
	}

	// query with index
	if err := idx.BeforeFirst(query.NewConstantWithInt(3)); err != nil {
		t.Fatalf("failed to move to before first: %v", err)
	}

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
			t.Fatalf("failed to get data RID: %v", err)
		}

		if err := us.MoveToRID(rid); err != nil {
			t.Fatalf("failed to move to RID: %v", err)
		}

		c, err := us.GetInt("c")
		if err != nil {
			t.Fatalf("failed to get int: %v", err)
		}
		if c != 3 {
			t.Fatalf("expected 3, got %d", c)
		}

		a, err := us.GetInt("a")
		if err != nil {
			t.Fatalf("failed to get int: %v", err)
		}
		if a%10 != 3 {
			t.Fatalf("expected 3, got %d", a%10)
		}

		t.Logf("a: %d, c: %d", a, c)
	}

	idx.Close()
	s.Close()

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit transaction: %v", err)
	}
}
