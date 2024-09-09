package plan_test

import (
	"fmt"
	"math"
	"math/rand/v2"
	"path"
	"testing"

	"github.com/adieumonks/simple-db/plan"
	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/server"
	"github.com/adieumonks/simple-db/tx"
)

func TestIndexJoin(t *testing.T) {
	db, err := server.NewSimpleDBWithMetadata(path.Join(t.TempDir(), "indexjointest"))
	if err != nil {
		t.Fatalf("failed to create new database: %v", err)
	}

	tx, err := db.NewTransaction()
	if err != nil {
		t.Fatalf("failed to create new transaction: %v", err)
	}

	prepare_tables_for_index_join(db, tx, t)

	// index join plan
	mdm := db.MetadataManager()
	indexes, err := mdm.GetIndexInfo("t2", tx)
	if err != nil {
		t.Fatalf("failed to get index info: %v", err)
	}
	ii, ok := indexes["idxa"]
	if !ok {
		t.Fatalf("index idxa not found")
	}

	p1, err := plan.NewTablePlan(tx, "t1", mdm)
	if err != nil {
		t.Fatalf("failed to create table plan: %v", err)
	}
	p2, err := plan.NewTablePlan(tx, "t2", mdm)
	if err != nil {
		t.Fatalf("failed to create table plan: %v", err)
	}

	p := plan.NewIndexJoinPlan(p1, p2, ii, "a")
	s, err := p.Open()
	if err != nil {
		t.Fatalf("failed to open index join plan: %v", err)
	}

	for {
		next, err := s.Next()
		if err != nil {
			t.Fatalf("failed to get next: %v", err)
		}
		if !next {
			break
		}

		b, err := s.GetVal("b")
		if err != nil {
			t.Fatalf("failed to get value: %v", err)
		}
		c, err := s.GetVal("c")
		if err != nil {
			t.Fatalf("failed to get value: %v", err)
		}
		if !b.Equals(c) {
			t.Fatalf("b != c: %v != %v", b, c)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit transaction: %v", err)
	}
}

func prepare_tables_for_index_join(db *server.SimpleDB, tx *tx.Transaction, t *testing.T) {
	planner := db.Planner()

	// create t1 table
	command := "create table t1(a int, b varchar(9))"
	_, err := planner.ExecuteUpdate(command, tx)
	if err != nil {
		t.Fatalf("failed to execute update: %v", err)
	}

	mdm := db.MetadataManager()

	layout, err := mdm.GetLayout("t1", tx)
	if err != nil {
		t.Fatalf("failed to get layout: %v", err)
	}

	ts, err := query.NewTableScan(tx, "t1", layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}

	n := 100
	t.Logf("inserting %d records into t1.", n)
	for i := 0; i < n; i++ {
		a := int32(math.Round(rand.Float64() * 50))
		b := fmt.Sprintf("%d", a)

		if err := ts.Insert(); err != nil {
			t.Fatalf("failed to insert record: %v", err)
		}
		if err := ts.SetInt("a", a); err != nil {
			t.Fatalf("failed to set int: %v", err)
		}
		if err := ts.SetString("b", b); err != nil {
			t.Fatalf("failed to set string: %v", err)
		}
	}

	// create t2 table
	command = "create table t2(a int, c varchar(9))"
	_, err = planner.ExecuteUpdate(command, tx)
	if err != nil {
		t.Fatalf("failed to execute update: %v", err)
	}

	// create index
	command = "create index idxa on t2(a)"
	_, err = planner.ExecuteUpdate(command, tx)
	if err != nil {
		t.Fatalf("failed to execute update: %v", err)
	}

	indexes, err := mdm.GetIndexInfo("t2", tx)
	if err != nil {
		t.Fatalf("failed to get index info: %v", err)
	}

	ii, ok := indexes["idxa"]
	if !ok {
		t.Fatalf("index idxa not found")
	}

	idx := ii.Open()

	layout, err = mdm.GetLayout("t2", tx)
	if err != nil {
		t.Fatalf("failed to get layout: %v", err)
	}

	ts, err = query.NewTableScan(tx, "t2", layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}

	t.Logf("inserting %d records into t2.", n)
	for i := 0; i < n; i++ {
		a := int32(math.Round(rand.Float64() * 50))
		c := fmt.Sprintf("%d", a)

		if err := ts.Insert(); err != nil {
			t.Fatalf("failed to insert record: %v", err)
		}
		if err := ts.SetInt("a", a); err != nil {
			t.Fatalf("failed to set int: %v", err)
		}
		if err := ts.SetString("c", c); err != nil {
			t.Fatalf("failed to set string: %v", err)
		}

		rid := ts.GetRID()

		if err := idx.Insert(query.NewConstantWithInt(a), rid); err != nil {
			t.Fatalf("failed to insert index: %v", err)
		}
	}
}
