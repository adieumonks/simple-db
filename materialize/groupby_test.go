package materialize_test

import (
	"fmt"
	"path"
	"testing"

	"github.com/adieumonks/simple-db/materialize"
	"github.com/adieumonks/simple-db/plan"
	"github.com/adieumonks/simple-db/server"
)

func TestGroupBy(t *testing.T) {
	db, err := server.NewSimpleDBWithMetadata(path.Join(t.TempDir(), "groupbytest"))
	if err != nil {
		t.Fatalf("failed to create new database: %v", err)
	}

	tx, err := db.NewTransaction()
	if err != nil {
		t.Fatalf("failed to create new transaction: %v", err)
	}

	// create table
	planner := db.Planner()

	command := "create table t1(a int, b int)"
	_, err = planner.ExecuteUpdate(command, tx)
	if err != nil {
		t.Fatalf("failed to execute update: %v", err)
	}

	n := 100
	t.Logf("inserting %d records into t1.", n)
	for i := 0; i < n; i++ {
		a := int32(i / 10)
		b := int32(i)
		command := fmt.Sprintf("insert into t1(a, b) values(%d, %d)", a, b)
		_, err = planner.ExecuteUpdate(command, tx)
		if err != nil {
			t.Fatalf("failed to execute update: %v", err)
		}
	}

	// group by
	mdm := db.MetadataManager()
	tp, err := plan.NewTablePlan(tx, "t1", mdm)
	if err != nil {
		t.Fatalf("failed to create table plan: %v", err)
	}
	p, err := materialize.NewGroupByPlan(
		tx,
		tp,
		[]string{"a"},
		[]materialize.AggregationFn{materialize.NewCountFn("b"), materialize.NewMaxFn("b")},
	)
	if err != nil {
		t.Fatalf("failed to create group by plan: %v", err)
	}
	s, err := p.Open()
	if err != nil {
		t.Fatalf("failed to open group by scan: %v", err)
	}
	if err := s.BeforeFirst(); err != nil {
		t.Fatalf("failed to before first: %v", err)
	}
	for {
		next, err := s.Next()
		if err != nil {
			t.Fatalf("failed to get next record: %v", err)
		}
		if !next {
			break
		}

		a, err := s.GetInt("a")
		if err != nil {
			t.Fatalf("failed to get int: %v", err)
		}

		countb, err := s.GetInt("countofb")
		if err != nil {
			t.Fatalf("failed to get count: %v", err)
		}

		maxb, err := s.GetInt("maxofb")
		if err != nil {
			t.Fatalf("failed to get max: %v", err)
		}

		t.Logf("a=%d, count=%d, max=%d", a, countb, maxb)

		if countb != 10 {
			t.Fatalf("expected count to be 10, got %d", countb)
		}

		if maxb != a*10+9 {
			t.Fatalf("expected max to be %d, got %d", a*10+9, maxb)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit transaction: %v", err)
	}
}
