package materialize_test

import (
	"fmt"
	"path"
	"testing"

	"github.com/adieumonks/simple-db/materialize"
	"github.com/adieumonks/simple-db/plan"
	"github.com/adieumonks/simple-db/server"
)

func TestMergeJoin(t *testing.T) {
	db, err := server.NewSimpleDBWithMetadata(path.Join(t.TempDir(), "mergejointest"))
	if err != nil {
		t.Fatalf("failed to create new database: %v", err)
	}

	tx, err := db.NewTransaction()
	if err != nil {
		t.Fatalf("failed to create new transaction: %v", err)
	}

	// create table
	planner := db.Planner()

	command := "create table t1(a int, b varchar(9))"
	_, err = planner.ExecuteUpdate(command, tx)
	if err != nil {
		t.Fatalf("failed to execute update: %v", err)
	}

	n := 100
	t.Logf("inserting %d records into t1.", n)
	for i := 0; i < n; i++ {
		a := int32(i / 10)
		b := fmt.Sprintf("rec%d", a)
		command := fmt.Sprintf("insert into t1(a, b) values(%d, '%s')", a, b)
		_, err = planner.ExecuteUpdate(command, tx)
		if err != nil {
			t.Fatalf("failed to execute update: %v", err)
		}
	}

	command = "create table t2(c int, d varchar(9))"
	_, err = planner.ExecuteUpdate(command, tx)
	if err != nil {
		t.Fatalf("failed to execute update: %v", err)
	}

	t.Logf("inserting %d records into t2.", n)
	for i := 0; i < n; i++ {
		c := int32((n - i - 1) / 10)
		d := fmt.Sprintf("rec%d", c)
		command := fmt.Sprintf("insert into t2(c, d) values(%d, '%s')", c, d)
		_, err = planner.ExecuteUpdate(command, tx)
		if err != nil {
			t.Fatalf("failed to execute update: %v", err)
		}
	}

	// merge join
	mdm := db.MetadataManager()
	p1, err := plan.NewTablePlan(tx, "t1", mdm)
	if err != nil {
		t.Fatalf("failed to create table plan: %v", err)
	}
	p2, err := plan.NewTablePlan(tx, "t2", mdm)
	if err != nil {
		t.Fatalf("failed to create table plan: %v", err)
	}
	p := materialize.NewMergeJoinPlan(tx, p1, p2, "a", "c")

	s, err := p.Open()
	if err != nil {
		t.Fatalf("failed to open scan: %v", err)
	}
	if err := s.BeforeFirst(); err != nil {
		t.Fatalf("failed to before first: %v", err)
	}

	preva := int32(0)
	count := int32(0)
	for {
		next, err := s.Next()
		if err != nil {
			t.Fatalf("failed to get next scan: %v", err)
		}
		if !next {
			break
		}

		a, err := s.GetInt("a")
		if err != nil {
			t.Fatalf("failed to get value: %v", err)
		}
		c, err := s.GetInt("c")
		if err != nil {
			t.Fatalf("failed to get value: %v", err)
		}
		if a != c {
			t.Fatalf("expected %d, but got %d", a, c)
		}
		if a == preva {
			count++
		} else {
			if count != 100 {
				t.Fatalf("expected 100, but got %d", count)
			}
			count = 1
			preva = a
		}
		t.Logf("a=%d, c=%d, count=%d", a, c, count)
	}
	if count != 100 {
		t.Fatalf("expected 100, but got %d", count)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit transaction: %v", err)
	}
}
