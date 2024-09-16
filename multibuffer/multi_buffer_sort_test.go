package multibuffer_test

import (
	"fmt"
	"math"
	"math/rand/v2"
	"path"
	"testing"

	"github.com/adieumonks/simple-db/multibuffer"
	"github.com/adieumonks/simple-db/plan"
	"github.com/adieumonks/simple-db/server"
)

func TestMultiBufferSort(t *testing.T) {
	db, err := server.NewSimpleDBWithMetadata(path.Join(t.TempDir(), "multibuffersorttest"))
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

	n := 2000
	t.Logf("inserting %d records into t1.", n)
	for i := 0; i < n; i++ {
		a := int32(math.Round(rand.Float64() * 50))
		b := fmt.Sprintf("rec%d", a)
		command := fmt.Sprintf("insert into t1(a, b) values(%d, '%s')", a, b)
		_, err = planner.ExecuteUpdate(command, tx)
		if err != nil {
			t.Fatalf("failed to execute update: %v", err)
		}
	}

	// sort
	mdm := db.MetadataManager()
	tp, err := plan.NewTablePlan(tx, "t1", mdm)
	if err != nil {
		t.Fatalf("failed to create table plan: %v", err)
	}
	sp := multibuffer.NewMultiBufferSortPlan(tx, tp, []string{"a"})

	s, err := sp.Open()
	if err != nil {
		t.Fatalf("failed to open scan: %v", err)
	}

	if err := s.BeforeFirst(); err != nil {
		t.Fatalf("failed to before first: %v", err)
	}
	next, err := s.Next()
	if err != nil {
		t.Fatalf("failed to get next scan: %v", err)
	}
	if !next {
		t.Fatalf("expected next")
	}
	val, err := s.GetInt("a")
	if err != nil {
		t.Fatalf("failed to get int: %v", err)
	}
	prev := val
	for {
		next, err := s.Next()
		if err != nil {
			t.Fatalf("failed to get next scan: %v", err)
		}
		if !next {
			break
		}
		val, err := s.GetInt("a")
		if err != nil {
			t.Fatalf("failed to get int: %v", err)
		}
		if val < prev {
			t.Fatalf("expected %d >= %d", val, prev)
		}
		t.Logf("a: %d", val)
		prev = val
	}

	s.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit transaction: %v", err)
	}
}
