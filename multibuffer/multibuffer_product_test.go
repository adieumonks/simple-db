package multibuffer_test

import (
	"fmt"
	"path"
	"testing"

	"github.com/adieumonks/simple-db/multibuffer"
	"github.com/adieumonks/simple-db/plan"
	"github.com/adieumonks/simple-db/server"
)

func TestMultibufferProduct(t *testing.T) {
	db, err := server.NewSimpleDBWithMetadata(path.Join(t.TempDir(), "multibufferproducttest"))
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

	n := 10
	t.Logf("inserting %d records into t1.", n)
	for i := 0; i < n; i++ {
		a := int32(i)
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
		c := int32(i)
		d := fmt.Sprintf("rec%d", c)
		command := fmt.Sprintf("insert into t2(c, d) values(%d, '%s')", c, d)
		_, err = planner.ExecuteUpdate(command, tx)
		if err != nil {
			t.Fatalf("failed to execute update: %v", err)
		}
	}

	// multi buffer product
	mdm := db.MetadataManager()
	p1, err := plan.NewTablePlan(tx, "t1", mdm)
	if err != nil {
		t.Fatalf("failed to create table plan: %v", err)
	}
	p2, err := plan.NewTablePlan(tx, "t2", mdm)
	if err != nil {
		t.Fatalf("failed to create table plan: %v", err)
	}
	p := multibuffer.NewMultibufferProductPlan(tx, p1, p2)

	s, err := p.Open()
	if err != nil {
		t.Fatalf("failed to open product scan: %v", err)
	}
	if err := s.BeforeFirst(); err != nil {
		t.Fatalf("failed to position to before first: %v", err)
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
		b, err := s.GetString("b")
		if err != nil {
			t.Fatalf("failed to get string: %v", err)
		}
		c, err := s.GetInt("c")
		if err != nil {
			t.Fatalf("failed to get int: %v", err)
		}
		d, err := s.GetString("d")
		if err != nil {
			t.Fatalf("failed to get string: %v", err)
		}
		t.Logf("a=%d, b=%s, c=%d, d=%s", a, b, c, d)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit transaction: %v", err)
	}
}
