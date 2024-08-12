package plan_test

import (
	"fmt"
	"math"
	"math/rand/v2"
	"path"
	"testing"

	"github.com/adieumonks/simple-db/server"
)

func TestQueryPlanner(t *testing.T) {
	db, err := server.NewSimpleDBWithMetadata(path.Join(t.TempDir(), "queryplannertest"))
	if err != nil {
		t.Fatalf("failed to create new database: %v", err)
	}

	tx, err := db.NewTransaction()
	if err != nil {
		t.Fatalf("failed to create new transaction: %v", err)
	}

	planner := db.Planner()

	command := "create table T1(A int, B varchar(9))"
	_, err = planner.ExecuteUpdate(command, tx)
	if err != nil {
		t.Fatalf("failed to execute update: %v", err)
	}

	n := 200
	t.Logf("inserting %d records into T1.", n)
	for i := 0; i < n; i++ {
		a := int32(math.Round(rand.Float64() * 50))
		b := fmt.Sprintf("rec%d", a)
		command := fmt.Sprintf("insert into T1(A, B) values(%d, '%s')", a, b)
		_, err = planner.ExecuteUpdate(command, tx)
		if err != nil {
			t.Fatalf("failed to execute update: %v", err)
		}
	}

	query := "select B from T1 where A=10"
	p, err := planner.CreateQueryPlan(query, tx)
	if err != nil {
		t.Fatalf("failed to create query plan: %v", err)
	}
	s, err := p.Open()
	if err != nil {
		t.Fatalf("failed to open scan: %v", err)
	}

	for {
		next, err := s.Next()
		if err != nil {
			t.Fatalf("failed to get next scan: %v", err)
		}
		if !next {
			break
		}
		b, err := s.GetString("b")
		if err != nil {
			t.Fatalf("failed to get string: %v", err)
		}
		if b != "rec10" {
			t.Fatalf("unexpected value for B: %s", b)
		}
	}

	s.Close()

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit transaction: %v", err)
	}
}

func TestUpdatePlanner(t *testing.T) {
	db, err := server.NewSimpleDBWithMetadata(path.Join(t.TempDir(), "queryplannertest"))
	if err != nil {
		t.Fatalf("failed to create new database: %v", err)
	}

	tx, err := db.NewTransaction()
	if err != nil {
		t.Fatalf("failed to create new transaction: %v", err)
	}

	planner := db.Planner()

	n := 200

	command1 := "create table T1(A int, B varchar(9))"
	_, err = planner.ExecuteUpdate(command1, tx)
	if err != nil {
		t.Fatalf("failed to execute update: %v", err)
	}

	t.Logf("inserting %d records into T1.", n)
	for i := 0; i < n; i++ {
		a := int32(i)
		b := fmt.Sprintf("%d", a)
		command := fmt.Sprintf("insert into T1(A, B) values(%d, '%s')", a, b)
		_, err = planner.ExecuteUpdate(command, tx)
		if err != nil {
			t.Fatalf("failed to execute update: %v", err)
		}
	}

	command2 := "create table T2(C int, D varchar(9))"
	_, err = planner.ExecuteUpdate(command2, tx)
	if err != nil {
		t.Fatalf("failed to execute update: %v", err)
	}

	t.Logf("inserting %d records into T2.", n)
	for i := 0; i < n; i++ {
		c := int32(n - i - 1)
		d := fmt.Sprintf("%d", c)
		command := fmt.Sprintf("insert into T2(C, D) values(%d, '%s')", c, d)
		_, err = planner.ExecuteUpdate(command, tx)
		if err != nil {
			t.Fatalf("failed to execute update: %v", err)
		}
	}

	query := "select B, D from T1, T2 where A=C"
	p, err := planner.CreateQueryPlan(query, tx)
	if err != nil {
		t.Fatalf("failed to create query plan: %v", err)
	}
	s, err := p.Open()
	if err != nil {
		t.Fatalf("failed to open scan: %v", err)
	}

	for {
		next, err := s.Next()
		if err != nil {
			t.Fatalf("failed to get next scan: %v", err)
		}
		if !next {
			break
		}
		b, err := s.GetString("b")
		if err != nil {
			t.Fatalf("failed to get string: %v", err)
		}
		d, err := s.GetString("d")
		if err != nil {
			t.Fatalf("failed to get string: %v", err)
		}
		if b != d {
			t.Fatalf("unexpected value: b=%s, d=%s", b, d)
		}
	}

	s.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit transaction: %v", err)
	}
}
