package query_test

import (
	"fmt"
	"math"
	"math/rand/v2"
	"path"
	"testing"

	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/record"
	"github.com/adieumonks/simple-db/server"
)

func TestScan1(t *testing.T) {
	db, _ := server.NewSimpleDB(path.Join(t.TempDir(), "scantest1"), 400, 8)
	tx := db.NewTransaction()

	sch1 := record.NewSchema()
	sch1.AddIntField("A")
	sch1.AddStringField("B", 9)
	layout := record.NewLayoutFromSchema(sch1)
	s1, err := record.NewTableScan(tx, "T1", layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}

	if err := s1.BeforeFirst(); err != nil {
		t.Fatalf("failed to prepare for first")
	}
	n := 200
	t.Logf("inserting %d random records.", n)
	for i := 0; i < n; i++ {
		s1.Insert()
		k := int32(math.Round(rand.Float64() * 50))
		s1.SetInt("A", k)
		s1.SetString("B", fmt.Sprintf("rec%d", k))
	}
	s1.Close()

	s2, err := record.NewTableScan(tx, "T1", layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}
	c := record.NewConstantFromInt(10)
	term := query.NewTerm(query.NewExpressionFromField("A"), query.NewExpressionFromConstant(c))
	pred := query.NewPredicateFromTerm(term)
	t.Logf("the predicate is %v", pred)

	s3 := query.NewSelectScan(s2, pred)

	fields := []string{"B"}
	s4 := query.NewProjectScan(s3, fields)

	next, err := s4.Next()
	if err != nil {
		t.Fatalf("failed to get next record: %v", err)
	}
	for next {
		b, err := s4.GetString("B")
		if err != nil {
			t.Fatalf("failed to get string: %v", err)
		}
		t.Logf("B: %s", b)
		next, err = s4.Next()
		if err != nil {
			t.Fatalf("failed to get next record: %v", err)
		}
	}
	s4.Close()
	tx.Commit()
}

func TestScan2(t *testing.T) {
	db, _ := server.NewSimpleDB(path.Join(t.TempDir(), "scantest2"), 400, 8)
	tx := db.NewTransaction()

	sch1 := record.NewSchema()
	sch1.AddIntField("A")
	sch1.AddStringField("B", 9)
	layout1 := record.NewLayoutFromSchema(sch1)
	us1, _ := record.NewTableScan(tx, "T1", layout1)

	n := 200

	if err := us1.BeforeFirst(); err != nil {
		t.Fatalf("failed to prepare for first")
	}
	t.Logf("inserting %d random records int T1.", n)
	for i := 0; i < n; i++ {
		us1.Insert()
		us1.SetInt("A", int32(i))
		us1.SetString("B", fmt.Sprintf("bbb%d", i))
	}
	us1.Close()

	sch2 := record.NewSchema()
	sch2.AddIntField("C")
	sch2.AddStringField("D", 9)
	layout2 := record.NewLayoutFromSchema(sch2)
	us2, _ := record.NewTableScan(tx, "T2", layout2)
	if err := us2.BeforeFirst(); err != nil {
		t.Fatalf("failed to prepare for first")
	}
	t.Logf("inserting %d random records into T2.", n)
	for i := 0; i < n; i++ {
		us2.Insert()
		us2.SetInt("C", int32(n-i-1))
		us2.SetString("D", fmt.Sprintf("ddd%d", n-i-1))
	}
	us2.Close()

	s1, _ := record.NewTableScan(tx, "T1", layout1)
	s2, _ := record.NewTableScan(tx, "T2", layout2)
	s3 := query.NewProductScan(s1, s2)

	term := query.NewTerm(query.NewExpressionFromField("A"), query.NewExpressionFromField("C"))
	pred := query.NewPredicateFromTerm(term)
	t.Logf("the predicate is %v", pred)
	s4 := query.NewSelectScan(s3, pred)

	fields := []string{"B", "D"}
	s5 := query.NewProjectScan(s4, fields)

	next, err := s5.Next()
	if err != nil {
		t.Fatalf("failed to get next record: %v", err)
	}
	for next {
		b, err := s5.GetString("B")
		if err != nil {
			t.Fatalf("failed to get string: %v", err)
		}
		d, err := s5.GetString("D")
		if err != nil {
			t.Fatalf("failed to get string: %v", err)
		}
		t.Logf("B: %s, D: %s", b, d)
		next, err = s5.Next()
		if err != nil {
			t.Fatalf("failed to get next record: %v", err)
		}
	}
	s5.Close()
	tx.Commit()
}
