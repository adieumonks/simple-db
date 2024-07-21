package query_test

import (
	"fmt"
	"path"
	"testing"

	"github.com/adieumonks/simple-db/query"
	"github.com/adieumonks/simple-db/record"
	"github.com/adieumonks/simple-db/server"
)

func TestProductScan(t *testing.T) {
	db, _ := server.NewSimpleDB(path.Join(t.TempDir(), "productscantest"), 400, 8)
	tx := db.NewTransaction()

	sch1 := record.NewSchema()
	sch1.AddIntField("A")
	sch1.AddStringField("B", 9)
	layout1 := record.NewLayoutFromSchema(sch1)
	ts1, _ := record.NewTableScan(tx, "T1", layout1)

	sch2 := record.NewSchema()
	sch2.AddIntField("C")
	sch2.AddStringField("D", 9)
	layout2 := record.NewLayoutFromSchema(sch2)
	ts2, _ := record.NewTableScan(tx, "T2", layout2)

	n := 200

	ts1.BeforeFirst()
	t.Logf("inserting %d records into T1.", n)
	for i := 0; i < n; i++ {
		ts1.Insert()
		ts1.SetInt("A", int32(i))
		ts1.SetString("B", fmt.Sprintf("aaa%d", i))
	}
	ts1.Close()

	ts2.BeforeFirst()
	t.Logf("inserting %d records into T2.", n)
	for i := 0; i < n; i++ {
		ts2.Insert()
		ts2.SetInt("C", int32(n-i-1))
		ts2.SetString("D", fmt.Sprintf("bbb%d", n-i-1))
	}
	ts2.Close()

	s1, _ := record.NewTableScan(tx, "T1", layout1)
	s2, _ := record.NewTableScan(tx, "T2", layout2)
	s3 := query.NewProductScan(s1, s2)

	next, err := s3.Next()
	if err != nil {
		t.Fatalf("failed to get next record: %v", err)
	}
	for next {
		a, err := s3.GetInt("A")
		if err != nil {
			t.Fatalf("failed to get A: %v", err)
		}
		b, err := s3.GetString("B")
		if err != nil {
			t.Fatalf("failed to get B: %v", err)
		}
		c, err := s3.GetInt("C")
		if err != nil {
			t.Fatalf("failed to get C: %v", err)
		}
		d, err := s3.GetString("D")
		if err != nil {
			t.Fatalf("failed to get D: %v", err)
		}
		t.Logf("A: %d, B: %s, C: %d, D: %s", a, b, c, d)

		next, err = s3.Next()
		if err != nil {
			t.Fatalf("failed to get next record: %v", err)
		}
	}
	s3.Close()
	tx.Commit()
}
