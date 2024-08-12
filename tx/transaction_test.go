package tx_test

import (
	"path"
	"testing"

	"github.com/adieumonks/simple-db/file"
	"github.com/adieumonks/simple-db/server"
	"github.com/adieumonks/simple-db/tx"
)

func TestTx(t *testing.T) {
	db, _ := server.NewSimpleDB(path.Join(t.TempDir(), "txtest"), 400, 8)
	fm := db.FileManager()
	lm := db.LogManager()
	bm := db.BufferManager()

	tx1, err := tx.NewTransaction(fm, lm, bm)
	if err != nil {
		t.Fatalf("failed to create new transaction: %v", err)
	}
	_, err = fm.Append("testfile")
	if err != nil {
		t.Fatalf("failed to append block: %v", err)
	}
	block := file.NewBlockID("testfile", 0)
	if err := tx1.Pin(block); err != nil {
		t.Fatalf("failed to pin block: %v", err)
	}
	// The block initially contains unknown bytes
	// so don't log those values here.
	if err := tx1.SetInt(block, 80, 1, false); err != nil {
		t.Fatalf("failed to set int: %v", err)
	}
	if err := tx1.SetString(block, 40, "one", false); err != nil {
		t.Fatalf("failed to set string: %v", err)
	}
	if err := tx1.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	tx2, err := tx.NewTransaction(fm, lm, bm)
	if err != nil {
		t.Fatalf("failed to create new transaction: %v", err)
	}
	if err := tx2.Pin(block); err != nil {
		t.Fatalf("failed to pin block: %v", err)
	}
	ival, _ := tx2.GetInt(block, 80)
	sval, _ := tx2.GetString(block, 40)
	t.Logf("initial value at location 80 = %d", ival)
	t.Logf("initial value at location 40 = %s", sval)

	newival := ival + 1
	newsval := sval + "!"
	if err := tx2.SetInt(block, 80, newival, true); err != nil {
		t.Fatalf("failed to set int: %v", err)
	}
	if err := tx2.SetString(block, 40, newsval, true); err != nil {
		t.Fatalf("failed to set string: %v", err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	tx3, err := tx.NewTransaction(fm, lm, bm)
	if err != nil {
		t.Fatalf("failed to create new transaction: %v", err)
	}
	if err := tx3.Pin(block); err != nil {
		t.Fatalf("failed to pin block: %v", err)
	}
	ival, _ = tx3.GetInt(block, 80)
	sval, _ = tx3.GetString(block, 40)
	t.Logf("new value at location 80 = %d", ival)
	t.Logf("new value at location 40 = %s", sval)
	if err := tx3.SetInt(block, 80, 9999, true); err != nil {
		t.Fatalf("failed to set int: %v", err)
	}
	ival, _ = tx3.GetInt(block, 80)
	t.Logf("pre-rollback value at location 80 = %d", ival)
	if err := tx3.Rollback(); err != nil {
		t.Fatalf("failed to rollback: %v", err)
	}

	tx4, err := tx.NewTransaction(fm, lm, bm)
	if err != nil {
		t.Fatalf("failed to create new transaction: %v", err)
	}
	if err := tx4.Pin(block); err != nil {
		t.Fatalf("failed to pin block: %v", err)
	}
	ival, _ = tx4.GetInt(block, 80)
	t.Logf("post-rollback value at location 80 = %d", ival)
	if err := tx4.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}
