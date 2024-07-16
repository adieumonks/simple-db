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

	tx1 := tx.NewTransaction(fm, lm, bm)
	block := file.NewBlockID("testfile", 1)
	tx1.Pin(block)
	// The block initially contains unknown bytes
	// so don't log those values here.
	tx1.SetInt(block, 80, 1, false)
	tx1.SetString(block, 40, "one", false)
	tx1.Commit()

	tx2 := tx.NewTransaction(fm, lm, bm)
	tx2.Pin(block)
	ival, _ := tx2.GetInt(block, 80)
	sval, _ := tx2.GetString(block, 40)
	t.Logf("initial value at location 80 = %d", ival)
	t.Logf("initial value at location 40 = %s", sval)

	newival := ival + 1
	newsval := sval + "!"
	tx2.SetInt(block, 80, newival, true)
	tx2.SetString(block, 40, newsval, true)
	tx2.Commit()

	tx3 := tx.NewTransaction(fm, lm, bm)
	tx3.Pin(block)
	ival, _ = tx3.GetInt(block, 80)
	sval, _ = tx3.GetString(block, 40)
	t.Logf("new value at location 80 = %d", ival)
	t.Logf("new value at location 40 = %s", sval)
	tx3.SetInt(block, 80, 9999, true)
	ival, _ = tx3.GetInt(block, 80)
	t.Logf("pre-rollback value at location 80 = %d", ival)
	tx3.Rollback()

	tx4 := tx.NewTransaction(fm, lm, bm)
	tx4.Pin(block)
	ival, _ = tx4.GetInt(block, 80)
	t.Logf("post-rollback value at location 80 = %d", ival)
	tx4.Commit()
}
