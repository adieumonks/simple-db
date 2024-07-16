package tx_test

import (
	"path"
	"testing"

	"github.com/adieumonks/simple-db/server"
	"github.com/adieumonks/simple-db/tx"
)

func TestTx(t *testing.T) {
	db, _ := server.NewSimpleDB(path.Join(t.TempDir(), "txtest"), 400, 8)
	fm := db.FileManager()
	lm := db.LogManager()
	bm := db.BufferManager()

	tx1 := tx.NewTransaction(fm, lm, bm)
	t.Log(tx1)
}
