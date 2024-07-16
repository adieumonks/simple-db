package concurrency_test

import (
	"path"
	"sync"
	"testing"
	"time"

	"github.com/adieumonks/simple-db/buffer"
	"github.com/adieumonks/simple-db/file"
	"github.com/adieumonks/simple-db/log"
	"github.com/adieumonks/simple-db/server"
	"github.com/adieumonks/simple-db/tx"
)

var (
	fm *file.FileManager
	lm *log.LogManager
	bm *buffer.BufferManager
	wg sync.WaitGroup
)

func TestConcurrency(t *testing.T) {
	db, _ := server.NewSimpleDB(path.Join(t.TempDir(), "concurrencytest"), 400, 8)
	fm = db.FileManager()
	lm = db.LogManager()
	bm = db.BufferManager()

	wg = sync.WaitGroup{}
	wg.Add(3)

	go runTransactionA(t)
	go runTransactionB(t)
	go runTransactionC(t)

	wg.Wait()
}

func runTransactionA(t *testing.T) {
	defer wg.Done()

	tx := tx.NewTransaction(fm, lm, bm)
	b1 := file.NewBlockID("testfile", 1)
	b2 := file.NewBlockID("testfile", 2)
	tx.Pin(b1)
	tx.Pin(b2)
	t.Log("Tx A: request slock 1")
	tx.GetInt(b1, 0)
	t.Log("Tx A: receive slock 1")
	time.Sleep(1 * time.Second)
	t.Log("Tx A: request slock 2")
	tx.GetInt(b2, 0)
	t.Log("Tx A: receive slock 2")
	tx.Commit()
	t.Log("Tx A: commit")
}

func runTransactionB(t *testing.T) {
	defer wg.Done()

	tx := tx.NewTransaction(fm, lm, bm)
	b1 := file.NewBlockID("testfile", 1)
	b2 := file.NewBlockID("testfile", 2)
	tx.Pin(b1)
	tx.Pin(b2)
	t.Log("Tx B: request xlock 2")
	tx.SetInt(b2, 0, 0, false)
	t.Log("Tx B: receive xlock 2")
	time.Sleep(1 * time.Second)
	t.Log("Tx B: request slock 1")
	tx.GetInt(b2, 0)
	t.Log("Tx B: receive slock 1")
	tx.Commit()
	t.Log("Tx B: commit")
}

func runTransactionC(t *testing.T) {
	defer wg.Done()

	tx := tx.NewTransaction(fm, lm, bm)
	b1 := file.NewBlockID("testfile", 1)
	b2 := file.NewBlockID("testfile", 2)
	tx.Pin(b1)
	tx.Pin(b2)
	time.Sleep(500 * time.Millisecond)
	t.Log("Tx C: request xlock 1")
	tx.SetInt(b1, 0, 0, false)
	t.Log("Tx C: receive xlock 1")
	time.Sleep(1 * time.Second)
	t.Log("Tx C: request slock 2")
	tx.GetInt(b2, 0)
	t.Log("Tx C: receive slock 2")
	tx.Commit()
	t.Log("Tx C: commit")
}
