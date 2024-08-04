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

	tx, err := tx.NewTransaction(fm, lm, bm)
	if err != nil {
		t.Errorf("failed to create new transaction: %v", err)
	}
	b1 := file.NewBlockID("testfile", 1)
	b2 := file.NewBlockID("testfile", 2)
	if err := tx.Pin(b1); err != nil {
		t.Errorf("Tx A: failed to pin block: %v", err)
	}
	if err := tx.Pin(b2); err != nil {
		t.Errorf("Tx A: failed to pin block: %v", err)
	}
	t.Log("Tx A: request slock 1")
	if _, err := tx.GetInt(b1, 0); err != nil {
		t.Errorf("Tx A: failed to get int: %v", err)
	}
	t.Log("Tx A: receive slock 1")
	time.Sleep(1 * time.Second)
	t.Log("Tx A: request slock 2")
	if _, err := tx.GetInt(b2, 0); err != nil {
		t.Errorf("Tx A: failed to get int: %v", err)
	}
	t.Log("Tx A: receive slock 2")
	if err := tx.Commit(); err != nil {
		t.Errorf("Tx A: failed to commit: %v", err)
	}
	t.Log("Tx A: commit")
}

func runTransactionB(t *testing.T) {
	defer wg.Done()

	tx, err := tx.NewTransaction(fm, lm, bm)
	if err != nil {
		t.Errorf("failed to create new transaction: %v", err)
		return
	}
	b1 := file.NewBlockID("testfile", 1)
	b2 := file.NewBlockID("testfile", 2)
	if err := tx.Pin(b1); err != nil {
		t.Errorf("Tx B: failed to pin block: %v", err)
	}
	if err := tx.Pin(b2); err != nil {
		t.Errorf("Tx B: failed to pin block: %v", err)
	}
	t.Log("Tx B: request xlock 2")
	if err := tx.SetInt(b2, 0, 0, false); err != nil {
		t.Errorf("Tx B: failed to set int: %v", err)
		return
	}
	t.Log("Tx B: receive xlock 2")
	time.Sleep(1 * time.Second)
	t.Log("Tx B: request slock 1")
	if _, err := tx.GetInt(b2, 0); err != nil {
		t.Errorf("Tx B: failed to get int: %v", err)
		return
	}
	t.Log("Tx B: receive slock 1")
	if err := tx.Commit(); err != nil {
		t.Errorf("Tx B: failed to commit: %v", err)
	}
	t.Log("Tx B: commit")
}

func runTransactionC(t *testing.T) {
	defer wg.Done()

	tx, err := tx.NewTransaction(fm, lm, bm)
	if err != nil {
		t.Errorf("failed to create new transaction: %v", err)
	}
	b1 := file.NewBlockID("testfile", 1)
	b2 := file.NewBlockID("testfile", 2)
	if err := tx.Pin(b1); err != nil {
		t.Errorf("Tx C: failed to pin block: %v", err)
	}
	if err := tx.Pin(b2); err != nil {
		t.Errorf("Tx C: failed to pin block: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	t.Log("Tx C: request xlock 1")
	if err := tx.SetInt(b1, 0, 0, false); err != nil {
		t.Errorf("Tx C: failed to set int: %v", err)
	}
	t.Log("Tx C: receive xlock 1")
	time.Sleep(1 * time.Second)
	t.Log("Tx C: request slock 2")
	if _, err := tx.GetInt(b2, 0); err != nil {
		t.Errorf("Tx C: failed to get int: %v", err)
	}
	t.Log("Tx C: receive slock 2")
	if err := tx.Commit(); err != nil {
		t.Errorf("Tx C: failed to commit: %v", err)
	}
	t.Log("Tx C: commit")
}
