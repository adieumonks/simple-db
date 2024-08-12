package recovery_test

import (
	"path"
	"testing"

	"github.com/adieumonks/simple-db/buffer"
	"github.com/adieumonks/simple-db/file"
	"github.com/adieumonks/simple-db/server"
)

var (
	db *server.SimpleDB
	fm *file.FileManager
	bm *buffer.BufferManager
	b0 file.BlockID
	b1 file.BlockID
)

func TestRecovery(t *testing.T) {
	db, _ = server.NewSimpleDB(path.Join(t.TempDir(), "recoverytest"), 400, 8)
	fm = db.FileManager()
	bm = db.BufferManager()
	b0 = file.NewBlockID("testfile", 0)
	b1 = file.NewBlockID("testfile", 1)

	if fileSize, _ := fm.Length("testfile"); fileSize == 0 {
		initialize(t)
		modify(t)
	} else {
		recovery(t)
	}
}

func initialize(t *testing.T) {
	tx, err := db.NewTransaction()
	if err != nil {
		t.Fatalf("failed to create new transaction: %v", err)
	}
	for i := 0; i < 2; i++ {
		_, err = tx.Append("testfile")
		if err != nil {
			t.Fatalf("failed to append block: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit transaction: %v", err)
	}

	tx1, err := db.NewTransaction()
	if err != nil {
		t.Fatalf("failed to create new transaction: %v", err)
	}
	tx2, err := db.NewTransaction()
	if err != nil {
		t.Fatalf("failed to create new transaction: %v", err)
	}
	if err := tx1.Pin(b0); err != nil {
		t.Fatal("failed to pin block")
	}
	if err := tx2.Pin(b1); err != nil {
		t.Fatal("failed to pin block")
	}
	pos := int32(0)
	for i := 0; i < 6; i++ {
		if err := tx1.SetInt(b0, pos, pos, false); err != nil {
			t.Fatal("failed to set int")
		}
		if err := tx2.SetInt(b1, pos, pos, false); err != nil {
			t.Fatal("failed to set int")
		}
		pos += file.Int32Bytes
	}
	if err := tx1.SetString(b0, 30, "abc", false); err != nil {
		t.Fatal("failed to set string")
	}
	if err := tx2.SetString(b1, 30, "def", false); err != nil {
		t.Fatal("failed to set string")
	}
	if err := tx1.Commit(); err != nil {
		t.Fatalf("failed to commit transaction: %v", err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatalf("failed to commit transaction: %v", err)
	}
	printValues(t, "After Initialization")
}

func modify(t *testing.T) {
	tx3, err := db.NewTransaction()
	if err != nil {
		t.Fatalf("failed to create new transaction: %v", err)
	}
	tx4, err := db.NewTransaction()
	if err != nil {
		t.Fatalf("failed to create new transaction: %v", err)
	}
	if err := tx3.Pin(b0); err != nil {
		t.Fatal("failed to pin block")
	}
	if err := tx4.Pin(b1); err != nil {
		t.Fatal("failed to pin block")
	}
	pos := int32(0)
	for i := 0; i < 6; i++ {
		if err := tx3.SetInt(b0, pos, int32(100+i), true); err != nil {
			t.Fatal("failed to set int")
		}
		if err := tx4.SetInt(b1, pos, int32(100+i), true); err != nil {
			t.Fatal("failed to set int")
		}
		pos += file.Int32Bytes
	}
	if err := tx3.SetString(b0, 30, "uvw", true); err != nil {
		t.Fatal("failed to set string")
	}
	if err := tx4.SetString(b1, 30, "xyz", true); err != nil {
		t.Fatal("failed to set string")
	}
	bm.FlushAll(3)
	bm.FlushAll(4)
	printValues(t, "After Modification")

	if err := tx3.Rollback(); err != nil {
		t.Fatalf("failed to rollback transaction: %v", err)
	}
	printValues(t, "After Rollback")
	// tx4 stops here without committing or rolling back,
	// so all its changes should be undone by recovery
}

func recovery(t *testing.T) {
	tx, err := db.NewTransaction()
	if err != nil {
		t.Fatalf("failed to create new transaction: %v", err)
	}
	if err := tx.Recover(); err != nil {
		t.Fatalf("failed to recover: %v", err)
	}
	printValues(t, "After Recovery")
}

func printValues(t *testing.T, msg string) {
	t.Log(msg)
	p0 := file.NewPage(fm.BlockSize())
	p1 := file.NewPage(fm.BlockSize())
	if err := fm.Read(b0, p0); err != nil {
		t.Fatal("failed to read block")
	}
	if err := fm.Read(b1, p1); err != nil {
		t.Fatal("failed to read block")
	}
	pos := int32(0)
	for i := 0; i < 6; i++ {
		t.Logf("%d ", p0.GetInt(pos))

		t.Logf("%d ", p1.GetInt(pos))
		pos += file.Int32Bytes
	}
	t.Logf("%s ", p0.GetString(30))
	t.Logf("%s ", p1.GetString(30))
}
