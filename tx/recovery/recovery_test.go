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
	tx1 := db.NewTransaction()
	tx2 := db.NewTransaction()
	tx1.Pin(b0)
	tx2.Pin(b1)
	pos := int32(0)
	for i := 0; i < 6; i++ {
		tx1.SetInt(b0, pos, pos, false)
		tx2.SetInt(b1, pos, pos, false)
		pos += file.Int32Bytes
	}
	tx1.SetString(b0, 30, "abc", false)
	tx2.SetString(b1, 30, "def", false)
	tx1.Commit()
	tx2.Commit()
	printValues(t, "After Initialization")
}

func modify(t *testing.T) {
	tx3 := db.NewTransaction()
	tx4 := db.NewTransaction()
	tx3.Pin(b0)
	tx4.Pin(b1)
	pos := int32(0)
	for i := 0; i < 6; i++ {
		tx3.SetInt(b0, pos, int32(100+i), true)
		tx4.SetInt(b1, pos, int32(100+i), true)
		pos += file.Int32Bytes
	}
	tx3.SetString(b0, 30, "uvw", true)
	tx4.SetString(b1, 30, "xyz", true)
	bm.FlushAll(3)
	bm.FlushAll(4)
	printValues(t, "After Modification")

	tx3.Rollback()
	printValues(t, "After Rollback")
	// tx4 stops here without committing or rolling back,
	// so all its changes should be undone by recovery
}

func recovery(t *testing.T) {
	tx := db.NewTransaction()
	tx.Recover()
	printValues(t, "After Recovery")
}

func printValues(t *testing.T, msg string) {
	t.Log(msg)
	p0 := file.NewPage(fm.BlockSize())
	p1 := file.NewPage(fm.BlockSize())
	fm.Read(b0, p0)
	fm.Read(b1, p1)
	pos := int32(0)
	for i := 0; i < 6; i++ {
		t.Logf("%d ", p0.GetInt(pos))

		t.Logf("%d ", p1.GetInt(pos))
		pos += file.Int32Bytes
	}
	t.Logf("%s ", p0.GetString(30))
	t.Logf("%s ", p1.GetString(30))
}
