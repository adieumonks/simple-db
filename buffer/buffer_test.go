package buffer_test

import (
	"testing"

	"github.com/adieumonks/simple-db/file"
	"github.com/adieumonks/simple-db/server"
)

func TestBuffer(t *testing.T) {
	db, err := server.NewSimpleDB("buffertest", 400, 3)
	if err != nil {
		t.Fatalf("failed to create new simple db: %v", err)
	}

	bm := db.BufferManager()

	buffer1, _ := bm.Pin(file.NewBlockID("testfile", 1))
	p := buffer1.Contents()
	n := p.GetInt(80)
	p.SetInt(80, n+1)
	buffer1.SetModified(1, 0)
	t.Logf("The new value is %d", n+1)
	bm.Unpin(buffer1)

	// One of these pins will flush buffer1 to disk
	buffer2, _ := bm.Pin(file.NewBlockID("testfile", 2))
	bm.Pin(file.NewBlockID("testfile", 3))
	bm.Pin(file.NewBlockID("testfile", 4))

	bm.Unpin(buffer2)
	buffer2, _ = bm.Pin(file.NewBlockID("testfile", 1))
	p2 := buffer2.Contents()

	t.Logf("offset %d contains %d", 80, p2.GetInt(80))

	if p2.GetInt(80) != n+1 {
		t.Fatalf("expected %d, got %d", n+1, p2.GetInt(80))
	}

	// This modification won't be written to disk
	p2.SetInt(80, 9999)
	buffer2.SetModified(1, 0)
	bm.Unpin(buffer2)
}
